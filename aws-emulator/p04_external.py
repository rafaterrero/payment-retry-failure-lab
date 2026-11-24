"""P04: outbox plus an independently committed provider ledger and fault matrix."""
import json
from pathlib import Path
import subprocess
import sys


def run_p04(api, table, report):
    def S(x): return {'S': x}
    def get(key):
        return api('dynamodb', 'get-item', table_name=table, key={'pk': S(key)}, consistent_read=True).get('Item', {})
    def provider(action, op):
        process = subprocess.run([sys.executable, str(Path(__file__).with_name('external_provider.py')),
                                  str(report / 'provider.sqlite'), action, op], capture_output=True, text=True, timeout=10)
        if process.returncode:
            raise RuntimeError('Synthetic provider failed: ' + process.stderr)
        response = json.loads(process.stdout)
        provider_calls.append({'action': action, 'operation_id': op, 'response': response})
        return response
    def state(op, old, new):
        api('dynamodb', 'update-item', table_name=table, key={'pk': S(op)},
            update_expression='SET #s = :new', condition_expression='#s = :old',
            expression_attribute_names={'#s': 'status'},
            expression_attribute_values={':old': S(old), ':new': S(new)})
    def create(op):
        api('dynamodb', 'transact-write-items', transact_items=[
            {'Put': {'TableName': table, 'Item': {'pk': S(op), 'status': S('PENDING')}, 'ConditionExpression': 'attribute_not_exists(pk)'}},
            {'Put': {'TableName': table, 'Item': {'pk': S(op+'-outbox'), 'status': S('UNSENT')}, 'ConditionExpression': 'attribute_not_exists(pk)'}}])
    class Fault(Exception): pass
    def consume(op, fault=None):
        current = get(op)['status']['S']
        if current == 'SUCCEEDED':
            return 'ALREADY_SUCCEEDED'
        if current == 'PENDING':
            state(op, 'PENDING', 'UNKNOWN')  # uncertainty persisted before contacting provider
            if fault == 'before_provider':
                raise Fault('before_provider')
            receipt = provider('charge', op)
            if fault == 'before_local_result':
                raise Fault('before_local_result')
        elif current == 'UNKNOWN':
            receipt = provider('lookup', op)
            if receipt['charge_count'] == 0:
                return 'UNKNOWN_REQUIRES_REVIEW'  # never resubmit on absence alone
        else:
            raise RuntimeError('Unexpected state')
        if receipt['charge_count'] != 1 or receipt['amount_minor'] != 1000:
            raise RuntimeError('Conflicting synthetic provider evidence')
        state(op, 'UNKNOWN', 'SUCCEEDED')
        if fault == 'after_local_result':
            raise Fault('after_local_result')
        return 'SUCCEEDED'

    provider_calls = []
    # A rejected operation+outbox transaction must not leave a partial write.
    create('synthetic-p04-conflict')
    try:
        api('dynamodb', 'transact-write-items', transact_items=[
            {'Put': {'TableName': table, 'Item': {'pk': S('synthetic-p04-rollback')}, 'ConditionExpression': 'attribute_not_exists(pk)'}},
            {'Put': {'TableName': table, 'Item': {'pk': S('synthetic-p04-conflict')}, 'ConditionExpression': 'attribute_not_exists(pk)'}}])
    except Exception as error:
        if 'ConditionalCheckFailed' not in str(error): raise
    else:
        raise RuntimeError('Conflicting transaction unexpectedly committed')
    if get('synthetic-p04-rollback'):
        raise RuntimeError('Rollback left a partial write')

    queue = api('sqs', 'create-queue', queue_name=table+'-external-outbox')['QueueUrl']
    op = 'synthetic-p04-outbox'
    create(op)
    sent = []
    for attempt in range(2):
        if get(op+'-outbox')['status']['S'] != 'UNSENT': raise RuntimeError('Outbox already sent')
        sent.append(api('sqs','send-message',queue_url=queue,message_body=op)['MessageId'])
        if attempt == 0: continue  # fault: send committed, delivery marker omitted
        state(op+'-outbox','UNSENT','SENT')
    seen = set(); outcomes=[]
    for _ in range(15):
        messages=api('sqs','receive-message',queue_url=queue,wait_time_seconds=1,max_number_of_messages=1).get('Messages',[])
        for message in messages:
            if message['MessageId'] not in sent or message['Body'] != op: raise RuntimeError('Unexpected queue message')
            outcomes.append(consume(op))  # no test-harness MessageId deduplication
            seen.add(message['MessageId'])
            api('sqs','delete-message',queue_url=queue,receipt_handle=message['ReceiptHandle'])
        if seen == set(sent): break
    if len(seen)!=2 or provider('lookup',op)['charge_count']!=1: raise RuntimeError('Outbox recovery failed')

    cases=[]
    for fault in ['before_provider','before_local_result','after_local_result']:
        op='synthetic-p04-'+fault
        create(op)
        try: consume(op,fault)
        except Fault as error:
            if str(error)!=fault: raise
        else: raise RuntimeError('Expected fault was not injected')
        checkpoint={'local_state':get(op)['status']['S'],'provider':provider('lookup',op)}
        recovered=consume(op)
        final=get(op)['status']['S']; count=provider('lookup',op)['charge_count']
        expected=('UNKNOWN',0,'UNKNOWN_REQUIRES_REVIEW') if fault=='before_provider' else ('SUCCEEDED',1,'SUCCEEDED' if fault=='before_local_result' else 'ALREADY_SUCCEEDED')
        if (final,count,recovered)!=expected: raise RuntimeError('Unexpected recovery outcome')
        charge_calls=sum(c['action']=='charge' and c['operation_id']==op for c in provider_calls)
        if charge_calls != (0 if fault=='before_provider' else 1): raise RuntimeError('Recovery repeated provider charge call')
        cases.append({'fault':fault,'checkpoint':checkpoint,'retry_outcome':recovered,'final_local_state':final,'provider_charges':count,'provider_charge_calls':charge_calls})
    (report/'provider-calls.json').write_text(json.dumps(provider_calls,indent=2)+'\n')
    return {'status':'passed','rollback_verified':True,'messages_sent':2,'distinct_messages_received':len(seen),
            'consumer_outcomes':outcomes,'outbox_provider_charges':1,'fault_cases':cases,
            'provider_boundary':'Separate OS processes and SQLite commits; DynamoDB transactions never include provider ledger.',
            'limitations':'Controlled exceptions in host worker, not kill -9 or network faults. Provider offers immediate authoritative lookup and permanent per-operation idempotency for this test. Before-provider uncertainty remains unresolved without a blind retry.'}
