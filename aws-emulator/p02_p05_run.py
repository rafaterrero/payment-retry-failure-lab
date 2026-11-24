"""Local CLI integration tests. Synthetic data only; never uses a cloud endpoint."""
import concurrent.futures
import json
import os
from pathlib import Path
import subprocess
import sys
import threading
import time
import uuid
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from payment_failure_lab.lab import FakeProvider, ManualClock
REPORT = ROOT / 'reports' / ('aws-emulator-p02-p05-' + uuid.uuid4().hex[:8])
REPORT.mkdir(parents=True)
env = os.environ.copy()
for key in list(env):
    if key.startswith('AWS_'):
        del env[key]
base = Path(os.environ.get('EMULATOR_DIR', str(Path.home() / '.aws/emulator'))) / '9297'
for name in ['emulator_credentials', 'config', 'ca.pem']:
    if not (base / name).is_file():
        raise RuntimeError('Start the local emulator first')
env.update(AWS_SHARED_CREDENTIALS_FILE=str(base / 'emulator_credentials'), AWS_CONFIG_FILE=str(base / 'config'), AWS_CA_BUNDLE=str(base / 'ca.pem'), AWS_EC2_METADATA_DISABLED='true', AWS_DISABLE_HOST_PREFIX_INJECTION='true', AWS_REGION='us-east-1', AWS_DEFAULT_REGION='us-east-1', AWS_PAGER='', AWS_MAX_ATTEMPTS='1')
trace = []
lock = threading.Lock()

class APIError(Exception):
    pass

def api(service, operation, **kwargs):
    args = ['aws', '--endpoint-url', 'https://127.0.0.1:9297', '--output', 'json', service, operation]
    for key, value in kwargs.items():
        args.append('--' + key.replace('_', '-'))
        if value is not True:
            args.append(json.dumps(value) if isinstance(value, (dict, list)) else str(value))
    result = subprocess.run(args, env=env, capture_output=True, text=True, timeout=40)
    with lock:
        trace.append(dict(service=service, operation=operation, input=kwargs, exit_code=result.returncode, output=result.stdout, error=result.stderr))
    if result.returncode:
        raise APIError(result.stderr)
    return json.loads(result.stdout) if result.stdout.strip() else {}

def S(value):
    return {'S': value}

def N(value):
    return {'N': str(value)}

def get(pk):
    return api('dynamodb', 'get-item', table_name=table, key={'pk': S(pk)}, consistent_read=True).get('Item', {})

def put(pk, **fields):
    return api('dynamodb', 'put-item', table_name=table, item={'pk': S(pk), **fields})

def receive(queue, wait=1):
    return api('sqs', 'receive-message', queue_url=queue, wait_time_seconds=wait, max_number_of_messages=1).get('Messages', [])

def delete(queue, message):
    api('sqs', 'delete-message', queue_url=queue, receipt_handle=message['ReceiptHandle'])

def condition_failure(call):
    try:
        call()
    except APIError as error:
        if not 'ConditionalCheckFailed' in str(error):
            raise RuntimeError(str(error))
        return
    raise AssertionError('Conditional operation unexpectedly succeeded')
results = {'data': 'synthetic', 'scenario_status': {}, 'execution': 'host Python orchestrator using AWS CLI; no Lambda invocation in P02–P05'}
try:
    identity = api('sts', 'get-caller-identity')
    if not identity['Account'] == '000000000000':
        raise RuntimeError('Refusing non-emulator account')
    results['emulator_version'] = subprocess.check_output(['docker', 'exec', 'payment-lab-emulator', 'aws-emulator', '--version'], text=True).strip()
    suffix = uuid.uuid4().hex[:8]
    table = 'synthetic_retry_' + suffix
    api('dynamodb', 'create-table', table_name=table, attribute_definitions=[{'AttributeName': 'pk', 'AttributeType': 'S'}], key_schema=[{'AttributeName': 'pk', 'KeyType': 'HASH'}], billing_mode='PAY_PER_REQUEST')
    for _ in range(30):
        if api('dynamodb', 'describe-table', table_name=table)['Table']['TableStatus'] == 'ACTIVE':
            break
        time.sleep(1)
    else:
        raise AssertionError('Table not active')
    fifo = api('sqs', 'create-queue', queue_name='synthetic-fifo-' + suffix + '.fifo', attributes={'FifoQueue': 'true', 'ContentBasedDeduplication': 'false'})['QueueUrl']

    def send_fifo():
        return api('sqs', 'send-message', queue_url=fifo, message_body='synthetic-p02', message_group_id='synthetic-group', message_deduplication_id='synthetic-stable-key')
    started = time.monotonic()
    first_send = send_fifo()
    initial = receive(fifo)
    if not (len(initial) == 1 and initial[0]['MessageId'] == first_send['MessageId']):
        raise RuntimeError("Validation failed: len(initial) == 1 and initial[0]['MessageId'] == first_send['MessageId']")
    delete(fifo, initial[0])
    within_elapsed = time.monotonic() - started
    send_fifo()
    within = receive(fifo, 2)
    if not not within:
        raise RuntimeError('FIFO duplicate delivered within window')
    clock = ManualClock()
    provider = FakeProvider(clock, retention_seconds=120)
    provider.charge('synthetic-p02', 1000, 'synthetic-provider-key')
    put('P02-operation', status=S('SUCCEEDED'))
    clock.advance(121)
    if get('P02-operation')['status']['S'] != 'SUCCEEDED':
        provider.charge('synthetic-p02', 1000, 'synthetic-provider-key')
    if not provider.count('synthetic-p02') == 1:
        raise RuntimeError("Validation failed: provider.count('synthetic-p02') == 1")
    naive_provider = FakeProvider(ManualClock(), retention_seconds=120)
    naive_provider.charge('synthetic-p02', 1000, 'synthetic-provider-key')
    naive_provider.clock.advance(121)
    naive_provider.charge('synthetic-p02', 1000, 'synthetic-provider-key')
    if not naive_provider.count('synthetic-p02') == 2:
        raise RuntimeError("Validation failed: naive_provider.count('synthetic-p02') == 2")
    print('P02: duplicate suppressed; waiting for real FIFO expiry while running P03–P05', flush=True)
    put('P03-operation', status=S('PENDING'))
    barrier = threading.Barrier(2)

    def claim():
        barrier.wait()
        try:
            api('dynamodb', 'update-item', table_name=table, key={'pk': S('P03-operation')}, update_expression='SET #s = :claimed', condition_expression='#s = :pending', expression_attribute_names={'#s': 'status'}, expression_attribute_values={':claimed': S('IN_FLIGHT'), ':pending': S('PENDING')})
            return 'CALL_PROVIDER'
        except APIError as error:
            if not 'ConditionalCheckFailedException' in str(error):
                raise RuntimeError("Validation failed: 'ConditionalCheckFailedException' in str(error)")
            return 'CLAIM_REJECTED'
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(claim) for _ in range(2)]
        claims = [future.result() for future in futures]
    if not sorted(claims) == ['CALL_PROVIDER', 'CLAIM_REJECTED']:
        raise RuntimeError("Validation failed: sorted(claims) == ['CALL_PROVIDER', 'CLAIM_REJECTED']")
    results['scenario_status']['P03'] = dict(status='passed', claims=claims, state=get('P03-operation'))
    print('P03 passed: one conditional claim won, one rejected', flush=True)
    from p04_external import run_p04
    results['scenario_status']['P04'] = run_p04(api, table, REPORT)
    print('P04 passed: independent provider and local-state fault cases', flush=True)
    put('P05-operation', status=S('PENDING'))

    def event(event_id, status):
        if get('P05-event-' + event_id):
            return 'DUPLICATE'
        current = get('P05-operation')['status']['S']
        next_status = 'SUCCEEDED' if current == 'SUCCEEDED' or status == 'SUCCEEDED' else 'PROCESSING'
        api('dynamodb', 'transact-write-items', transact_items=[{'Put': {'TableName': table, 'Item': {'pk': S('P05-event-' + event_id)}, 'ConditionExpression': 'attribute_not_exists(pk)'}}, {'Update': {'TableName': table, 'Key': {'pk': S('P05-operation')}, 'UpdateExpression': 'SET #s = :next', 'ConditionExpression': '#s = :current', 'ExpressionAttributeNames': {'#s': 'status'}, 'ExpressionAttributeValues': {':next': S(next_status), ':current': S(current)}}}])
        return 'APPLIED' if current != next_status else 'STATE_PRESERVED'
    events = [event('synthetic-1', 'SUCCEEDED'), event('synthetic-1', 'SUCCEEDED'), event('synthetic-2', 'PROCESSING')]
    if not events == ['APPLIED', 'DUPLICATE', 'STATE_PRESERVED']:
        raise RuntimeError("Validation failed: events == ['APPLIED', 'DUPLICATE', 'STATE_PRESERVED']")
    if not get('P05-operation')['status']['S'] == 'SUCCEEDED':
        raise RuntimeError("Validation failed: get('P05-operation')['status']['S'] == 'SUCCEEDED'")
    results['scenario_status']['P05'] = dict(status='passed', events=events, final_state='SUCCEEDED', limitation='Sequential pre-verified synthetic events; no webhook transport, authentication or concurrent event test.')
    print('P05 passed: duplicate ignored and late PROCESSING preserved SUCCEEDED', flush=True)
    while time.monotonic() - started < 305:
        remaining = 305 - (time.monotonic() - started)
        print('P02: real-time FIFO expiry check in %.0f seconds' % remaining, flush=True)
        time.sleep(min(20, remaining))
    after_elapsed = time.monotonic() - started
    last_send = send_fifo()
    after = receive(fifo, 5)
    if not (len(after) == 1 and after[0]['Body'] == 'synthetic-p02'):
        raise RuntimeError("Validation failed: len(after) == 1 and after[0]['Body'] == 'synthetic-p02'")
    if not after[0]['MessageId'] == last_send['MessageId']:
        raise RuntimeError("Validation failed: after[0]['MessageId'] == last_send['MessageId']")
    if not last_send['MessageId'] != first_send['MessageId']:
        raise RuntimeError("Validation failed: last_send['MessageId'] != first_send['MessageId']")
    delete(fifo, after[0])
    results['scenario_status']['P02'] = dict(status='passed', first_delivered=1, within_window_delivered=0, within_send_elapsed_seconds=round(within_elapsed, 2), after_window_delivered=1, after_send_elapsed_seconds=round(after_elapsed, 2), final_message_id_matches_send=True, final_message_id_differs_from_first=True, provider_retention_simulated_seconds=120, provider_replay_simulated_seconds=121, naive_provider_charges=2, guarded_provider_charges=1, limitation='FIFO uses real elapsed time; provider retention remains a manual-clock test double. No Lambda in this scenario.')
    print('P02 passed: same FIFO deduplication ID delivered after %.1f seconds' % after_elapsed, flush=True)
    results['status'] = 'passed'
except Exception as error:
    results['status'] = 'failed'
    results['error'] = str(error)
    raise
finally:
    (REPORT / 'results.json').write_text(json.dumps(results, indent=2) + '\n')
    (REPORT / 'trace.json').write_text(json.dumps(trace, indent=2) + '\n')
    print('Evidence: ' + str(REPORT.relative_to(ROOT)), flush=True)
