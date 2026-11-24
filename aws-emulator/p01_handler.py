"""P01 on AWS Emulator: synthetic ledger only; no payment API or private data."""
import json
import os
import time

import boto3
from botocore.exceptions import ClientError


def _ddb():
    return boto3.client(
        "dynamodb",
        endpoint_url=os.environ["EMULATOR_ENDPOINT"],
        verify=os.path.join(os.path.dirname(__file__), "ca.pem"),
        region_name="us-east-1",
    )


def _item(ddb, pk):
    return ddb.get_item(
        TableName=os.environ["LAB_TABLE"],
        Key={"pk": {"S": pk}},
        ConsistentRead=True,
    ).get("Item", {})


def _naive(ddb, operation_id):
    attempts = ddb.update_item(
        TableName=os.environ["LAB_TABLE"],
        Key={"pk": {"S": "ATTEMPTS#" + operation_id}},
        UpdateExpression="ADD attempts :one",
        ExpressionAttributeValues={":one": {"N": "1"}},
        ReturnValues="UPDATED_NEW",
    )["Attributes"]["attempts"]["N"]
    ddb.update_item(
        TableName=os.environ["LAB_TABLE"],
        Key={"pk": {"S": "LEDGER#" + operation_id}},
        UpdateExpression="ADD charge_count :one",
        ExpressionAttributeValues={":one": {"N": "1"}},
    )
    if attempts == "1":
        raise RuntimeError("Synthetic acknowledgment lost after ledger write")


def _guarded(ddb, operation_id, fault="after_unknown"):
    now = int(time.time())
    operation_key = "OP#" + operation_id
    ledger_key = "LEDGER#" + operation_id
    ddb.update_item(
        TableName=os.environ["LAB_TABLE"],
        Key={"pk": {"S": "ATTEMPTS#" + operation_id}},
        UpdateExpression="ADD attempts :one",
        ExpressionAttributeValues={":one": {"N": "1"}},
    )
    operation = _item(ddb, operation_key)
    status = operation.get("status", {}).get("S")
    if status == "PENDING":
        try:
            ddb.update_item(
                TableName=os.environ["LAB_TABLE"],
                Key={"pk": {"S": operation_key}},
                UpdateExpression="SET #s = :in_flight, lease_until = :lease",
                ConditionExpression="#s = :pending",
                ExpressionAttributeNames={"#s": "status"},
                ExpressionAttributeValues={
                    ":pending": {"S": "PENDING"},
                    ":in_flight": {"S": "IN_FLIGHT"},
                    ":lease": {"N": str(now + 3)},
                },
            )
        except ClientError as error:
            if error.response["Error"]["Code"] == "ConditionalCheckFailedException":
                raise RuntimeError("Claim changed; retry and inspect durable state") from error
            raise
        ddb.put_item(
            TableName=os.environ["LAB_TABLE"],
            Item={
                "pk": {"S": ledger_key},
                "charge_count": {"N": "1"},
                "amount_minor": {"N": operation["amount_minor"]["N"]},
            },
            ConditionExpression="attribute_not_exists(pk)",
        )
        if fault == "before_unknown":
            raise RuntimeError("Injected failure after provider ledger, before UNKNOWN")
        ddb.update_item(
            TableName=os.environ["LAB_TABLE"],
            Key={"pk": {"S": operation_key}},
            UpdateExpression="SET #s = :unknown",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":unknown": {"S": "UNKNOWN"}},
        )
        raise RuntimeError("Synthetic acknowledgment lost after ledger write")
    if status == "IN_FLIGHT":
        if now < int(operation.get("lease_until", {}).get("N", "0")):
            raise RuntimeError("Claim still leased; retry later")
        ddb.update_item(
            TableName=os.environ["LAB_TABLE"], Key={"pk": {"S": operation_key}},
            UpdateExpression="SET #s = :unknown",
            ConditionExpression="#s = :flight AND lease_until <= :now",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":unknown": {"S": "UNKNOWN"}, ":flight": {"S": "IN_FLIGHT"}, ":now": {"N": str(now)}})
        status = "UNKNOWN"
    if status == "UNKNOWN":
        ledger = _item(ddb, ledger_key)
        if not ledger:
            raise RuntimeError("No ledger evidence; refusing blind retry")
        if ledger.get("amount_minor") != operation.get("amount_minor") or ledger.get("charge_count") != {"N": "1"}:
            raise RuntimeError("Conflicting ledger evidence; manual review")
        ddb.update_item(
            TableName=os.environ["LAB_TABLE"],
            Key={"pk": {"S": operation_key}},
            UpdateExpression="SET #s = :succeeded",
            ConditionExpression="#s = :unknown",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={
                ":unknown": {"S": "UNKNOWN"},
                ":succeeded": {"S": "SUCCEEDED"},
            },
        )
        return
    if status == "SUCCEEDED":
        return
    raise RuntimeError("Unexpected operation state: " + str(status))


def handler(event, context):
    ddb = _ddb()
    for record in event.get("Records", []):
        work = json.loads(record["body"])
        operation_id = work["operation_id"]
        if not operation_id.startswith("synthetic-"):
            raise ValueError("Only synthetic operation IDs are accepted")
        if work["mode"] == "naive":
            _naive(ddb, operation_id)
        elif work["mode"] == "guarded":
            _guarded(ddb, operation_id, work.get("fault", "after_unknown"))
        else:
            raise ValueError("Unsupported test mode")
    return {"processed": len(event.get("Records", []))}
