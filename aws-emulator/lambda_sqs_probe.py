"""Synthetic SQS -> Lambda -> DynamoDB probe for the local AWS Emulator."""
import os

import boto3


def handler(event, context):
    if event.get("probe_env"):
        return {
            "aws_env_names": sorted(k for k in os.environ if k.startswith("AWS_")),
            "has_packaged_ca_file": os.path.exists(os.path.join(os.path.dirname(__file__), "ca.pem")),
        }
    dynamodb = boto3.client(
        "dynamodb",
        endpoint_url=os.environ["EMULATOR_ENDPOINT"],
        verify=os.path.join(os.path.dirname(__file__), "ca.pem"),
        region_name="us-east-1",
    )
    for record in event.get("Records", []):
        dynamodb.put_item(
            TableName=os.environ["LAB_TABLE"],
            Item={
                "pk": {"S": record["messageId"]},
                "body": {"S": record["body"]},
            },
        )
    return {"records": len(event.get("Records", []))}
