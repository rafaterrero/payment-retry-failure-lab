"""A synthetic AWS Emulator Lambda probe; no payment or network calls."""


def handler(event, context):
    try:
        import boto3  # noqa: F401 - capability probe inside the emulated runtime
        boto3_available = True
    except ImportError:
        boto3_available = False
    return {
        "ok": True,
        "operation_id": event.get("operation_id"),
        "record_count": len(event.get("Records", [])),
        "boto3_available": boto3_available,
    }
