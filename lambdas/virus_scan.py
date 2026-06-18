import json
from datetime import datetime, timezone


def lambda_handler(event, context):
    document_id = event.get("documentId") or event.get("id") or "unknown"
    result = dict(event)
    result["documentId"] = document_id
    result["scanStatus"] = "clean"
    result["scanTimestamp"] = datetime.now(timezone.utc).isoformat()
    return result
