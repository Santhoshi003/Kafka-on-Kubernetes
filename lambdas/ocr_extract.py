from datetime import datetime, timezone


def lambda_handler(event, context):
    document_id = event.get("documentId") or event.get("id") or "unknown"
    fraud_requested = bool(event.get("fraud")) or str(event.get("forceFraud", "")).lower() in {"1", "true", "yes"}
    source_text = event.get("sourceText") or event.get("text") or f"document {document_id} processed successfully"
    if fraud_requested:
        source_text = f"{source_text} fraud suspected"

    result = dict(event)
    result["documentId"] = document_id
    result["extractedText"] = source_text.lower()
    result["ocrTimestamp"] = datetime.now(timezone.utc).isoformat()
    return result
