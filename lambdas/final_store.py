import json
import os
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

try:
    from kafka import KafkaProducer
except Exception:  # pragma: no cover - packaging fallback
    KafkaProducer = None


def _client(service_name):
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("LOCALSTACK_ENDPOINT") or "http://localstack:4566"
    region = os.environ.get("AWS_REGION", "us-east-1")
    return boto3.client(service_name, endpoint_url=endpoint, region_name=region)


def _publish_kafka(message):
    if KafkaProducer is None:
        raise RuntimeError("kafka-python is required for claims.processed publishing")

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda payload: json.dumps(payload).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value is not None else None,
        retries=3,
        linger_ms=10,
    )
    producer.send(os.environ.get("KAFKA_PROCESSED_TOPIC", "claims.processed"), key=message["documentId"], value=message)
    producer.flush(timeout=10)


def lambda_handler(event, context):
    document_id = event.get("documentId") or event.get("id")
    if not document_id:
        raise ValueError("documentId is required")

    table_name = os.environ.get("DYNAMODB_TABLE_NAME", "claims")
    dynamodb = _client("dynamodb")
    item = {
        "documentId": {"S": document_id},
        "status": {"S": "processed"},
        "scanStatus": {"S": str(event.get("virusScanResult", {}).get("scanStatus", "unknown"))},
        "extractedText": {"S": str(event.get("ocrResult", {}).get("extractedText", ""))},
        "updatedAt": {"S": datetime.now(timezone.utc).isoformat()}
    }

    try:
        dynamodb.put_item(
            TableName=table_name,
            Item=item,
            ConditionExpression="attribute_not_exists(documentId)",
        )
        stored = True
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "ConditionalCheckFailedException":
            stored = False
        else:
            raise

    confirmation = {
        "documentId": document_id,
        "status": "processed",
        "stored": stored,
        "source": "final-store",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    _publish_kafka(confirmation)
    return confirmation
