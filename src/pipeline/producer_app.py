import argparse
import json
import os
from pathlib import Path

from kafka import KafkaProducer


def load_submission_payload() -> tuple[str, dict]:
    submission_path = Path(__file__).resolve().parents[2] / "submission.json"
    data = json.loads(submission_path.read_text(encoding="utf-8"))
    document_id = data["idempotencyTest"]["documentId"]
    payload = dict(data["idempotencyTest"]["payload"])
    return document_id, payload


def build_message(document_id: str, payload: dict, fraud: bool) -> dict:
    message = {
        "documentId": document_id,
        "payload": payload,
    }
    if fraud:
        message["fraud"] = True
        message["sourceText"] = f"document {document_id} contains fraud signals"
    return message


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish claim documents to Kafka.")
    parser.add_argument("--document-id", default=None)
    parser.add_argument("--fraud", action="store_true")
    parser.add_argument("--count", type=int, default=1)
    parser.add_argument("--duplicate", action="store_true")
    args = parser.parse_args()

    default_document_id, default_payload = load_submission_payload()
    document_id = args.document_id or default_document_id
    payload = dict(default_payload)

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    topic = os.environ.get("KAFKA_INCOMING_TOPIC", "claims.incoming")
    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        value_serializer=lambda item: json.dumps(item).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8"),
        retries=3,
    )

    message_count = max(args.count, 1)
    messages = [build_message(document_id, payload, args.fraud) for _ in range(message_count)]
    if args.duplicate:
        messages.append(build_message(document_id, payload, args.fraud))

    for index, message in enumerate(messages, start=1):
        producer.send(topic, key=message["documentId"], value=message).get(timeout=10)
        print(json.dumps({"sent": index, "topic": topic, "documentId": message["documentId"]}))

    producer.flush(timeout=10)


if __name__ == "__main__":
    main()
