import argparse
import json
import os
import time
from typing import Any

import boto3
from kafka import KafkaConsumer


def resolve_state_machine_arn(stepfunctions_client: Any, explicit_arn: str | None) -> str:
    if explicit_arn:
        return explicit_arn

    state_machine_name = os.environ.get("STEP_FUNCTION_NAME", "ClaimProcessor")
    response = stepfunctions_client.list_state_machines()
    for state_machine in response.get("stateMachines", []):
        if state_machine.get("name") == state_machine_name:
            return state_machine["stateMachineArn"]
    raise RuntimeError(f"Unable to locate Step Functions state machine named {state_machine_name}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Consume Kafka claims and trigger Step Functions.")
    parser.add_argument("--max-messages", type=int, default=0, help="Stop after processing this many messages.")
    parser.add_argument("--poll-timeout", type=int, default=1000)
    args = parser.parse_args()

    bootstrap_servers = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
    topic = os.environ.get("KAFKA_INCOMING_TOPIC", "claims.incoming")
    group_id = os.environ.get("KAFKA_CONSUMER_GROUP", "claim-processor-consumer")
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("LOCALSTACK_ENDPOINT") or "http://localhost:4566"
    region = os.environ.get("AWS_REGION", "us-east-1")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap_servers,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        group_id=group_id,
        value_deserializer=lambda payload: json.loads(payload.decode("utf-8")),
        key_deserializer=lambda payload: payload.decode("utf-8") if payload else None,
        consumer_timeout_ms=0,
    )
    stepfunctions = boto3.client("stepfunctions", endpoint_url=endpoint, region_name=region)
    state_machine_arn = resolve_state_machine_arn(stepfunctions, os.environ.get("STEP_FUNCTION_ARN"))

    processed = 0
    try:
        while True:
            records = consumer.poll(timeout_ms=args.poll_timeout, max_records=10)
            if not records:
                if args.max_messages and processed >= args.max_messages:
                    break
                time.sleep(0.5)
                continue

            for _, message_batch in records.items():
                for record in message_batch:
                    payload = record.value
                    execution_name = f"claim-{payload.get('documentId', 'unknown')}-{int(time.time() * 1000)}"
                    stepfunctions.start_execution(
                        stateMachineArn=state_machine_arn,
                        name=execution_name,
                        input=json.dumps(payload),
                    )
                    processed += 1
                    print(json.dumps({"startedExecution": execution_name, "documentId": payload.get("documentId")}))
                    consumer.commit()

            if args.max_messages and processed >= args.max_messages:
                break
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
