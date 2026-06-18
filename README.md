# Serverless Document Processing Pipeline

This repository implements a local, event-driven document processing pipeline using AWS Step Functions for orchestration, Kafka on Kubernetes via Strimzi for ingestion and completion events, and LocalStack for local AWS service simulation.

## Architecture

- Kafka topic `claims.incoming` receives claim/document intake events.
- `consumer.py` reads incoming Kafka messages and starts a Step Functions execution.
- The Step Functions workflow runs `VirusScan`, `OCRExtract`, `FraudDetection`, `HumanReviewQueue`, and `FinalStore`.
- `FinalStore` writes idempotently to DynamoDB and publishes a completion event to `claims.processed`.
- Any unhandled failure is routed to an SNS dead-letter topic.
- `generate_report.py` reads execution history from LocalStack and prints p50/p95 state latency metrics as JSON.

## Repository Layout

- [docker-compose.yml](docker-compose.yml) starts LocalStack.
- [init-aws.sh](init-aws.sh) provisions DynamoDB, SNS, Lambda mocks, and the Step Functions state machine.
- [k8s/kafka-cluster.yml](k8s/kafka-cluster.yml) defines the Strimzi Kafka cluster.
- [k8s/kafka-topics.yml](k8s/kafka-topics.yml) defines the incoming and processed topics.
- [state-machine.asl.json](state-machine.asl.json) contains the Step Functions definition.
- [src/pipeline](src/pipeline) contains the packaged Python application logic.
- [producer.py](producer.py), [consumer.py](consumer.py), and [generate_report.py](generate_report.py) are runnable entry points.
- [lambdas](lambdas) contains the Lambda handlers used by LocalStack.

## Prerequisites

- Docker and Docker Compose
- Minikube
- kubectl
- Python 3.11+
- awslocal or the LocalStack CLI utilities available inside the LocalStack container

## Setup

1. Copy the sample environment file and adjust values if needed.
2. Start LocalStack:

```bash
docker compose up -d
```

3. Wait until the LocalStack healthcheck passes.
4. Provision the AWS resources:

```bash
chmod +x init-aws.sh
./init-aws.sh
```

5. Start Minikube and install Strimzi.
6. Apply the Kafka manifests:

```bash
kubectl apply -f k8s/kafka-cluster.yml
kubectl apply -f k8s/kafka-topics.yml
```

## Running the pipeline

### Producer

Send a normal document:

```bash
python producer.py
```

Send a fraud-path document:

```bash
python producer.py --fraud
```

Send the same document twice for idempotency testing:

```bash
python producer.py --duplicate
```

### Consumer

Run the Kafka consumer in one terminal:

```bash
python consumer.py
```

### Reporting

Generate latency metrics after a few executions complete:

```bash
python generate_report.py
```

## Docker Images

If you want to containerize the host-side producer and consumer, build with:

```bash
docker build -f Dockerfile.producer -t task-kafka-producer .
docker build -f Dockerfile.consumer -t task-kafka-consumer .
```

## Environment Variables

See [.env.example](.env.example) for the full list. The key values are:

- `AWS_ENDPOINT_URL` and `LOCALSTACK_ENDPOINT`
- `AWS_REGION`
- `KAFKA_BOOTSTRAP_SERVERS`
- `KAFKA_INCOMING_TOPIC`
- `KAFKA_PROCESSED_TOPIC`
- `STEP_FUNCTION_NAME` and `STEP_FUNCTION_ARN`
- `DYNAMODB_TABLE_NAME`

## Validation Notes

- `submission.json` provides the document ID used by the idempotency test.
- `FinalStore` uses `attribute_not_exists(documentId)` to avoid duplicate records.
- The fraud branch is triggered when the OCR output contains the word `fraud`.
