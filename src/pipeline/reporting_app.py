import json
import os
import statistics
from collections import defaultdict
from datetime import datetime
from typing import Any

import boto3


TARGET_STATES = ("VirusScan", "OCRExtract", "FinalStore")


def resolve_state_machine_arn(stepfunctions_client: Any, explicit_arn: str | None) -> str:
    if explicit_arn:
        return explicit_arn

    name = os.environ.get("STEP_FUNCTION_NAME", "ClaimProcessor")
    response = stepfunctions_client.list_state_machines()
    for state_machine in response.get("stateMachines", []):
        if state_machine.get("name") == name:
            return state_machine["stateMachineArn"]
    raise RuntimeError(f"Unable to find state machine named {name}")


def percentile(values: list[float], percentile_rank: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    rank = (len(ordered) - 1) * percentile_rank
    lower = int(rank)
    upper = min(lower + 1, len(ordered) - 1)
    weight = rank - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def build_histories(stepfunctions_client: Any, state_machine_arn: str) -> list[dict[str, Any]]:
    executions: list[dict[str, Any]] = []
    next_token = None
    while True:
        kwargs = {"stateMachineArn": state_machine_arn, "maxResults": 100}
        if next_token:
            kwargs["nextToken"] = next_token
        response = stepfunctions_client.list_executions(**kwargs)
        executions.extend(
            execution
            for execution in response.get("executions", [])
            if execution.get("status") == "SUCCEEDED"
        )
        next_token = response.get("nextToken")
        if not next_token:
            break
    return executions


def extract_latencies(stepfunctions_client: Any, execution_arn: str) -> dict[str, float]:
    history = stepfunctions_client.get_execution_history(executionArn=execution_arn, includeExecutionData=True)
    started_at: dict[str, datetime] = {}
    latencies: dict[str, float] = {}

    for event in history.get("events", []):
        event_type = event.get("type", "")
        timestamp = event.get("timestamp")
        if event_type in {"TaskStateEntered", "TaskStateExited"}:
            details_key = "stateEnteredEventDetails" if event_type == "TaskStateEntered" else "stateExitedEventDetails"
            details = event.get(details_key, {})
            state_name = details.get("name")
            if state_name in TARGET_STATES:
                if event_type == "TaskStateEntered":
                    started_at[state_name] = timestamp
                elif state_name in started_at:
                    delta = timestamp - started_at[state_name]
                    latencies[state_name] = delta.total_seconds() * 1000.0

    return latencies


def main() -> None:
    endpoint = os.environ.get("AWS_ENDPOINT_URL") or os.environ.get("LOCALSTACK_ENDPOINT") or "http://localhost:4566"
    region = os.environ.get("AWS_REGION", "us-east-1")
    stepfunctions = boto3.client("stepfunctions", endpoint_url=endpoint, region_name=region)
    state_machine_arn = resolve_state_machine_arn(stepfunctions, os.environ.get("STEP_FUNCTION_ARN"))

    executions = build_histories(stepfunctions, state_machine_arn)
    aggregate: dict[str, list[float]] = defaultdict(list)

    for execution in executions:
        latencies = extract_latencies(stepfunctions, execution["executionArn"])
        for state_name, latency in latencies.items():
            aggregate[state_name].append(latency)

    report = {
        "report_summary": {
            "total_executions_analyzed": len(executions),
        },
        "state_latency_ms": {
            state_name: {
                "p50": round(statistics.median(aggregate[state_name]), 2) if aggregate[state_name] else 0.0,
                "p95": round(percentile(aggregate[state_name], 0.95), 2) if aggregate[state_name] else 0.0,
            }
            for state_name in TARGET_STATES
        },
    }
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
