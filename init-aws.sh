#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BUILD_DIR="${ROOT_DIR}/.localstack/build"
LAMBDA_SRC_DIR="${ROOT_DIR}/lambdas"
STATE_MACHINE_TEMPLATE="${ROOT_DIR}/state-machine.asl.json"
TMP_STATE_MACHINE="${BUILD_DIR}/claim-processor.rendered.json"

AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"
AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-us-east-1}"
AWS_REGION="${AWS_REGION:-${AWS_DEFAULT_REGION}}"
LOCALSTACK_ENDPOINT="${LOCALSTACK_ENDPOINT:-http://localhost:4566}"
KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9094}"

export AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_DEFAULT_REGION AWS_REGION
export AWS_ENDPOINT_URL="${LOCALSTACK_ENDPOINT}"
export LOCALSTACK_ENDPOINT KAFKA_BOOTSTRAP_SERVERS

mkdir -p "${BUILD_DIR}"

aws() {
  awslocal "$@"
}

ensure_role() {
  local role_name="$1"
  local trust_policy="$2"
  if ! aws iam get-role --role-name "${role_name}" >/dev/null 2>&1; then
    aws iam create-role --role-name "${role_name}" --assume-role-policy-document "${trust_policy}" >/dev/null
  fi
  aws iam get-role --role-name "${role_name}" --query 'Role.Arn' --output text
}

create_zip() {
  local source_file="$1"
  local zip_file="$2"
  python - <<'PY' "$source_file" "$zip_file"
import sys
import zipfile
from pathlib import Path

source = Path(sys.argv[1])
zip_path = Path(sys.argv[2])
zip_path.parent.mkdir(parents=True, exist_ok=True)
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    zf.write(source, arcname=source.name)
PY
}

ensure_dynamodb_table() {
  if ! aws dynamodb describe-table --table-name claims >/dev/null 2>&1; then
    aws dynamodb create-table \
      --table-name claims \
      --attribute-definitions AttributeName=documentId,AttributeType=S \
      --key-schema AttributeName=documentId,KeyType=HASH \
      --billing-mode PAY_PER_REQUEST >/dev/null
  fi
}

ensure_sns_topic() {
  aws sns create-topic --name claims-dead-letter-topic --query 'TopicArn' --output text
}

package_lambda() {
  local name="$1"
  local source_file="$2"
  local output_zip="${BUILD_DIR}/${name}.zip"
  create_zip "${source_file}" "${output_zip}"
  echo "${output_zip}"
}

package_final_store_lambda() {
  local output_dir="${BUILD_DIR}/final_store_pkg"
  local output_zip="${BUILD_DIR}/final_store.zip"
  rm -rf "${output_dir}" "${output_zip}"
  mkdir -p "${output_dir}"
  python -m pip install --disable-pip-version-check --quiet --target "${output_dir}" kafka-python >/dev/null
  cp "${LAMBDA_SRC_DIR}/final_store.py" "${output_dir}/final_store.py"
  python - <<'PY' "${output_dir}" "${output_zip}"
import sys
import zipfile
from pathlib import Path

source_dir = Path(sys.argv[1])
zip_path = Path(sys.argv[2])
with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as zf:
    for path in source_dir.rglob("*"):
        if path.is_file():
            zf.write(path, arcname=str(path.relative_to(source_dir)))
PY
  echo "${output_zip}"
}

ensure_lambda() {
  local function_name="$1"
  local handler="$2"
  local zip_file="$3"
  local role_arn="$4"

  if aws lambda get-function --function-name "${function_name}" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "${function_name}" --zip-file "fileb://${zip_file}" >/dev/null
  else
    aws lambda create-function \
      --function-name "${function_name}" \
      --runtime python3.11 \
      --handler "${handler}" \
      --role "${role_arn}" \
      --zip-file "fileb://${zip_file}" \
      --environment "Variables={AWS_ENDPOINT_URL=${LOCALSTACK_ENDPOINT},LOCALSTACK_ENDPOINT=${LOCALSTACK_ENDPOINT},AWS_REGION=${AWS_REGION},DYNAMODB_TABLE_NAME=claims,KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS},KAFKA_PROCESSED_TOPIC=claims.processed}" \
      >/dev/null
  fi
}

lambda_role_policy="${BUILD_DIR}/lambda-trust.json"
stepfunctions_role_policy="${BUILD_DIR}/stepfunctions-trust.json"

cat >"${lambda_role_policy}" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

cat >"${stepfunctions_role_policy}" <<'JSON'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Principal": {
        "Service": "states.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}
JSON

ensure_dynamodb_table
CLAIMS_DEAD_LETTER_TOPIC_ARN="$(ensure_sns_topic)"

LAMBDA_ROLE_ARN="$(ensure_role localstack-lambda-role "file://${lambda_role_policy}")"
STEP_FUNCTIONS_ROLE_ARN="$(ensure_role localstack-stepfunctions-role "file://${stepfunctions_role_policy}")"

VIRUS_SCAN_ZIP="$(package_lambda virus_scan "${LAMBDA_SRC_DIR}/virus_scan.py")"
OCR_EXTRACT_ZIP="$(package_lambda ocr_extract "${LAMBDA_SRC_DIR}/ocr_extract.py")"
FINAL_STORE_ZIP="$(package_final_store_lambda)"

ensure_lambda VirusScan virus_scan.lambda_handler "${VIRUS_SCAN_ZIP}" "${LAMBDA_ROLE_ARN}"
ensure_lambda OCRExtract ocr_extract.lambda_handler "${OCR_EXTRACT_ZIP}" "${LAMBDA_ROLE_ARN}"
ensure_lambda FinalStore final_store.lambda_handler "${FINAL_STORE_ZIP}" "${LAMBDA_ROLE_ARN}"

VIRUS_SCAN_LAMBDA_ARN="$(aws lambda get-function --function-name VirusScan --query 'Configuration.FunctionArn' --output text)"
OCR_EXTRACT_LAMBDA_ARN="$(aws lambda get-function --function-name OCRExtract --query 'Configuration.FunctionArn' --output text)"
FINAL_STORE_LAMBDA_ARN="$(aws lambda get-function --function-name FinalStore --query 'Configuration.FunctionArn' --output text)"

CLAIMS_DEAD_LETTER_TOPIC_ARN="$(aws sns list-topics --query "Topics[?contains(TopicArn, 'claims-dead-letter-topic')].TopicArn | [0]" --output text)"

python - <<'PY' "$STATE_MACHINE_TEMPLATE" "$TMP_STATE_MACHINE" "$VIRUS_SCAN_LAMBDA_ARN" "$OCR_EXTRACT_LAMBDA_ARN" "$FINAL_STORE_LAMBDA_ARN" "$CLAIMS_DEAD_LETTER_TOPIC_ARN"
import json
import sys
from pathlib import Path

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
virus_arn, ocr_arn, final_arn, dead_letter_arn = sys.argv[3:7]

definition = template_path.read_text(encoding="utf-8")
definition = definition.replace("__VIRUS_SCAN_LAMBDA_ARN__", virus_arn)
definition = definition.replace("__OCR_EXTRACT_LAMBDA_ARN__", ocr_arn)
definition = definition.replace("__FINAL_STORE_LAMBDA_ARN__", final_arn)
definition = definition.replace("__CLAIMS_DEAD_LETTER_TOPIC_ARN__", dead_letter_arn)
json.loads(definition)
output_path.write_text(definition, encoding="utf-8")
PY

STATE_MACHINE_ARN="$(aws stepfunctions create-state-machine --name ClaimProcessor --definition "file://${TMP_STATE_MACHINE}" --role-arn "${STEP_FUNCTIONS_ROLE_ARN}" --query 'stateMachineArn' --output text 2>/dev/null || true)"
if [[ -z "${STATE_MACHINE_ARN}" || "${STATE_MACHINE_ARN}" == "None" ]]; then
  STATE_MACHINE_ARN="$(aws stepfunctions list-state-machines --query "stateMachines[?name=='ClaimProcessor'].stateMachineArn | [0]" --output text)"
fi

cat >"${ROOT_DIR}/.localstack/runtime.env" <<EOF
AWS_ENDPOINT_URL=${LOCALSTACK_ENDPOINT}
LOCALSTACK_ENDPOINT=${LOCALSTACK_ENDPOINT}
AWS_REGION=${AWS_REGION}
KAFKA_BOOTSTRAP_SERVERS=${KAFKA_BOOTSTRAP_SERVERS}
STEP_FUNCTION_ARN=${STATE_MACHINE_ARN}
SNS_DEAD_LETTER_TOPIC_ARN=${CLAIMS_DEAD_LETTER_TOPIC_ARN}
EOF

echo "LocalStack resources are ready."
echo "Step Functions ARN: ${STATE_MACHINE_ARN}"
echo "Dead-letter topic ARN: ${CLAIMS_DEAD_LETTER_TOPIC_ARN}"
