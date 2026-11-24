#!/usr/bin/env bash
set -euo pipefail

PORT="${EMULATOR_PORT:-9297}"
EMULATOR_DIR="${EMULATOR_DIR:-$HOME/.aws/emulator}"
RUN_DIR="$EMULATOR_DIR/$PORT"
ENDPOINT="https://127.0.0.1:$PORT"
REPORT_DIR="${REPORT_DIR:-reports/aws-emulator-lambda}"

for file in "$RUN_DIR/emulator_credentials" "$RUN_DIR/config" "$RUN_DIR/ca.pem"; do
  [[ -r "$file" ]] || { echo "Missing emulator file: $file" >&2; exit 2; }
done
command -v aws >/dev/null || { echo "AWS CLI is required" >&2; exit 2; }
command -v zip >/dev/null || { echo "zip is required" >&2; exit 2; }

export AWS_ENDPOINT_URL="$ENDPOINT"
export AWS_SHARED_CREDENTIALS_FILE="$RUN_DIR/emulator_credentials"
export AWS_CONFIG_FILE="$RUN_DIR/config"
export AWS_CA_BUNDLE="$RUN_DIR/ca.pem"
export AWS_EC2_METADATA_DISABLED=true
export AWS_DISABLE_HOST_PREFIX_INJECTION=true
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER="" AWS_MAX_ATTEMPTS=1
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE

mkdir -p "$REPORT_DIR"
account="$(aws --endpoint-url "$ENDPOINT" sts get-caller-identity --query Account --output text)"
[[ "$account" == "000000000000" ]] || { echo "Refusing non-emulator account" >&2; exit 3; }

suffix="$(date +%s)_$$"
role="payment-retry-lab-role-$suffix"
function="payment-retry-lab-function-$suffix"
cat > "$REPORT_DIR/assume-role.json" <<'JSON'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
JSON
role_arn="$(aws --endpoint-url "$ENDPOINT" iam create-role \
  --role-name "$role" \
  --assume-role-policy-document "file://$REPORT_DIR/assume-role.json" \
  --query Role.Arn --output text)"

zip -q -j "$REPORT_DIR/lambda-smoke.zip" aws-emulator/lambda_smoke.py
aws --endpoint-url "$ENDPOINT" lambda create-function \
  --function-name "$function" --runtime python3.12 \
  --handler lambda_smoke.handler --role "$role_arn" \
  --zip-file "fileb://$REPORT_DIR/lambda-smoke.zip" \
  > "$REPORT_DIR/lambda-create.json"
aws --endpoint-url "$ENDPOINT" lambda wait function-active --function-name "$function"
aws --endpoint-url "$ENDPOINT" lambda invoke \
  --function-name "$function" --cli-binary-format raw-in-base64-out \
  --payload '{"operation_id":"synthetic-op-smoke"}' \
  "$REPORT_DIR/lambda-response.json" > "$REPORT_DIR/lambda-invoke.json"
python3 - "$REPORT_DIR/lambda-response.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    response = json.load(source)
assert response.get("ok") is True, response
assert response.get("operation_id") == "synthetic-op-smoke", response
assert response.get("record_count") == 0, response
assert isinstance(response.get("boto3_available"), bool), response
print("Lambda direct invoke returned the expected synthetic response")
print("boto3_available_in_lambda=" + str(response["boto3_available"]).lower())
PY
printf 'emulator_account=%s\nfunction=%s\n' "$account" "$function" | tee "$REPORT_DIR/summary.txt"
echo "This checks direct Lambda execution only; SQS event source mapping is not yet tested."
