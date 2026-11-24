#!/usr/bin/env bash
set -euo pipefail
PORT="${EMULATOR_PORT:-9297}"
EMULATOR_DIR="${EMULATOR_DIR:-$HOME/.aws/emulator}"
RUN_DIR="$EMULATOR_DIR/$PORT"
ENDPOINT="https://127.0.0.1:$PORT"
REPORT_DIR="${REPORT_DIR:-reports/aws-emulator-sqs-lambda}"
export AWS_ENDPOINT_URL="$ENDPOINT"
export AWS_SHARED_CREDENTIALS_FILE="$RUN_DIR/emulator_credentials"
export AWS_CONFIG_FILE="$RUN_DIR/config"
export AWS_CA_BUNDLE="$RUN_DIR/ca.pem"
export AWS_EC2_METADATA_DISABLED=true AWS_DISABLE_HOST_PREFIX_INJECTION=true
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER="" AWS_MAX_ATTEMPTS=1
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE
account="$(aws --endpoint-url "$ENDPOINT" sts get-caller-identity --query Account --output text)"
[[ "$account" == "000000000000" ]] || { echo "Refusing non-emulator account" >&2; exit 3; }
function="$(python3 - "$REPORT_DIR/lambda-create.json" <<'PY'
import json, sys
with open(sys.argv[1], encoding="utf-8") as source:
    print(json.load(source)["FunctionName"])
PY
)"
zip -q -j "$REPORT_DIR/lambda-sqs-probe.zip" aws-emulator/lambda_sqs_probe.py "$RUN_DIR/ca.pem"
aws --endpoint-url "$ENDPOINT" lambda update-function-code \
  --function-name "$function" \
  --zip-file "fileb://$REPORT_DIR/lambda-sqs-probe.zip" \
  > "$REPORT_DIR/lambda-code-update.json"
aws --endpoint-url "$ENDPOINT" lambda wait function-updated --function-name "$function"
aws --endpoint-url "$ENDPOINT" lambda invoke \
  --function-name "$function" --cli-binary-format raw-in-base64-out \
  --payload '{"probe_env":true}' \
  "$REPORT_DIR/lambda-env-response.json" > "$REPORT_DIR/lambda-env-invoke.json"
cat "$REPORT_DIR/lambda-env-response.json"
