#!/usr/bin/env bash
set -euo pipefail

# Synthetic, local-only capability check. Every AWS call uses the loopback endpoint.
PORT="${EMULATOR_PORT:-9297}"
EMULATOR_DIR="${EMULATOR_DIR:-$HOME/.aws/emulator}"
RUN_DIR="$EMULATOR_DIR/$PORT"
ENDPOINT="https://127.0.0.1:$PORT"
REPORT_DIR="${REPORT_DIR:-reports/aws-emulator-smoke}"

for file in "$RUN_DIR/emulator_credentials" "$RUN_DIR/config" "$RUN_DIR/ca.pem"; do
  if [[ ! -r "$file" ]]; then
    echo "Missing emulator file: $file" >&2
    exit 2
  fi
done
command -v aws >/dev/null || { echo "AWS CLI is required" >&2; exit 2; }
command -v curl >/dev/null || { echo "curl is required" >&2; exit 2; }

export AWS_ENDPOINT_URL="$ENDPOINT"
export AWS_SHARED_CREDENTIALS_FILE="$RUN_DIR/emulator_credentials"
export AWS_CONFIG_FILE="$RUN_DIR/config"
export AWS_CA_BUNDLE="$RUN_DIR/ca.pem"
export AWS_EC2_METADATA_DISABLED=true
export AWS_DISABLE_HOST_PREFIX_INJECTION=true
export AWS_REGION=us-east-1
export AWS_DEFAULT_REGION=us-east-1
export AWS_PAGER=""
export AWS_MAX_ATTEMPTS=1
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE

mkdir -p "$REPORT_DIR"
curl -fsS --cacert "$AWS_CA_BUNDLE" "$ENDPOINT/__admin/health" > "$REPORT_DIR/health.txt"
account="$(aws --endpoint-url "$ENDPOINT" sts get-caller-identity --query Account --output text)"
if [[ "$account" != "000000000000" ]]; then
  echo "Refusing to continue: caller identity is not the emulator account" >&2
  exit 3
fi
printf '%s\n' "$account" > "$REPORT_DIR/account.txt"

suffix="$(date +%s)_$$"
table="payment_retry_lab_$suffix"
queue="payment-retry-lab-$suffix"

aws --endpoint-url "$ENDPOINT" dynamodb create-table \
  --table-name "$table" \
  --attribute-definitions AttributeName=operation_id,AttributeType=S \
  --key-schema AttributeName=operation_id,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST > "$REPORT_DIR/dynamodb-create.json"

aws --endpoint-url "$ENDPOINT" dynamodb wait table-exists --table-name "$table"
aws --endpoint-url "$ENDPOINT" dynamodb describe-table \
  --table-name "$table" > "$REPORT_DIR/dynamodb-active.json"

aws --endpoint-url "$ENDPOINT" dynamodb put-item \
  --table-name "$table" \
  --item '{"operation_id":{"S":"op-smoke"},"status":{"S":"PENDING"}}' \
  --condition-expression 'attribute_not_exists(operation_id)' \
  > "$REPORT_DIR/dynamodb-first-put.json"

if aws --endpoint-url "$ENDPOINT" dynamodb put-item \
    --table-name "$table" \
    --item '{"operation_id":{"S":"op-smoke"},"status":{"S":"PENDING"}}' \
    --condition-expression 'attribute_not_exists(operation_id)' \
    > "$REPORT_DIR/dynamodb-second-put.json" 2> "$REPORT_DIR/dynamodb-second-put.err"; then
  echo "Unexpected: duplicate conditional write succeeded" >&2
  exit 4
fi
if ! grep -q 'ConditionalCheckFailedException' "$REPORT_DIR/dynamodb-second-put.err"; then
  echo "Duplicate write failed for a reason other than the expected condition" >&2
  exit 4
fi

aws --endpoint-url "$ENDPOINT" dynamodb get-item \
  --table-name "$table" --key '{"operation_id":{"S":"op-smoke"}}' \
  > "$REPORT_DIR/dynamodb-get.json"

queue_url="$(aws --endpoint-url "$ENDPOINT" sqs create-queue \
  --queue-name "$queue" --query QueueUrl --output text)"
aws --endpoint-url "$ENDPOINT" sqs send-message \
  --queue-url "$queue_url" --message-body 'synthetic-op-smoke' \
  > "$REPORT_DIR/sqs-send.json"
aws --endpoint-url "$ENDPOINT" sqs receive-message \
  --queue-url "$queue_url" --wait-time-seconds 5 \
  > "$REPORT_DIR/sqs-receive.json"

if aws --endpoint-url "$ENDPOINT" lambda list-functions \
    > "$REPORT_DIR/lambda-list.json" 2> "$REPORT_DIR/lambda-list.err"; then
  lambda_status="API_LIST_OK"
else
  lambda_status="API_LIST_FAILED"
fi

printf 'emulator_account=%s\ntable=%s\nqueue=%s\nlambda=%s\n' \
  "$account" "$table" "$queue" "$lambda_status" | tee "$REPORT_DIR/summary.txt"
echo "This checks API reachability and a DynamoDB condition. It does not run a Lambda function or any payment scenario."
