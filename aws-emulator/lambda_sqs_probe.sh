#!/usr/bin/env bash
set -euo pipefail

PORT="${EMULATOR_PORT:-9297}"
EMULATOR_DIR="${EMULATOR_DIR:-$HOME/.aws/emulator}"
RUN_DIR="$EMULATOR_DIR/$PORT"
ENDPOINT="https://127.0.0.1:$PORT"
REPORT_DIR="${REPORT_DIR:-reports/aws-emulator-sqs-lambda}"
for file in "$RUN_DIR/emulator_credentials" "$RUN_DIR/config" "$RUN_DIR/ca.pem"; do
  [[ -r "$file" ]] || { echo "Missing emulator file: $file" >&2; exit 2; }
done
export AWS_ENDPOINT_URL="$ENDPOINT"
export AWS_SHARED_CREDENTIALS_FILE="$RUN_DIR/emulator_credentials"
export AWS_CONFIG_FILE="$RUN_DIR/config"
export AWS_CA_BUNDLE="$RUN_DIR/ca.pem"
export AWS_EC2_METADATA_DISABLED=true AWS_DISABLE_HOST_PREFIX_INJECTION=true
export AWS_REGION=us-east-1 AWS_DEFAULT_REGION=us-east-1 AWS_PAGER="" AWS_MAX_ATTEMPTS=1
unset AWS_ACCESS_KEY_ID AWS_SECRET_ACCESS_KEY AWS_SESSION_TOKEN AWS_PROFILE

mkdir -p "$REPORT_DIR"
account="$(aws --endpoint-url "$ENDPOINT" sts get-caller-identity --query Account --output text)"
[[ "$account" == "000000000000" ]] || { echo "Refusing non-emulator account" >&2; exit 3; }

suffix="$(date +%s)_$$"
table="payment_retry_probe_$suffix"
queue="payment-retry-probe-$suffix"
role="payment-retry-probe-role-$suffix"
function="payment-retry-probe-function-$suffix"

aws --endpoint-url "$ENDPOINT" dynamodb create-table \
  --table-name "$table" \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST > "$REPORT_DIR/table-create.json"
aws --endpoint-url "$ENDPOINT" dynamodb wait table-exists --table-name "$table"

queue_url="$(aws --endpoint-url "$ENDPOINT" sqs create-queue \
  --queue-name "$queue" \
  --attributes VisibilityTimeout=30 \
  --query QueueUrl --output text)"
queue_arn="$(aws --endpoint-url "$ENDPOINT" sqs get-queue-attributes \
  --queue-url "$queue_url" --attribute-names QueueArn \
  --query Attributes.QueueArn --output text)"

cat > "$REPORT_DIR/assume-role.json" <<'JSON'
{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Principal":{"Service":"lambda.amazonaws.com"},"Action":"sts:AssumeRole"}]}
JSON
role_arn="$(aws --endpoint-url "$ENDPOINT" iam create-role \
  --role-name "$role" \
  --assume-role-policy-document "file://$REPORT_DIR/assume-role.json" \
  --query Role.Arn --output text)"
cat > "$REPORT_DIR/worker-policy.json" <<JSON
{"Version":"2012-10-17","Statement":[
  {"Effect":"Allow","Action":["sqs:ReceiveMessage","sqs:DeleteMessage","sqs:GetQueueAttributes","sqs:ChangeMessageVisibility"],"Resource":"$queue_arn"},
  {"Effect":"Allow","Action":["dynamodb:PutItem"],"Resource":"arn:aws:dynamodb:us-east-1:000000000000:table/$table"}
]}
JSON
aws --endpoint-url "$ENDPOINT" iam put-role-policy \
  --role-name "$role" --policy-name payment-retry-probe \
  --policy-document "file://$REPORT_DIR/worker-policy.json"

zip -q -j "$REPORT_DIR/lambda-sqs-probe.zip" aws-emulator/lambda_sqs_probe.py "$RUN_DIR/ca.pem"
cat > "$REPORT_DIR/environment.json" <<JSON
{"Variables":{"LAB_TABLE":"$table","EMULATOR_ENDPOINT":"$ENDPOINT"}}
JSON
aws --endpoint-url "$ENDPOINT" lambda create-function \
  --function-name "$function" --runtime python3.12 \
  --handler lambda_sqs_probe.handler --role "$role_arn" \
  --timeout 3 --zip-file "fileb://$REPORT_DIR/lambda-sqs-probe.zip" \
  --environment "file://$REPORT_DIR/environment.json" \
  > "$REPORT_DIR/lambda-create.json"
aws --endpoint-url "$ENDPOINT" lambda wait function-active --function-name "$function"

aws --endpoint-url "$ENDPOINT" lambda create-event-source-mapping \
  --function-name "$function" --event-source-arn "$queue_arn" \
  --batch-size 1 > "$REPORT_DIR/event-source-mapping.json"

message_id="$(aws --endpoint-url "$ENDPOINT" sqs send-message \
  --queue-url "$queue_url" --message-body 'synthetic-op-sqs-lambda' \
  --query MessageId --output text)"
printf '%s\n' "$message_id" > "$REPORT_DIR/message-id.txt"
for _ in $(seq 1 30); do
  got="$(aws --endpoint-url "$ENDPOINT" dynamodb get-item \
    --table-name "$table" --key "{\"pk\":{\"S\":\"$message_id\"}}" \
    --consistent-read --query Item.body.S --output text)"
  if [[ "$got" == "synthetic-op-sqs-lambda" ]]; then
    printf 'emulator_account=%s\nmessage_id=%s\nobserved_body=%s\n' \
      "$account" "$message_id" "$got" | tee "$REPORT_DIR/summary.txt"
    echo "Observed SQS -> Lambda -> DynamoDB for one synthetic message."
    exit 0
  fi
  sleep 1
done
echo "No matching DynamoDB item observed after 30 seconds" >&2
exit 5
