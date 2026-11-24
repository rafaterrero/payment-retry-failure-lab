#!/usr/bin/env bash
set -euo pipefail

PORT="${EMULATOR_PORT:-9297}"
EMULATOR_DIR="${EMULATOR_DIR:-$HOME/.aws/emulator}"
RUN_DIR="$EMULATOR_DIR/$PORT"
ENDPOINT="https://127.0.0.1:$PORT"
REPORT_DIR="${REPORT_DIR:-reports/aws-emulator-p01}"
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
table="payment_retry_p01_$suffix"
queue="payment-retry-p01-$suffix"
role="payment-retry-p01-role-$suffix"
function="payment-retry-p01-function-$suffix"

aws --endpoint-url "$ENDPOINT" dynamodb create-table \
  --table-name "$table" \
  --attribute-definitions AttributeName=pk,AttributeType=S \
  --key-schema AttributeName=pk,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST > "$REPORT_DIR/table-create.json"
aws --endpoint-url "$ENDPOINT" dynamodb wait table-exists --table-name "$table"
aws --endpoint-url "$ENDPOINT" dynamodb put-item \
  --table-name "$table" \
  --item '{"pk":{"S":"OP#synthetic-guarded"},"status":{"S":"PENDING"},"amount_minor":{"N":"1000"},"idempotency_key":{"S":"op:synthetic-guarded"}}' \
  > "$REPORT_DIR/operation-create.json"

aws --endpoint-url "$ENDPOINT" dynamodb put-item --table-name "$table" \
  --item '{"pk":{"S":"OP#synthetic-recovery"},"status":{"S":"PENDING"},"amount_minor":{"N":"1000"}}' > "$REPORT_DIR/recovery-create.json"

queue_url="$(aws --endpoint-url "$ENDPOINT" sqs create-queue \
  --queue-name "$queue" --attributes VisibilityTimeout=12 \
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
  {"Effect":"Allow","Action":["dynamodb:GetItem","dynamodb:PutItem","dynamodb:UpdateItem"],"Resource":"arn:aws:dynamodb:us-east-1:000000000000:table/$table"}
]}
JSON
aws --endpoint-url "$ENDPOINT" iam put-role-policy \
  --role-name "$role" --policy-name payment-retry-p01 \
  --policy-document "file://$REPORT_DIR/worker-policy.json"

zip -q -j "$REPORT_DIR/p01-handler.zip" aws-emulator/p01_handler.py "$RUN_DIR/ca.pem"
cat > "$REPORT_DIR/environment.json" <<JSON
{"Variables":{"LAB_TABLE":"$table","EMULATOR_ENDPOINT":"$ENDPOINT"}}
JSON
aws --endpoint-url "$ENDPOINT" lambda create-function \
  --function-name "$function" --runtime python3.12 \
  --handler p01_handler.handler --role "$role_arn" \
  --timeout 2 --zip-file "fileb://$REPORT_DIR/p01-handler.zip" \
  --environment "file://$REPORT_DIR/environment.json" \
  > "$REPORT_DIR/lambda-create.json"
aws --endpoint-url "$ENDPOINT" lambda wait function-active --function-name "$function"

mapping="$(aws --endpoint-url "$ENDPOINT" lambda create-event-source-mapping \
  --function-name "$function" --event-source-arn "$queue_arn" \
  --batch-size 1 --query UUID --output text)"
printf '%s\n' "$mapping" > "$REPORT_DIR/mapping-uuid.txt"
for _ in $(seq 1 30); do
  state="$(aws --endpoint-url "$ENDPOINT" lambda get-event-source-mapping \
    --uuid "$mapping" --query State --output text)"
  [[ "$state" == "Enabled" ]] && break
  sleep 1
done
[[ "$state" == "Enabled" ]] || { echo "Mapping did not become Enabled: $state" >&2; exit 4; }

aws --endpoint-url "$ENDPOINT" sqs send-message \
  --queue-url "$queue_url" \
  --message-body '{"mode":"naive","operation_id":"synthetic-naive"}' \
  > "$REPORT_DIR/naive-send.json"
aws --endpoint-url "$ENDPOINT" sqs send-message \
  --queue-url "$queue_url" \
  --message-body '{"mode":"guarded","operation_id":"synthetic-guarded"}' \
  > "$REPORT_DIR/guarded-send.json"

aws --endpoint-url "$ENDPOINT" sqs send-message --queue-url "$queue_url" \
  --message-body '{"mode":"guarded","operation_id":"synthetic-recovery","fault":"before_unknown"}' > "$REPORT_DIR/recovery-send.json"

get_field() {
  aws --endpoint-url "$ENDPOINT" dynamodb get-item \
    --table-name "$table" --key "{\"pk\":{\"S\":\"$1\"}}" \
    --consistent-read --query "Item.$2" --output text
}
for _ in $(seq 1 90); do
  naive_charges="$(get_field 'LEDGER#synthetic-naive' 'charge_count.N')"
  guarded_charges="$(get_field 'LEDGER#synthetic-guarded' 'charge_count.N')"
  guarded_status="$(get_field 'OP#synthetic-guarded' 'status.S')"
  recovery_status="$(get_field 'OP#synthetic-recovery' 'status.S')"
  if [[ "$recovery_status" == "SUCCEEDED" && "$naive_charges" == "2" && "$guarded_charges" == "1" && "$guarded_status" == "SUCCEEDED" ]]; then
    break
  fi
  sleep 1
done

for key in 'ATTEMPTS#synthetic-naive' 'ATTEMPTS#synthetic-guarded' 'LEDGER#synthetic-naive' 'LEDGER#synthetic-guarded' 'OP#synthetic-guarded' 'OP#synthetic-recovery' 'LEDGER#synthetic-recovery' 'ATTEMPTS#synthetic-recovery'; do
  name="${key//\#/-}"
  aws --endpoint-url "$ENDPOINT" dynamodb get-item \
    --table-name "$table" --key "{\"pk\":{\"S\":\"$key\"}}" \
    --consistent-read > "$REPORT_DIR/$name.json"
done

naive_attempts="$(get_field 'ATTEMPTS#synthetic-naive' 'attempts.N')"
guarded_attempts="$(get_field 'ATTEMPTS#synthetic-guarded' 'attempts.N')"
printf 'emulator_account=%s\nnaive_attempts=%s\nnaive_charges=%s\nguarded_attempts=%s\nguarded_charges=%s\nguarded_status=%s\n' \
  "$account" "$naive_attempts" "$naive_charges" "$guarded_attempts" "$guarded_charges" "$guarded_status" \
  | tee "$REPORT_DIR/summary.txt"
if [[ "$naive_charges" != "2" || "$guarded_charges" != "1" || "$guarded_status" != "SUCCEEDED" ]]; then
  echo "P01 did not reach the expected synthetic state" >&2
  exit 5
fi
echo "P01 observed through SQS -> Lambda -> DynamoDB with a synthetic ledger."

recovery_charges="$(get_field 'LEDGER#synthetic-recovery' 'charge_count.N')"
recovery_attempts="$(get_field 'ATTEMPTS#synthetic-recovery' 'attempts.N')"
[[ "$naive_attempts" == "2" && "$guarded_attempts" == "2" && "$recovery_attempts" == "2" && "$recovery_charges" == "1" && "$recovery_status" == "SUCCEEDED" ]] || { echo "Attempt/recovery validation failed" >&2; exit 6; }
printf 'recovery_attempts=%s\nrecovery_charges=%s\nrecovery_status=%s\n' "$recovery_attempts" "$recovery_charges" "$recovery_status" | tee -a "$REPORT_DIR/summary.txt"
