#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env
source "$PROJECT_DIR/.runtime.env"

ssh_options=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
remote="yc-user@$VM_PUBLIC_IP"

ssh "${ssh_options[@]}" "$remote" 'set -a; source /opt/pw7/.env; set +a; MESSAGE_COUNT=5 /opt/pw7/bin/consumer' \
  >"$PROJECT_DIR/artifacts/logs/consumer.log" 2>&1 &
consumer_pid=$!
sleep 3

ssh "${ssh_options[@]}" "$remote" 'set -a; source /opt/pw7/.env; set +a; MESSAGE_COUNT=5 /opt/pw7/bin/producer' 2>&1 \
  | tee "$PROJECT_DIR/artifacts/logs/producer.log"

wait "$consumer_pid"
cat "$PROJECT_DIR/artifacts/logs/consumer.log"

echo "Producer and consumer E2E check completed."
