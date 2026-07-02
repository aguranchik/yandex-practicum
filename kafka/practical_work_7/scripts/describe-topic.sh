#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env
source "$PROJECT_DIR/.runtime.env"

ssh_options=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
remote="yc-user@$VM_PUBLIC_IP"

ssh "${ssh_options[@]}" "$remote" \
  "docker run --rm --network host -v /opt/pw7:/work:ro apache/kafka:3.9.1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server '$KAFKA_BROKERS' --command-config /work/client.properties --describe --topic '$EVENT_TOPIC'" \
  | tee "$PROJECT_DIR/artifacts/logs/kafka-topics-describe.log"
