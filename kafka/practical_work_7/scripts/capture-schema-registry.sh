#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env
source "$PROJECT_DIR/.runtime.env"

registry_host="${SCHEMA_REGISTRY_URL#https://}"
registry_host="${registry_host%:443}"
ssh_options=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
remote="yc-user@$VM_PUBLIC_IP"

ssh "${ssh_options[@]}" "$remote" \
  "curl --silent --show-error --fail --cacert /opt/pw7/certs/CA.pem --user '$KAFKA_USERNAME:$KAFKA_PASSWORD' 'https://$registry_host/subjects'" \
  | tee "$PROJECT_DIR/artifacts/logs/schema-registry-subjects.json"
echo
ssh "${ssh_options[@]}" "$remote" \
  "curl --silent --show-error --fail --cacert /opt/pw7/certs/CA.pem --user '$KAFKA_USERNAME:$KAFKA_PASSWORD' 'https://$registry_host/subjects/$EVENT_TOPIC-value/versions'" \
  | tee "$PROJECT_DIR/artifacts/logs/schema-registry-versions.json"
echo
