#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env

runtime_file="$PROJECT_DIR/.runtime.env"
if [[ ! -f "$runtime_file" ]]; then
  echo "Run scripts/configure-cloud.sh first." >&2
  exit 1
fi
# shellcheck disable=SC1090
source "$runtime_file"

"$PROJECT_DIR/scripts/build-linux.sh"
"$PROJECT_DIR/scripts/render-client-config.sh"

ssh_options=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
ssh "${ssh_options[@]}" "yc-user@$VM_PUBLIC_IP" 'cloud-init status --wait && test -f /opt/pw7/cloud-init-complete'

scp "${ssh_options[@]}" \
  "$PROJECT_DIR/build/producer" \
  "$PROJECT_DIR/build/consumer" \
  "yc-user@$VM_PUBLIC_IP:/opt/pw7/bin/"
scp "${ssh_options[@]}" \
  "$PROJECT_DIR/schemas/pw7-event.avsc" \
  "yc-user@$VM_PUBLIC_IP:/opt/pw7/schemas-pw7-event.avsc"
scp "${ssh_options[@]}" \
  "$PROJECT_DIR/config/nifi/docker-compose.yaml" \
  "yc-user@$VM_PUBLIC_IP:/opt/pw7/nifi/docker-compose.yaml"
scp "${ssh_options[@]}" \
  "$PROJECT_DIR/config/kafka/client.properties" \
  "yc-user@$VM_PUBLIC_IP:/opt/pw7/client.properties"
scp "${ssh_options[@]}" \
  "$runtime_file" \
  "yc-user@$VM_PUBLIC_IP:/opt/pw7/.env"

ssh "${ssh_options[@]}" "yc-user@$VM_PUBLIC_IP" \
  'mkdir -p /opt/pw7/schemas && mv /opt/pw7/schemas-pw7-event.avsc /opt/pw7/schemas/pw7-event.avsc && chmod 700 /opt/pw7/bin/* && chmod 600 /opt/pw7/.env /opt/pw7/client.properties'

echo "Application, NiFi config and runtime settings deployed to $VM_PUBLIC_IP"
