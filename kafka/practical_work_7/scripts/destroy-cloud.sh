#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env

if [[ "${CONFIRM_DESTROY:-}" != "DELETE" ]]; then
  cat >&2 <<EOF
This deletes only practical work 7 resources:
  $KAFKA_CLUSTER_NAME, $VM_NAME, project security groups, subnets and network.

Run:
  CONFIRM_DESTROY=DELETE scripts/destroy-cloud.sh
EOF
  exit 2
fi

yc_cmd managed-kafka cluster delete "$KAFKA_CLUSTER_NAME" || true
yc_cmd compute instance delete "$VM_NAME" || true
yc_cmd vpc security-group delete "$KAFKA_SECURITY_GROUP_NAME" || true
yc_cmd vpc security-group delete "$VM_SECURITY_GROUP_NAME" || true
yc_cmd vpc subnet delete "$SUBNET_D_NAME" || true
yc_cmd vpc subnet delete "$SUBNET_B_NAME" || true
yc_cmd vpc subnet delete "$SUBNET_A_NAME" || true
yc_cmd vpc network delete "$NETWORK_NAME" || true

echo "Remaining pw7 resources:"
yc_cmd managed-kafka cluster list --format json | jq -r '.[] | select(.name | startswith("pw7")) | .name'
yc_cmd compute instance list --format json | jq -r '.[] | select(.name | startswith("pw7")) | .name'
yc_cmd compute disk list --format json | jq -r '.[] | select(.name | startswith("pw7")) | .name'
