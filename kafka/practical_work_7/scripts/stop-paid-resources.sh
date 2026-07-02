#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env

echo "Stopping compute charges. Network disks will remain billable."
yc_cmd compute instance stop "$VM_NAME" --async || true
yc_cmd managed-kafka cluster stop "$KAFKA_CLUSTER_NAME" --async || true
