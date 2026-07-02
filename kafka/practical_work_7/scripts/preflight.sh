#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env

for command_name in yc jq curl go docker openssl ssh scp; do
  require_command "$command_name"
done

if [[ -z "${ADMIN_CIDR:-}" || ! "$ADMIN_CIDR" =~ ^[0-9]{1,3}(\.[0-9]{1,3}){3}/32$ ]]; then
  echo "ADMIN_CIDR must contain your public IPv4 address in the form 203.0.113.10/32" >&2
  exit 1
fi

current_public_ip="$(curl -fsS https://api.ipify.org)"
if [[ "$ADMIN_CIDR" != "$current_public_ip/32" ]]; then
  echo "ADMIN_CIDR=$ADMIN_CIDR does not match current public IP $current_public_ip/32" >&2
  echo "Update .env before creating resources so SSH is not locked out." >&2
  exit 1
fi

expanded_ssh_key_path="${SSH_PUBLIC_KEY_PATH/#\~/$HOME}"
if [[ ! -f "$expanded_ssh_key_path" ]]; then
  echo "SSH public key not found: $expanded_ssh_key_path" >&2
  exit 1
fi

echo "yc CLI: $(yc version | head -n 1)"
echo "Authenticated subject: $(yc_cmd iam whoami)"
echo "Cloud: $(yc_cmd resource-manager cloud get "$YC_CLOUD_ID" --format json | jq -r '.name')"
echo "Folder: $(yc_cmd resource-manager folder get "$YC_FOLDER_ID" --format json | jq -r '.name')"

echo "Available zones:"
yc_cmd compute zone list --format json | jq -r '.[].id' | sort

echo "Existing paid resources with the pw7 prefix:"
yc_cmd managed-kafka cluster list --format json | jq -r '.[] | select(.name | startswith("pw7")) | "Kafka: \(.name) status=\(.status)"'
yc_cmd compute instance list --format json | jq -r '.[] | select(.name | startswith("pw7")) | "VM: \(.name) status=\(.status)"'

echo "Preflight checks passed. No resources were created."
