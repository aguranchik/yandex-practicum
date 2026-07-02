#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env
source "$PROJECT_DIR/.runtime.env"

ssh_options=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
remote="yc-user@$VM_PUBLIC_IP"

ssh "${ssh_options[@]}" "$remote" 'cd /opt/pw7/nifi && docker compose up -d'
ssh "${ssh_options[@]}" "$remote" 'cd /opt/pw7/nifi && docker compose ps'

cat <<EOF
NiFi is starting. Open an SSH tunnel in a separate terminal:
  ssh -L 8087:127.0.0.1:8080 yc-user@$VM_PUBLIC_IP

Then open:
  http://localhost:8087/nifi
EOF
