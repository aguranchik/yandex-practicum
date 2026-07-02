#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env
source "$PROJECT_DIR/.runtime.env"

registry_host="${SCHEMA_REGISTRY_URL#https://}"
registry_host="${registry_host%:443}"
ssh_options=(-o StrictHostKeyChecking=accept-new -o ConnectTimeout=10)
remote="yc-user@$VM_PUBLIC_IP"

ssh "${ssh_options[@]}" "$remote" "cat >/opt/pw7/stunnel-schema-registry.conf <<EOF
client = yes
foreground = no
pid = /opt/pw7/stunnel-schema-registry.pid

[schema-registry]
accept = 127.0.0.1:8081
connect = $registry_host:443
CAfile = /opt/pw7/certs/CA.pem
verifyChain = yes
checkHost = $registry_host
EOF
if [[ -f /opt/pw7/stunnel-schema-registry.pid ]]; then
  kill \"\$(cat /opt/pw7/stunnel-schema-registry.pid)\" 2>/dev/null || true
fi
stunnel /opt/pw7/stunnel-schema-registry.conf"

cat <<EOF
Schema Registry TLS proxy is running on the VM.
Keep the following command open in this terminal:

  ssh -L 8081:127.0.0.1:8081 yc-user@$VM_PUBLIC_IP

In another terminal run (password is stored only in .runtime.env):

  curl --user '$KAFKA_USERNAME:<password>' http://localhost:8081/subjects
  curl --user '$KAFKA_USERNAME:<password>' http://localhost:8081/subjects/$EVENT_TOPIC-value/versions
EOF
