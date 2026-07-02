#!/usr/bin/env bash

set -Eeuo pipefail
source "$(dirname "$0")/common.sh"
load_env

cluster_id="$(resource_id 'managed-kafka cluster' "$KAFKA_CLUSTER_NAME")"
if [[ -z "$cluster_id" ]]; then
  echo "Kafka cluster not found: $KAFKA_CLUSTER_NAME" >&2
  exit 1
fi

runtime_file="$PROJECT_DIR/.runtime.env"
if [[ -f "$runtime_file" ]]; then
  # shellcheck disable=SC1090
  source "$runtime_file"
fi

if [[ -z "${KAFKA_PASSWORD:-}" ]]; then
  KAFKA_PASSWORD="$(openssl rand -hex 24)"
fi

if ! yc_cmd managed-kafka user get "$KAFKA_USERNAME" --cluster-id "$cluster_id" >/dev/null 2>&1; then
  yc_cmd managed-kafka user create "$KAFKA_USERNAME" \
    --cluster-id "$cluster_id" \
    --password "$KAFKA_PASSWORD" \
    --permission "topic=$EVENT_TOPIC,role=producer" \
    --permission "topic=$EVENT_TOPIC,role=consumer" \
    --permission "topic=$NIFI_TOPIC,role=producer" \
    --permission "topic=$NIFI_TOPIC,role=consumer"
fi

create_topic() {
  local topic="$1"
  if yc_cmd managed-kafka topic get "$topic" --cluster-id "$cluster_id" >/dev/null 2>&1; then
    return
  fi
  yc_cmd managed-kafka topic create "$topic" \
    --cluster-id "$cluster_id" \
    --partitions 3 \
    --replication-factor 3 \
    --cleanup-policy delete \
    --retention-ms 86400000 \
    --retention-bytes 268435456 \
    --segment-bytes 134217728 \
    --min-insync-replicas 2 \
    --compression-type zstd
}

create_topic "$EVENT_TOPIC"
create_topic "$NIFI_TOPIC"

mapfile -t kafka_hosts < <(yc_cmd managed-kafka cluster list-hosts "$cluster_id" --format json | jq -r '.[].name' | sort)
if [[ "${#kafka_hosts[@]}" -ne 3 ]]; then
  echo "Expected 3 Kafka hosts, found ${#kafka_hosts[@]}" >&2
  exit 1
fi

kafka_brokers="$(printf '%s:9091,' "${kafka_hosts[@]}")"
kafka_brokers="${kafka_brokers%,}"
vm_public_ip="$(yc_cmd compute instance get "$VM_NAME" --format json | jq -r '.network_interfaces[0].primary_v4_address.one_to_one_nat.address')"

umask 077
cat >"$runtime_file" <<EOF
KAFKA_CLUSTER_ID=$cluster_id
KAFKA_BROKERS=$kafka_brokers
KAFKA_USERNAME=$KAFKA_USERNAME
KAFKA_PASSWORD=$KAFKA_PASSWORD
KAFKA_TOPIC=$EVENT_TOPIC
KAFKA_GROUP_ID=pw7-consumer
KAFKA_CA_FILE=/opt/pw7/certs/CA.pem
SCHEMA_REGISTRY_URL=https://${kafka_hosts[0]}:443
SCHEMA_REGISTRY_USERNAME=$KAFKA_USERNAME
SCHEMA_REGISTRY_PASSWORD=$KAFKA_PASSWORD
SCHEMA_REGISTRY_CA_FILE=/opt/pw7/certs/CA.pem
AVRO_SCHEMA_FILE=/opt/pw7/schemas/pw7-event.avsc
MESSAGE_COUNT=5
CONSUMER_IDLE_TIMEOUT=20s
VM_PUBLIC_IP=$vm_public_ip
EOF

yc_cmd managed-kafka cluster get "$cluster_id" --format yaml >"$PROJECT_DIR/artifacts/logs/kafka-cluster.yaml"
yc_cmd managed-kafka cluster list-hosts "$cluster_id" --format yaml >"$PROJECT_DIR/artifacts/logs/kafka-hosts.yaml"
yc_cmd managed-kafka topic get "$EVENT_TOPIC" --cluster-id "$cluster_id" --format yaml >"$PROJECT_DIR/artifacts/logs/topic-$EVENT_TOPIC.yaml"

echo "Kafka user and topics are ready. Runtime settings saved to .runtime.env (ignored by Git)."
