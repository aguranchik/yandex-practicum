#!/usr/bin/env bash

set -euo pipefail

BOOTSTRAP_SERVER="${BOOTSTRAP_SERVER:-kafka-1:9092}"
ADMIN_CONFIG="/etc/kafka/client/admin.properties"

for attempt in $(seq 1 60); do
  if kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --command-config "${ADMIN_CONFIG}" \
    --list >/dev/null 2>&1; then
    break
  fi

  if [[ "${attempt}" -eq 60 ]]; then
    echo "Kafka cluster did not become ready" >&2
    exit 1
  fi

  sleep 2
done

for topic in topic-1 topic-2; do
  kafka-topics \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --command-config "${ADMIN_CONFIG}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions 3 \
    --replication-factor 3
done

for topic in topic-1 topic-2; do
  kafka-acls \
    --bootstrap-server "${BOOTSTRAP_SERVER}" \
    --command-config "${ADMIN_CONFIG}" \
    --add \
    --allow-principal "User:producer" \
    --operation Write \
    --operation Describe \
    --topic "${topic}"
done

kafka-acls \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --command-config "${ADMIN_CONFIG}" \
  --add \
  --allow-principal "User:consumer" \
  --operation Read \
  --operation Describe \
  --topic topic-1

kafka-acls \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --command-config "${ADMIN_CONFIG}" \
  --add \
  --allow-principal "User:consumer" \
  --operation Read \
  --group pw6-consumer-group

echo
echo "Configured topics:"
kafka-topics \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --command-config "${ADMIN_CONFIG}" \
  --list

echo
echo "Configured ACLs:"
kafka-acls \
  --bootstrap-server "${BOOTSTRAP_SERVER}" \
  --command-config "${ADMIN_CONFIG}" \
  --list
