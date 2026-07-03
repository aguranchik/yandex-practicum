#!/usr/bin/env bash

set -euo pipefail

PRIMARY_BOOTSTRAP="${PRIMARY_BOOTSTRAP:-primary-kafka-1:9092}"
SECONDARY_BOOTSTRAP="${SECONDARY_BOOTSTRAP:-secondary-kafka-1:9092}"
ADMIN_CONFIG="${ADMIN_CONFIG:-/etc/kafka/client/admin.properties}"

wait_for_cluster() {
  local bootstrap="$1"

  for attempt in $(seq 1 90); do
    if kafka-topics \
      --bootstrap-server "${bootstrap}" \
      --command-config "${ADMIN_CONFIG}" \
      --list >/dev/null 2>&1; then
      return
    fi
    sleep 2
  done

  echo "Kafka cluster ${bootstrap} did not become ready" >&2
  exit 1
}

create_topic() {
  local bootstrap="$1"
  local topic="$2"
  local cleanup_policy="${3:-delete}"
  local partitions="${4:-3}"

  kafka-topics \
    --bootstrap-server "${bootstrap}" \
    --command-config "${ADMIN_CONFIG}" \
    --create \
    --if-not-exists \
    --topic "${topic}" \
    --partitions "${partitions}" \
    --replication-factor 3 \
    --config min.insync.replicas=2 \
    --config "cleanup.policy=${cleanup_policy}"
}

allow_topic() {
  local bootstrap="$1"
  local principal="$2"
  local topic="$3"
  shift 3

  for operation in "$@"; do
    kafka-acls \
      --bootstrap-server "${bootstrap}" \
      --command-config "${ADMIN_CONFIG}" \
      --add \
      --allow-principal "User:${principal}" \
      --operation "${operation}" \
      --topic "${topic}"
  done
}

allow_topic_prefix() {
  local bootstrap="$1"
  local principal="$2"
  local topic_prefix="$3"
  shift 3

  for operation in "$@"; do
    kafka-acls \
      --bootstrap-server "${bootstrap}" \
      --command-config "${ADMIN_CONFIG}" \
      --add \
      --allow-principal "User:${principal}" \
      --operation "${operation}" \
      --resource-pattern-type prefixed \
      --topic "${topic_prefix}"
  done
}

allow_group() {
  local bootstrap="$1"
  local principal="$2"
  local group="$3"
  shift 3

  for operation in "$@"; do
    kafka-acls \
      --bootstrap-server "${bootstrap}" \
      --command-config "${ADMIN_CONFIG}" \
      --add \
      --allow-principal "User:${principal}" \
      --operation "${operation}" \
      --group "${group}"
  done
}

allow_cluster() {
  local bootstrap="$1"
  local principal="$2"
  shift 2

  for operation in "$@"; do
    kafka-acls \
      --bootstrap-server "${bootstrap}" \
      --command-config "${ADMIN_CONFIG}" \
      --add \
      --allow-principal "User:${principal}" \
      --operation "${operation}" \
      --cluster
  done
}

wait_for_cluster "${PRIMARY_BOOTSTRAP}"
wait_for_cluster "${SECONDARY_BOOTSTRAP}"

create_topic "${PRIMARY_BOOTSTRAP}" products.raw
create_topic "${PRIMARY_BOOTSTRAP}" products.filtered
create_topic "${PRIMARY_BOOTSTRAP}" client.events
create_topic "${PRIMARY_BOOTSTRAP}" blacklist.commands compact
create_topic "${PRIMARY_BOOTSTRAP}" product-filter-table compact
create_topic "${PRIMARY_BOOTSTRAP}" connect-configs compact 1
create_topic "${PRIMARY_BOOTSTRAP}" connect-offsets compact
create_topic "${PRIMARY_BOOTSTRAP}" connect-statuses compact
create_topic "${SECONDARY_BOOTSTRAP}" recommendations compact

allow_topic "${PRIMARY_BOOTSTRAP}" shop products.raw Write Describe

allow_topic "${PRIMARY_BOOTSTRAP}" client client.events Write Describe

allow_topic "${PRIMARY_BOOTSTRAP}" processor products.raw Read Describe
allow_topic "${PRIMARY_BOOTSTRAP}" processor blacklist.commands Read Write Describe
allow_topic "${PRIMARY_BOOTSTRAP}" processor products.filtered Write Describe
allow_topic "${PRIMARY_BOOTSTRAP}" processor product-filter-table Read Write Describe
allow_topic "${PRIMARY_BOOTSTRAP}" processor product-filter-table DescribeConfigs
allow_group "${PRIMARY_BOOTSTRAP}" processor product-filter Read Describe

allow_topic "${PRIMARY_BOOTSTRAP}" connect products.filtered Read Describe
allow_topic "${PRIMARY_BOOTSTRAP}" connect connect-configs All
allow_topic "${PRIMARY_BOOTSTRAP}" connect connect-offsets All
allow_topic "${PRIMARY_BOOTSTRAP}" connect connect-statuses All
allow_group "${PRIMARY_BOOTSTRAP}" connect final-connect Read Describe
allow_group "${PRIMARY_BOOTSTRAP}" connect connect-filtered-products-file Read Describe
allow_cluster "${PRIMARY_BOOTSTRAP}" connect Describe

allow_topic "${PRIMARY_BOOTSTRAP}" storage products.filtered Read Describe
allow_group "${PRIMARY_BOOTSTRAP}" storage storage-products Read Describe

allow_topic "${PRIMARY_BOOTSTRAP}" mirror products.filtered Read Describe DescribeConfigs
allow_topic "${PRIMARY_BOOTSTRAP}" mirror client.events Read Describe DescribeConfigs
allow_topic_prefix "${PRIMARY_BOOTSTRAP}" mirror mm2- All
allow_topic "${PRIMARY_BOOTSTRAP}" mirror heartbeats All
allow_group "${PRIMARY_BOOTSTRAP}" mirror '*' Read Describe
allow_cluster "${PRIMARY_BOOTSTRAP}" mirror All

allow_topic "${SECONDARY_BOOTSTRAP}" mirror '*' All
allow_group "${SECONDARY_BOOTSTRAP}" mirror '*' All
allow_cluster "${SECONDARY_BOOTSTRAP}" mirror All

allow_topic "${SECONDARY_BOOTSTRAP}" analytics primary.products.filtered Read Describe
allow_topic "${SECONDARY_BOOTSTRAP}" analytics primary.client.events Read Describe
allow_topic "${SECONDARY_BOOTSTRAP}" analytics recommendations Write Describe
allow_group "${SECONDARY_BOOTSTRAP}" analytics hdfs-loader Read Describe

allow_topic "${SECONDARY_BOOTSTRAP}" storage recommendations Read Describe
allow_group "${SECONDARY_BOOTSTRAP}" storage storage-recommendations Read Describe

allow_topic "${SECONDARY_BOOTSTRAP}" client recommendations Read Describe
allow_group "${SECONDARY_BOOTSTRAP}" client client-recommendations Read Describe

echo "Primary and secondary topics and ACLs are configured."
