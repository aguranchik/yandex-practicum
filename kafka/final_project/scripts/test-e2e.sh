#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_DIR}"

docker compose ps

docker compose exec -T stream-processor \
  /usr/local/bin/app blacklist load /data/blacklist.json

sleep 3

docker compose --profile tools run --no-deps --rm shop-producer

for attempt in $(seq 1 30); do
  stored_count="$(docker compose exec -T postgres \
    psql -U marketplace -d marketplace -Atc 'SELECT count(*) FROM products;' \
    | tr -d '[:space:]')"
  if [[ "${stored_count}" == "4" ]]; then
    break
  fi
  if [[ "${attempt}" -eq 30 ]]; then
    echo "Expected four permitted products in PostgreSQL, got ${stored_count}" >&2
    exit 1
  fi
  sleep 2
done

blocked_count="$(docker compose exec -T postgres \
  psql -U marketplace -d marketplace -Atc "SELECT count(*) FROM products WHERE product_id = '99999';" \
  | tr -d '[:space:]')"

if [[ "${blocked_count}" != "0" ]]; then
  echo "Blocked product 99999 reached PostgreSQL" >&2
  exit 1
fi

for attempt in $(seq 1 30); do
  mirrored_count="$(docker compose exec -T kafka-client \
    kafka-get-offsets \
    --bootstrap-server secondary-kafka-1:9092 \
    --command-config /etc/kafka/client/admin.properties \
    --topic primary.products.filtered \
    | awk -F: '{sum += $3} END {print sum + 0}')"
  if [[ "${mirrored_count}" -ge 4 ]]; then
    break
  fi
  if [[ "${attempt}" -eq 30 ]]; then
    echo "Expected mirrored products in secondary Kafka, got ${mirrored_count}" >&2
    exit 1
  fi
  sleep 2
done

connect_output="runtime/connect-output/filtered-products.jsonl"
for attempt in $(seq 1 30); do
  if [[ -s "${connect_output}" ]] &&
    [[ "$(grep -c '"product_id"' "${connect_output}" || true)" -ge 4 ]]; then
    break
  fi
  if [[ "${attempt}" -eq 30 ]]; then
    echo "Kafka Connect did not write four permitted products to ${connect_output}" >&2
    exit 1
  fi
  sleep 2
done

if grep -q '"product_id":"99999"' "${connect_output}"; then
  echo "Blocked product 99999 reached Kafka Connect output" >&2
  exit 1
fi

docker compose --profile tools run --no-deps --rm client \
  search --user demo-user часы

echo "Core end-to-end check passed."
