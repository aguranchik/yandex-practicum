#!/usr/bin/env bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${PROJECT_DIR}"

docker compose --profile analytics up -d hadoop-namenode hadoop-datanode-1 hadoop-datanode-2 hadoop-datanode-3 spark-master spark-worker hdfs-loader

for attempt in $(seq 1 60); do
  product_files="$(docker compose --profile analytics exec -T hadoop-namenode \
    hdfs dfs -count /marketplace/products 2>/dev/null \
    | awk '{print $2}')"
  event_files="$(docker compose --profile analytics exec -T hadoop-namenode \
    hdfs dfs -count /marketplace/client-events 2>/dev/null \
    | awk '{print $2}')"
  if [[ "${product_files:-0}" -gt 0 && "${event_files:-0}" -gt 0 ]]; then
    break
  fi
  if [[ "${attempt}" -eq 60 ]]; then
    echo "Mirrored data did not reach HDFS" >&2
    exit 1
  fi
  sleep 3
done

docker compose --profile analytics run --no-deps --rm spark-analytics
docker compose --profile analytics run --no-deps --rm recommendation-publisher

sleep 5

docker compose --profile tools run --no-deps --rm client \
  recommendations --user demo-user

echo "Analytics pipeline completed."
