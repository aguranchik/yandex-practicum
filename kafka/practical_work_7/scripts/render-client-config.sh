#!/usr/bin/env bash

set -Eeuo pipefail
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
runtime_file="${1:-$PROJECT_DIR/.runtime.env}"
output_file="${2:-$PROJECT_DIR/config/kafka/client.properties}"

if [[ ! -f "$runtime_file" ]]; then
  echo "Runtime file not found: $runtime_file" >&2
  exit 1
fi

set -a
# shellcheck disable=SC1090
source "$runtime_file"
set +a

escaped_username="${KAFKA_USERNAME//\/\\}"
escaped_username="${escaped_username//&/\&}"
escaped_password="${KAFKA_PASSWORD//\/\\}"
escaped_password="${escaped_password//&/\&}"

sed \
  -e "s/__KAFKA_USERNAME__/$escaped_username/g" \
  -e "s/__KAFKA_PASSWORD__/$escaped_password/g" \
  "$PROJECT_DIR/config/kafka/client.properties.template" >"$output_file"
chmod 600 "$output_file"

echo "Kafka CLI properties written to $output_file"
