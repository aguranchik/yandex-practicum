#!/usr/bin/env bash

set -Eeuo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

load_env() {
  local env_file="${1:-$PROJECT_DIR/.env}"
  if [[ ! -f "$env_file" ]]; then
    echo "Configuration file not found: $env_file" >&2
    echo "Copy .env.example to .env and fill ADMIN_CIDR." >&2
    exit 1
  fi

  set -a
  # shellcheck disable=SC1090
  source "$env_file"
  set +a

  : "${YC_PROFILE:?YC_PROFILE is required}"
  : "${YC_CLOUD_ID:?YC_CLOUD_ID is required}"
  : "${YC_FOLDER_ID:?YC_FOLDER_ID is required}"
}

yc_cmd() {
  yc "$@" --profile "$YC_PROFILE" --cloud-id "$YC_CLOUD_ID" --folder-id "$YC_FOLDER_ID"
}

resource_id() {
  local group="$1"
  local name="$2"
  local result
  result="$(yc_cmd $group get "$name" --format json 2>/dev/null || true)"
  if [[ -n "$result" ]]; then
    jq -r '.id // empty' <<<"$result"
  fi
  return 0
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    echo "Required command is not installed: $command_name" >&2
    exit 1
  fi
}
