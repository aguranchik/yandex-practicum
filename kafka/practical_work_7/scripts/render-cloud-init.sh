#!/usr/bin/env bash

set -Eeuo pipefail
PROJECT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
public_key_file="${1:-$HOME/.ssh/id_ed25519.pub}"
template="$PROJECT_DIR/config/cloud-init/user-data.yaml"
output="$PROJECT_DIR/build/user-data.yaml"

if [[ ! -f "$public_key_file" ]]; then
  echo "SSH public key not found: $public_key_file" >&2
  exit 1
fi

public_key="$(tr -d '\r\n' <"$public_key_file")"
if [[ "$public_key" != ssh-* ]]; then
  echo "Unexpected SSH public key format in $public_key_file" >&2
  exit 1
fi

mkdir -p "$PROJECT_DIR/build"
while IFS= read -r line || [[ -n "$line" ]]; do
  if [[ "$line" == *"__SSH_PUBLIC_KEY__"* ]]; then
    printf '%s\n' "${line/__SSH_PUBLIC_KEY__/$public_key}"
  else
    printf '%s\n' "$line"
  fi
done <"$template" >"$output"
chmod 600 "$output"

if rg -q '__SSH_PUBLIC_KEY__' "$output"; then
  echo "Failed to render SSH public key into cloud-init" >&2
  exit 1
fi

echo "Rendered cloud-init: $output"
