#!/usr/bin/env bash

set -euo pipefail

NAMENODE_DIR=/usr/local/hadoop/hdfs/namenode
mkdir -p "${NAMENODE_DIR}"
chmod -R 777 "${NAMENODE_DIR}"

if [[ ! -f "${NAMENODE_DIR}/current/VERSION" ]]; then
  hdfs namenode -format -force -nonInteractive
fi

exec "$@"
