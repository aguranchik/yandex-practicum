#!/usr/bin/env bash

set -euo pipefail

DATANODE_DIR=/usr/local/hadoop/hdfs/datanode
mkdir -p "${DATANODE_DIR}"
chmod -R 777 "${DATANODE_DIR}"

exec "$@"
