#!/usr/bin/env bash
set -euo pipefail

POOL=docker-pool
NET=prefect_internal_network

# 1. Cria/atualiza o pool com configurações Docker
prefect work-pool create --name "$POOL" --type docker --overwrite \
  --config '{
    "image": "mrschrodingers/pmi-runtime:latest",
    "network": "'"$NET"'",
    "host": "unix:///var/run/docker.sock"
  }'

# 2. Inicia o worker
prefect worker start --work-pool "$POOL"