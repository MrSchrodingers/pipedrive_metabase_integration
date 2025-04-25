#!/usr/bin/env bash
set -euo pipefail

POOL=docker-pool
NET=prefect_internal_network

# 1. Cria/atualiza o pool com configurações Docker
prefect work-pool create \
  --type docker \
  --default-docker-image "mrschrodingers/pmi-runtime:latest" \
  --default-docker-network "prefect_internal_network" \
  docker-pool

# 2. Inicia o worker
prefect worker start --pool docker-pool --network prefect_internal_network