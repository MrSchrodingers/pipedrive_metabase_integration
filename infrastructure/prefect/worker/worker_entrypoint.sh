#!/usr/bin/env bash
set -euo pipefail

POOL=docker-pool
TYPE=docker
NET=prefect_internal_network

# cria/atualiza o pool sempre (overwrite é barato)
prefect work-pool create --overwrite --type $TYPE \
                         --default-docker-network $NET \
                         --default-docker-image mrschrodingers/pmi-runtime:latest \
                         "$POOL"

# agora sim inicia o worker
exec prefect worker start --pool "$POOL" --type $TYPE --network $NET
