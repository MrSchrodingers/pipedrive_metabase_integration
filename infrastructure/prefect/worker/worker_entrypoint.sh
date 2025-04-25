#!/usr/bin/env bash
set -euo pipefail

POOL=docker-pool
TYPE=docker
NET=prefect_internal_network

# 1. Cria/atualiza o pool
prefect work-pool create --overwrite --type docker "$POOL"

# 2. Aplica o template
prefect work-pool set-template "$POOL" --file infrastructure/prefect/worker/docker-worker-template.yml

# 3. Inicia o worker (sem --network, configurado no template)
exec prefect worker start --pool "$POOL" --type $TYPE