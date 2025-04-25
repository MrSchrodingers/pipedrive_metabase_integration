#!/usr/bin/env bash
set -euo pipefail
echo "[$(date '+%Y-%m-%d %H:%M:%S')] [INFO] Iniciando Prefect worker para pool 'docker-pool'..."
exec prefect worker start --pool "docker-pool"