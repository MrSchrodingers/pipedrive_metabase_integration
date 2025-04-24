#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

export APP_ROLE="${APP_ROLE:-orion}"
export AUTO_DEPLOY_ON_START="${AUTO_DEPLOY_ON_START:-true}"

HOST_PREFECT_PORT="${HOST_PREFECT_PORT:-4200}"
HOST_METRICS_PORT="${HOST_METRICS_PORT:-8082}"

declare -A APP_PORTS=(
  ["orion"]="${HOST_PREFECT_PORT}"
  ["metrics"]="${HOST_METRICS_PORT}"
)

log() {
  printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${1^^}" "$2"
}

auto_deploy_flows() {
  if [[ "${AUTO_DEPLOY_ON_START}" == "true" ]]; then
    log info "Automatic deploy enabled"
    export PREFECT_API_URL="http://localhost:${APP_PORTS[orion]}/api"
    export PREFECT_API_AUTH_STRING="${PREFECT_SERVER_API_AUTH_STRING}"
    prefect config set PREFECT_API_AUTH_STRING="${PREFECT_API_AUTH_STRING}" || true

    until curl -sf "$PREFECT_API_URL/health" > /dev/null; do
      log info "Waiting for Orion..."
      sleep 2
    done

    log info "Creating/updating blocks"
    python /app/create_or_update_core_blocks.py

    log info "Creating Docker work pool"
    prefect work-pool create --type docker docker-pool --overwrite

    log info "Applying deployments"
    prefect deploy --all --prefect-file infrastructure/k8s/prefect.yaml
  fi
}

cd /app

case "$APP_ROLE" in
  etl)
    log info "Starting ETL flow"
    poetry run python -u flows/pipedrive_metabase_etl.py
    ;;
  metrics)
    log info "Starting metrics server"
    python -m infrastructure.monitoring.metrics_server
    ;;
  orion)
    prefect server start --host 0.0.0.0 --port 4200 --log-level WARNING &
    sleep 5
    auto_deploy_flows
    wait
    ;;
  *)
    log error "Invalid APP_ROLE: $APP_ROLE"
    exit 1
    ;;
esac
