#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

export APP_ROLE="${APP_ROLE:-orion}"
export AUTO_DEPLOY_ON_START="${AUTO_DEPLOY_ON_START:-true}"

CONTAINER_PREFECT_PORT="${CONTAINER_PREFECT_PORT:-4200}"
CONTAINER_METRICS_PORT="${CONTAINER_METRICS_PORT:-8082}"

declare -A APP_PORTS=(
  ["orion"]="${CONTAINER_PREFECT_PORT}"
  ["metrics"]="${CONTAINER_METRICS_PORT}"
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
  prefect server start --host 0.0.0.0 --port "${CONTAINER_PREFECT_PORT}" --log-level WARNING &
  ORION_PID=$!
  log info "Waiting for Orion server to become healthy..."
  until curl -sf "http://localhost:${CONTAINER_PREFECT_PORT}/api/health" > /dev/null; do
    log info "Waiting for Orion API..."
    if ! kill -0 $ORION_PID 2>/dev/null; then
        log error "Orion server failed to start."
        exit 1
    fi
    sleep 3
  done
  log info "Orion API is up. Waiting additional time for migrations..."
  sleep 15 
  if ! kill -0 $ORION_PID 2>/dev/null; then
      log error "Orion server terminated unexpectedly before deployment."
      exit 1
  fi
  auto_deploy_flows 
  wait $ORION_PID 
  ;;
  *)
    log error "Invalid APP_ROLE: $APP_ROLE"
    exit 1
    ;;
esac
