#!/bin/bash
set -euo pipefail #
IFS=$'\n\t'

# --- Variáveis Globais e Função de Log (Usadas inicialmente por root) ---
APP_USER=app
DOCKER_SOCKET=/var/run/docker.sock

log() {
  printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${1^^}" "$2"
}

log INFO "Entrypoint: Iniciando como $(whoami)"

# --- Bloco de Correção do GID do Docker (Executado como root) ---
if [ -S "$DOCKER_SOCKET" ]; then
    log INFO "Entrypoint: Socket Docker encontrado em $DOCKER_SOCKET"
    SOCKET_GID=$(stat -c '%g' "$DOCKER_SOCKET")
    log INFO "Entrypoint: GID do Socket Docker do host: $SOCKET_GID"

    CONTAINER_GROUP_GID=$(getent group docker | cut -d: -f3 || echo "")

    if [ -z "$CONTAINER_GROUP_GID" ]; then
      log INFO "Entrypoint: Grupo 'docker' não encontrado no container. Criando com GID $SOCKET_GID."
      groupadd -r -g "$SOCKET_GID" docker
      adduser $APP_USER docker > /dev/null 2>&1 || true
    elif [ "$SOCKET_GID" != "$CONTAINER_GROUP_GID" ]; then
        log INFO "Entrypoint: GID do grupo 'docker' no container ($CONTAINER_GROUP_GID) difere do socket ($SOCKET_GID)."
        log INFO "Entrypoint: Ajustando GID do grupo 'docker' do container para $SOCKET_GID."
        groupmod -g "$SOCKET_GID" docker
        adduser $APP_USER docker > /dev/null 2>&1 || true
    else
        log INFO "Entrypoint: GID do grupo 'docker' ($CONTAINER_GROUP_GID) já corresponde ao GID do socket."
        adduser $APP_USER docker > /dev/null 2>&1 || true
    fi
else
    log WARN "Entrypoint: Aviso - Socket Docker ($DOCKER_SOCKET) não encontrado. Operações Docker podem falhar."
fi

log INFO "Entrypoint: Trocando para o usuário '$APP_USER' para executar a lógica principal."

exec gosu $APP_USER bash << EOF_APP
set -euo pipefail
IFS=$'\n\t'

export APP_ROLE="${APP_ROLE:-orion}"
export AUTO_DEPLOY_ON_START="${AUTO_DEPLOY_ON_START:-true}"
export CONTAINER_PREFECT_PORT="${CONTAINER_PREFECT_PORT:-4200}"
export CONTAINER_METRICS_PORT="${CONTAINER_METRICS_PORT:-8082}"
export PREFECT_SERVER_API_AUTH_STRING="${PREFECT_SERVER_API_AUTH_STRING:-}" 

log() {
  printf "[%s] [%s] %s\n" "\$(date '+%Y-%m-%d %H:%M:%S')" "\${1^^}" "\$2"
}

declare -A APP_PORTS=(
  ["orion"]="\${CONTAINER_PREFECT_PORT}"
  ["metrics"]="\${CONTAINER_METRICS_PORT}"
)

# Função auto_deploy_flows
auto_deploy_flows() {
  sleep 20 
  if [[ "\${AUTO_DEPLOY_ON_START}" == "true" ]]; then
    prefect config set PREFECT_API_AUTH_STRING="\${PREFECT_SERVER_API_AUTH_STRING}" || true
    prefect deploy --all --prefect-file infrastructure/k8s/prefect.yaml
  fi
}

cd /app
log INFO "Executando como \$(whoami) no diretório \$(pwd)"
log INFO "APP_ROLE=\${APP_ROLE}"

case "\$APP_ROLE" in
  etl)
    log info "Starting ETL flow"
    poetry run python -u flows/pipedrive_metabase_etl.py
    ;;
  metrics)
    log info "Starting metrics server"
    python -m infrastructure.monitoring.metrics_server
    ;;
  orion)
    log info "Starting Prefect Server..."
    prefect server start \
      --host 0.0.0.0 \
      --port "${CONTAINER_PREFECT_PORT}" \
      --log-level WARNING \
      --keep-alive-timeout 60 &
    ORION_PID=\$!
    log INFO "Prefect Server started with PID \$ORION_PID. Waiting for it to become healthy..."

    until curl -sf "http://localhost:\${CONTAINER_PREFECT_PORT}/api/health" > /dev/null; do
      log info "Waiting for Orion API..."
      if ! kill -0 \$ORION_PID 2>/dev/null; then
          log error "Orion server failed to start or terminated unexpectedly."
          exit 1
      fi
      sleep 3
    done
    log INFO "Orion API is up. Waiting additional time for migrations/startup..."
    sleep 15 

    if ! kill -0 \$ORION_PID 2>/dev/null; then
        log error "Orion server terminated unexpectedly before deployment."
        exit 1
    fi

    auto_deploy_flows

    log INFO "Orion setup complete. Tailing server process \$ORION_PID."
    wait \$ORION_PID
    log INFO "Orion server process \$ORION_PID finished."
    ;;
  *)
    log error "Invalid APP_ROLE: \$APP_ROLE"
    exit 1
    ;;
esac

log INFO "Entrypoint finished for APP_ROLE=\${APP_ROLE}"

EOF_APP