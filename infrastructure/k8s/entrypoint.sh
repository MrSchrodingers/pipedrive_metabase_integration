#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# ─────────────────────────── Root section ────────────────────────────
APP_USER=app
DOCKER_SOCKET=/var/run/docker.sock

log() { printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${1^^}" "$2"; }

log INFO "Entrypoint: iniciado como $(whoami)"

: "${PREFECT__FLOW_RUN_ID:=}"

if [[ -n "${PREFECT__FLOW_RUN_ID}" ]]; then
  log INFO "Variável PREFECT__FLOW_RUN_ID detectada, executando flow run..." 
  exec prefect flow-run execute "$@"
fi

# Ajuste de GID para /var/run/docker.sock
if [ -S "$DOCKER_SOCKET" ]; then
  SOCKET_GID=$(stat -c '%g' "$DOCKER_SOCKET")
  CONTAINER_GID=$(getent group docker | cut -d: -f3 || echo "")
  if [ -z "$CONTAINER_GID" ]; then
    log INFO "Criando grupo docker ($SOCKET_GID) no container"
    groupadd -r -g "$SOCKET_GID" docker
  elif [ "$CONTAINER_GID" != "$SOCKET_GID" ]; then
    log INFO "Ajustando GID do grupo docker de $CONTAINER_GID → $SOCKET_GID"
    groupmod -g "$SOCKET_GID" docker
  fi
  adduser $APP_USER docker &>/dev/null || true
else
  log WARN "Socket Docker não encontrado; workers locais podem falhar"
fi

log INFO "Trocando para usuário $APP_USER"
exec gosu $APP_USER bash <<'EOSU'
set -euo pipefail
IFS=$'\n\t'

# ─────────────────────────── Variáveis básicas ───────────────────────────
export APP_ROLE="${APP_ROLE:-orion}"
export AUTO_DEPLOY_ON_START="${AUTO_DEPLOY_ON_START:-true}"
export CONTAINER_PREFECT_PORT="${CONTAINER_PREFECT_PORT:-4200}"
export CONTAINER_METRICS_PORT="${CONTAINER_METRICS_PORT:-8082}"
export PREFECT_SERVER_API_AUTH_STRING="${PREFECT_SERVER_API_AUTH_STRING:-}"

log() { printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${1^^}" "$2"; }

# ─────────────────────── Sanitize (PREFECT_API_URL) ───────────────────────
unset PREFECT_API_URL PREFECT_PUBLIC_API_URL || true   # nunca deve vir de fora

LOCAL_API="http://localhost:${CONTAINER_PREFECT_PORT}/api"
prefect config set PREFECT_API_URL="$LOCAL_API"
prefect config set PREFECT_API_AUTH_STRING="$PREFECT_SERVER_API_AUTH_STRING" || true

# ───────────────────────── Função de bootstrap ───────────────────────────
auto_deploy_flows() {
  python /app/create_or_update_core_blocks.py
  if [[ "$AUTO_DEPLOY_ON_START" == "true" ]]; then
    prefect deploy --all --prefect-file infrastructure/k8s/prefect.yaml
  fi
}

# ───────────────────────────── Execução ──────────────────────────────────
cd /app
log INFO "Executando como \$(whoami) (APP_ROLE=$APP_ROLE)"

case "$APP_ROLE" in
  etl)
    poetry run python -u flows/pipedrive_metabase_etl.py ;;
  metrics)
    python -m infrastructure.monitoring.metrics_server ;;
  orion)
    prefect server start \
      --host 0.0.0.0 \
      --port "$CONTAINER_PREFECT_PORT" \
      --log-level WARNING \
      --keep-alive-timeout 60 &
    ORION_PID=$!
    log INFO "Prefect Server PID=$ORION_PID – esperando /health"
    until curl -sf "http://localhost:${CONTAINER_PREFECT_PORT}/api/health" >/dev/null; do
      sleep 3
      kill -0 $ORION_PID 2>/dev/null || { log ERROR "Orion morreu antes do /health"; exit 1; }
    done

    # Orion está saudável, podemos configurar o pool
    log INFO "Configurando Work Pool 'docker-pool' a partir do entrypoint do Orion..."
    POOL_NAME="docker-pool"
    POOL_TYPE="docker"
    TEMPLATE_FILE="/app/infrastructure/prefect/worker/docker-pool-template.yaml" # Caminho dentro do container Orion

    if [ ! -f "$TEMPLATE_FILE" ]; then
        log WARN "Arquivo de template do Work Pool '$TEMPLATE_FILE' não encontrado! Pulando configuração customizada do pool."
    else
        # Tenta criar, se falhar, tenta editar. Usa o 'prefect' CLI dentro deste container.
        if prefect work-pool create "$POOL_NAME" --type "$POOL_TYPE" --base-job-template "$TEMPLATE_FILE"; then
            log INFO "Work Pool '$POOL_NAME' criado com sucesso."
        else
            log INFO "Work Pool '$POOL_NAME' já existe ou criação falhou. Tentando editar..."
            # Assumindo que a imagem base prefecthq/prefect tem o comando 'edit'
            if prefect work-pool edit "$POOL_NAME" --base-job-template "$TEMPLATE_FILE"; then
               log INFO "Work Pool '$POOL_NAME' atualizado com sucesso via edit."
            else
               log WARN "Falha ao criar ou editar Work Pool '$POOL_NAME'. Pode ser necessário verificar a versão do Prefect ou o template."
               # Não vamos sair com erro aqui para permitir que o Orion continue, mas registramos o aviso.
            fi
        fi
    fi

    sleep 15
    auto_deploy_flows
    wait $ORION_PID
    ;;
  *)
    log ERROR "APP_ROLE inválido: $APP_ROLE"; exit 1 ;;
esac

log INFO "Entrypoint finalizado (APP_ROLE=$APP_ROLE)"
EOSU
