#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

# --- Variáveis Globais ---
APP_USER="app"
DOCKER_SOCKET="/var/run/docker.sock"
HEALTH_CHECK_ENDPOINT="http://localhost:${CONTAINER_PREFECT_PORT:-4200}/health"
DEPLOYMENT_DELAY=25  # Segundos para estabilização pós-startup

# --- Funções Auxiliares ---
log() {
    printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${1^^}" "$2"
}

adjust_docker_gid() {
    if [ -S "$DOCKER_SOCKET" ]; then
        local socket_gid=$(stat -c '%g' "$DOCKER_SOCKET")
        local container_gid=$(getent group docker | cut -d: -f3 || echo "")
        
        if [ -z "$container_gid" ]; then
            log info "Criando grupo docker com GID $socket_gid"
            groupadd -r -g "$socket_gid" docker
        elif [ "$socket_gid" != "$container_gid" ]; then
            log info "Ajustando GID do grupo docker para $socket_gid"
            groupmod -g "$socket_gid" docker
        fi
        
        adduser "$APP_USER" docker > /dev/null 2>&1 || true
    else
        log warn "Socket Docker não encontrado em $DOCKER_SOCKET"
    fi
}

# --- Execução como Root ---
log info "Iniciando entrypoint como root"
adjust_docker_gid

# --- Switch para Usuário App ---
exec gosu "$APP_USER" bash << EOF_APP
set -euo pipefail
IFS=$'\n\t'

# --- Configuração do Ambiente ---
export PREFFECT_SERVER_API_AUTH_STRING="${PREFECT_SERVER_API_AUTH_STRING:-}"
export PREFECT_API_URL="http://localhost:${CONTAINER_PREFECT_PORT:-4200}/api"

cd /app
log info "APP_ROLE=${APP_ROLE:-orion}"

# --- Lógica Principal ---
case "${APP_ROLE}" in
    etl)
        log info "Iniciando fluxo ETL principal"
        poetry run python -u flows/pipedrive_metabase_etl.py
        ;;

    metrics)
        log info "Iniciando servidor de métricas"
        python -m infrastructure.monitoring.metrics_server
        ;;

    orion)
        # 1. Inicialização do Servidor
        log info "Iniciando Prefect Orion Server"
        prefect server start \
            --host 0.0.0.0 \
            --port "${CONTAINER_PREFECT_PORT:-4200}" \
            --log-level WARNING \
            --keep-alive-timeout 60 &
        ORION_PID=\$!

        # 2. Healthcheck com Timeout
        log info "Verificando saúde do servidor (PID \$ORION_PID)"
        for i in {1..10}; do
            if curl -sf "$HEALTH_CHECK_ENDPOINT" >/dev/null; then
                log info "Servidor Orion pronto após \$((i*3)) segundos"
                break
            elif ! kill -0 \$ORION_PID 2>/dev/null; then
                log error "Falha na inicialização do servidor"
                exit 1
            fi
            sleep 3
        done

        # 3. Deploy Automático (Se Habilitado)
        if [[ "${AUTO_DEPLOY_ON_START:-true}" == "true" ]]; then
            log info "Iniciando deploy automático"
            sleep $DEPLOYMENT_DELAY  # Estabilização pós-startup
            
            prefect config set PREFECT_API_AUTH_STRING="\$PREFECT_SERVER_API_AUTH_STRING" || true
            prefect deploy --all --prefect-file infrastructure/k8s/prefect.yaml
        fi

        # 4. Monitoramento Contínuo
        log info "Monitorando processo do servidor (PID \$ORION_PID)"
        wait \$ORION_PID
        log warn "Servidor Orion finalizado"
        ;;

    *)
        log error "APP_ROLE inválido: ${APP_ROLE}"
        exit 1
        ;;
esac

log info "Entrypoint concluído para APP_ROLE=${APP_ROLE}"
EOF_APP