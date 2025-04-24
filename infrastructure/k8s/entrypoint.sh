#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

##############################
# Configurações Hardcoded
##############################
export APP_ROLE="${APP_ROLE:-orion}"
export AUTO_DEPLOY_ON_START="${AUTO_DEPLOY_ON_START:-true}"

declare -A APP_PORTS=(
    ["orion"]="4200"
    ["metrics"]="8082"
)

##############################
# Funções Auxiliares
##############################
log() {
    local LEVEL="$1"
    local MESSAGE="$2"
    printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${LEVEL^^}" "${MESSAGE}"
}

validate_env() {
    local REQUIRED_ENV=("POSTGRES_USER" "POSTGRES_PASSWORD" "PIPEDRIVE_API_KEY")
    for var in "${REQUIRED_ENV[@]}"; do
        if [[ -z "${!var:-}" ]]; then
            log "error" "Variável de ambiente obrigatória não definida: $var"
            exit 1
        fi
    done
}

start_server() {
    local APP="$1"
    shift
    log "info" "Iniciando $APP..."
    exec "$@"
}

##############################
# Deploy Automático
##############################
auto_deploy_flows() {
    if [[ "${AUTO_DEPLOY_ON_START}" == "true" ]]; then
        log "info" "AUTO_DEPLOY_ON_START está habilitado. Rodando 'prefect deploy --all'..."
        export PREFECT_API_URL="http://localhost:${APP_PORTS[orion]}/api"
        export PREFECT_API_AUTH_STRING="${PREFECT_SERVER_API_AUTH_STRING}"
        unset PREFECT_API_KEY

        local health_check_url="http://localhost:${APP_PORTS[orion]}/api/health"
        local attempts=0
        local max_attempts=30

        until curl -sf "$health_check_url" > /dev/null; do
            if [[ $attempts -ge $max_attempts ]]; then
                log "error" "Timeout esperando Prefect Orion ficar saudável."
                return 1
            fi
            log "info" "Aguardando Prefect Orion ficar disponível... (tentativa $((++attempts)))"
            sleep 2
        done

        log "info" "Prefect Orion está online. Aplicando deployments..."
        if prefect deploy --all --prefect-file infrastructure/k8s/prefect.yaml; then
            log "success" "Deploy automático concluído com sucesso!"
        else
            log "error" "Falha ao aplicar os deployments via 'prefect deploy --all'"
        fi
    fi
}

##############################
# Fluxo Principal
##############################
cd /app

case "${APP_ROLE}" in
    etl)
        validate_env
        log "info" "Iniciando fluxo ETL..."
        poetry run python -u flows/pipedrive_metabase_etl.py
        ;;
    metrics)
        start_server "metrics server" \
            python -m infrastructure.monitoring.metrics_server
        ;;
    orion)
        prefect server start --host 0.0.0.0 --port "${APP_PORTS[orion]}" --log-level WARNING &
        
        sleep 5
        auto_deploy_flows
        wait
        ;;
    *)
        log "error" "APP_ROLE inválido ou não definido. Valores permitidos: etl, metrics, orion"
        exit 1
        ;;
esac
