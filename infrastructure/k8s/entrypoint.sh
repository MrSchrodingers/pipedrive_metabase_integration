#!/usr/bin/env bash
set -euo pipefail
IFS=$'\n\t'

##############################
# Configurações
##############################
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
# Fluxo Principal
##############################
cd /app

case "${APP_ROLE:-}" in
    etl)
        validate_env

        # Executar fluxo ETL
        log "info" "Iniciando fluxo ETL (dependências esperadas via Init Containers)..."
        poetry run python -u flows/pipedrive_metabase_etl.py
        ;;
    metrics)
        start_server "metrics server" \
            python -m infrastructure.monitoring.metrics_server
        ;;
    orion)
        start_server "Prefect Orion" \
            prefect orion start \
                --host 0.0.0.0 \
                --port "${APP_PORTS[orion]}" \
                --log-level WARNING
        ;;
    *)
        log "error" "APP_ROLE inválido ou não definido. Valores permitidos: etl, metrics, orion"
        exit 1
        ;;
esac