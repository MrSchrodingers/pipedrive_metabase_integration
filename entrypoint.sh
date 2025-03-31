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

wait_for_service() {
    local HOST=$1
    local PORT=$2
    local TIMEOUT=30
    local COUNT=0
    
    log "info" "Aguardando $HOST:$PORT..."
    until nc -z $HOST $PORT; do
        sleep 5
        COUNT=$((COUNT + 5))
        if [ $COUNT -ge $TIMEOUT ]; then
            log "error" "Timeout ao aguardar $HOST:$PORT"
            exit 1
        fi
    done
}

register_blocks() {
    log "info" "Registrando blocos necessários..."
    poetry run python <<EOF
import os
from prefect.blocks.system import JSON

# Corrigir formato da DSN para psycopg2
JSON(value={
    "dsn": f"postgresql://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@db:5432/{os.getenv('POSTGRES_DB')}"
}).save("postgres-pool", overwrite=True)

JSON(value={
    "connection_string": "redis://redis:6379/0"
}).save("redis-cache", overwrite=True)

print("Blocos registrados com sucesso!")
EOF
}

##############################
# Fluxo Principal
##############################
cd /app

case "${APP_ROLE:-}" in
    etl)
        validate_env
        
        # Aguardar serviços essenciais
        wait_for_service prefect-orion 4200
        wait_for_service db 5432
        wait_for_service redis 6379

        # Instalar dependências adicionais
        poetry run pip install prefect-sqlalchemy
        prefect block register -m prefect_sqlalchemy

        # Registrar blocos
        register_blocks

        # Executar fluxo ETL
        log "info" "Iniciando fluxo ETL..."
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