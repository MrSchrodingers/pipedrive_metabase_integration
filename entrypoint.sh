#!/usr/bin/env bash
set -e

cd /app
echo "Conteúdo de /app:"
ls -la

case "$APP_ROLE" in
  etl)
    echo "Aguardando Prefect Orion..."
    ./wait-for-it.sh prefect-orion 4200 poetry run python flows/pipedrive_metabase_etl.py
    ;;
  metrics)
    echo "Iniciando o servidor de métricas..."
    python infrastructure/monitoring/metrics_server.py
    ;;
  orion)
    echo "Iniciando o Prefect Orion na porta 4200..."
    prefect orion start --host=0.0.0.0 --port=4200
    ;;
  *)
    echo "Nenhum APP_ROLE definido ou papel desconhecido. Utilize: etl, metrics ou orion."
    ;;
esac
