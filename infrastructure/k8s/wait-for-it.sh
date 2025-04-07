#!/usr/bin/env bash
host="$1"
port="$2"
shift 2
cmd="$@"

echo "Aguardando Orion em http://$host:$port/api/health..."
until curl -s "http://$host:$port/api/health" >/dev/null 2>&1; do
  echo "Ainda aguardando..."
  sleep 1
done

echo "Orion está disponível. Executando comando: $cmd"
exec $cmd
