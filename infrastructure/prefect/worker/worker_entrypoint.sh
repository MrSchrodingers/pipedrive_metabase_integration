#!/usr/bin/env bash
set -euo pipefail

RUNTIME_IMAGE="mrschrodingers/pmi-runtime:latest"
RUNTIME_DOCKERFILE="Dockerfile.runtime"

POOL_NAME="docker-pool"
POOL_TYPE="docker"
TEMPLATE_FILE="infrastructure/prefect/worker/docker-pool-template.yaml"

echo "-----------------------------------------------------"
echo "Construindo e enviando a imagem runtime '$RUNTIME_IMAGE'..."
echo "Usando Dockerfile: $RUNTIME_DOCKERFILE"
echo "-----------------------------------------------------"

docker build --no-cache -f "$RUNTIME_DOCKERFILE" -t "$RUNTIME_IMAGE" . \
  && docker push "$RUNTIME_IMAGE"

echo "Imagem runtime construída e enviada com sucesso."

echo "-----------------------------------------------------"
echo "Verificando/Configurando o Work Pool '$POOL_NAME'..."
echo "Usando template: $TEMPLATE_FILE"
echo "-----------------------------------------------------"

if [ ! -f "$TEMPLATE_FILE" ]; then
    echo "ERRO: Arquivo de template '$TEMPLATE_FILE' não encontrado!"
    exit 1
fi

if prefect work-pool create "$POOL_NAME" --type "$POOL_TYPE" --base-job-template "$TEMPLATE_FILE" --overwrite; then
  echo "Work Pool '$POOL_NAME' criado com sucesso."
else
  echo "Work Pool '$POOL_NAME' já existe ou ocorreu um erro na criação. Tentando atualizar a configuração..."
  if prefect work-pool create "$POOL_NAME" --base-job-template "$TEMPLATE_FILE" --overwrite; then
    echo "Work Pool '$POOL_NAME' atualizado com sucesso com a configuração do template."
  else
    echo "ERRO: Falha ao criar ou editar o Work Pool '$POOL_NAME'. Verifique os logs e a configuração."
    exit 1 
  fi
fi

echo "-----------------------------------------------------"
echo "Iniciando o worker para o pool '$POOL_NAME'..."
echo "A imagem '$RUNTIME_IMAGE' será usada para executar os flows."
echo "Use Ctrl+C para parar o worker."
echo "-----------------------------------------------------"

prefect worker start --pool "$POOL_NAME"

echo "Worker para o pool '$POOL_NAME' encerrado."

exit 0