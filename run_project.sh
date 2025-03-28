#!/bin/bash
set -e

# ----------------------------------------------------
# Função para "parar tudo"
# ----------------------------------------------------
stop_all() {
  echo "Parando todos os recursos do projeto..."
  # 1) Mata o 'port-forward' se estiver rodando
  pkill -f "kubectl port-forward svc/prefect-orion 4200:4200" || true

  # 2) Opcional: deletar resources do Kubernetes
  kubectl delete deployment prefect-orion etl metrics redis db --ignore-not-found
  kubectl delete service prefect-orion etl metrics redis db --ignore-not-found
  kubectl delete hpa etl-hpa metrics-hpa --ignore-not-found
  kubectl delete pvc pgdata-pvc --ignore-not-found
  kubectl delete secret db-secrets --ignore-not-found

  echo "✅ Todos os recursos foram parados e/ou removidos."
  exit 0
}

# Se o primeiro argumento for "stop", chamamos a função e saímos
if [ "$1" = "stop" ]; then
  stop_all
fi

# ----------------------------------------------------
# Demais comandos de "start/deploy" do seu projeto
# ----------------------------------------------------

########################################
# SEGURANÇA: Não execute como root
########################################
if [ "$(id -u)" -eq 0 ]; then
  echo "Por favor, não execute este script como root. Execute-o como um usuário normal que pertença ao grupo 'docker'."
  exit 1
fi

########################################
# PERMISSÕES DO DOCKER
########################################
echo "=============================="
echo "Verificando permissões do Docker para o usuário atual..."
echo "=============================="

if ! getent group docker > /dev/null; then
  echo "Grupo 'docker' não existe. Criando..."
  sudo groupadd docker
fi

if ! groups "$USER" | grep -q '\bdocker\b'; then
  echo "Usuário $USER não pertence ao grupo 'docker'. Adicionando..."
  sudo usermod -aG docker "$USER"
  echo "✅ Usuário adicionado ao grupo 'docker'."
  echo "⚠️  É necessário reiniciar o terminal ou executar 'newgrp docker' para aplicar as permissões."
  exit 0
else
  echo "✅ Usuário $USER já está no grupo 'docker'."
fi

if ! docker info >/dev/null 2>&1; then
  echo "❌ Não foi possível se comunicar com o Docker."
  echo "✅ O usuário está no grupo 'docker', mas talvez você precise reiniciar o terminal ou rodar 'newgrp docker'."
  exit 1
fi

echo "=============================="
echo "Verificando se o Minikube está iniciado corretamente..."
echo "=============================="

minikube_container_status=$(docker ps -a --filter "name=minikube" --format "{{.Status}}")

if [[ -z "$minikube_container_status" || "$minikube_container_status" == *"Exited"* || "$minikube_container_status" == *"Dead"* ]]; then
  echo "⚠️  Minikube container não encontrado ou não está rodando corretamente."
  echo "Recriando o cluster Minikube..."
  minikube delete || true
  minikube start --driver=docker
else
  echo "✅ Minikube está rodando: $minikube_container_status"
fi

echo "=============================="
echo "Configurando Docker para usar o daemon do Minikube..."
echo "=============================="
eval "$(minikube docker-env)"
echo "DOCKER_HOST: $DOCKER_HOST"

echo "=============================="
echo "Configurando o contexto para Minikube..."
echo "=============================="
kubectl config use-context minikube

# --- Hard reset dos recursos do Kubernetes ---
echo "=============================="
echo "Excluindo Deployments, Services, HPAs, PVC e Secrets do projeto..."
echo "=============================="
kubectl delete deployment prefect-orion etl metrics redis db --ignore-not-found
kubectl delete service prefect-orion etl metrics redis db --ignore-not-found
kubectl delete hpa etl-hpa metrics-hpa --ignore-not-found
kubectl delete pvc pgdata-pvc --ignore-not-found
kubectl delete secret db-secrets --ignore-not-found

sleep 5

echo "=============================="
echo "Removendo imagens Docker anteriores, se existirem..."
echo "=============================="
docker image rm -f pipedrive_metabase_integration-etl:latest || true
docker image rm -f pipedrive_metabase_integration-prefect-orion:latest || true
docker image rm -f pipedrive_metabase_integration-metrics:latest || true

echo "=============================="
echo "Buildando as imagens localmente com Docker (via Minikube)..."
echo "=============================="
docker build --no-cache -t pipedrive_metabase_integration-etl:latest .
docker build --no-cache -t pipedrive_metabase_integration-prefect-orion:latest ./infrastructure/prefect/orion
docker build --no-cache -t pipedrive_metabase_integration-metrics:latest .

echo "=============================="
echo "Imagens buildadas. Prosseguindo para o deploy no Kubernetes."
echo "=============================="

echo "Aguardando 10 segundos para os containers iniciarem..."
sleep 10

echo "=============================="
echo "Aplicando ConfigMap de Observabilidade no Kubernetes..."
echo "=============================="
kubectl apply -f observability-config.yaml --validate=false

echo "=============================="
echo "Aplicando Secret e PVC do DB..."
echo "=============================="
kubectl apply -f db-secrets.yaml
kubectl apply -f persistent-volume-claim.yaml

echo "=============================="
echo "Aplicando manifest do Kubernetes..."
echo "=============================="
kubectl apply -f pipedrive_metabase_integration.yaml

echo "=============================="
echo "Aguardando rollout dos deployments..."
echo "=============================="
kubectl rollout status deployment/prefect-orion --timeout=120s
kubectl rollout status deployment/etl --timeout=120s
kubectl rollout status deployment/metrics --timeout=120s
kubectl rollout status deployment/redis --timeout=120s
kubectl rollout status deployment/db --timeout=120s

echo "=============================="
echo "Aguardando o Prefect Orion escutar na porta 4200 (readiness)..."
echo "=============================="
MAX_WAIT=30
WAITED=0
until kubectl exec deploy/prefect-orion -- curl -s http://localhost:4200/api/health > /dev/null 2>&1 || [ $WAITED -ge $MAX_WAIT ]; do
  echo "⏳ Esperando Prefect Orion... (${WAITED}s)"
  sleep 3
  WAITED=$((WAITED + 3))
done

if [ $WAITED -ge $MAX_WAIT ]; then
  echo "❌ Prefect Orion não ficou pronto a tempo."
  exit 1
fi

echo "✅ Prefect Orion está escutando. Fazendo port-forward..."
kubectl port-forward svc/prefect-orion 4200:4200 &
PORT_FORWARD_PID_ORION=$!

echo "✅ Banco de dados está disponível via port-forward..."
kubectl port-forward svc/db 5432:5432 &
PORT_FORWARD_PID_DB=$!

sleep 3

# # Se quisermos parar o port-forward quando sair do script,
# # use um trap:
# trap 'kill $PORT_FORWARD_PID_ORION $PORT_FORWARD_PID_DB 2>/dev/null' EXIT

# Exportamos pra poder usar local
export PREFECT_API_URL=http://localhost:4200/api

echo "=============================="
echo "Limpando deployments existentes no Prefect (se houverem)..."
echo "=============================="
if command -v prefect &>/dev/null; then
  DEPLOYMENT_IDS=$(prefect deployment ls | grep "/" || true)
  if [ -n "$DEPLOYMENT_IDS" ]; then
    echo "Deletando deployments antigos no Orion..."
    for dep_id in $DEPLOYMENT_IDS; do
      prefect deployment delete "$dep_id" --yes
    done
  else
    echo "Nenhum deployment encontrado no Orion."
  fi
else
  echo "⚠️ Prefect CLI não está instalado localmente. Pulando limpeza de deployments no Orion."
fi

echo "=============================="
echo "Tudo implantado! O Orion está disponível localmente em http://localhost:4200"
echo "O ETL está rodando dentro do cluster no pod 'etl'."
echo "=============================="

# Fim do script
