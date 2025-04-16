#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

##############################
# Configurações
##############################
declare -A IMAGES=(
    ["etl"]="pipedrive_metabase_integration-etl:latest"
    ["orion"]="pipedrive_metabase_integration-prefect-orion:latest"
)

RESOURCE_TIMEOUT=1800 # 30min
MINUTES=$((RESOURCE_TIMEOUT / 60))
MINIKUBE_CPUS=4
MINIKUBE_MEMORY=10240 # 10GB
MINIKUBE_DRIVER=docker
CLEANUP_NAMESPACES="default,kube-system"
PREFECT_YAML_FILE="./infrastructure/k8s/prefect.yaml"

##############################
# Funções Auxiliares
##############################
log() {
    local LEVEL="$1"
    local MESSAGE="$2"
    printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${LEVEL^^}" "${MESSAGE}"
}

fail() {
    log "error" "$1"
    exit 1
}

cleanup() {
    log "info" "Limpando recursos temporários..."
    pkill -P $$ || true
    kubectl delete pods --field-selector=status.phase!=Running,status.phase!=Pending -n default --wait=false || true
}

check_dependencies() {
    declare -A DEPS=(
        ["docker"]="Docker"
        ["kubectl"]="Kubernetes CLI"
        ["minikube"]="Minikube"
        ["curl"]="cURL"
        ["prefect"]="Prefect CLI" 
    )

    for cmd in "${!DEPS[@]}"; do
        if ! command -v "${cmd}" &> /dev/null; then
            fail "${DEPS[$cmd]} não encontrado. Por favor instale/configure primeiro."
        fi
    done
    if [[ ! -f "${PREFECT_YAML_FILE}" ]]; then
        fail "Arquivo de configuração Prefect não encontrado em: ${PREFECT_YAML_FILE}"
    fi
}


##############################
# Funções Principais
##############################
stop_resources() {
    log "info" "Parando todos os recursos..."

    declare -a KILL_PIDS=(
        "kubectl port-forward svc/db"       
        "kubectl port-forward svc/prefect-orion"
        "kubectl port-forward svc/metabase"
        "kubectl port-forward svc/grafana"
    )

    for pid_pattern in "${KILL_PIDS[@]}"; do
        pkill -f "${pid_pattern}" || true
    done

    kubectl delete --all deployments,services,jobs,hpa,pvc,secrets,configmaps -n default --wait=true --ignore-not-found=true
    minikube addons disable metrics-server || true

    log "success" "Recursos parados com sucesso."
    exit 0
}

start_minikube() {
    local STATUS
    STATUS=$(minikube status -o json | jq -r '.Host' 2>/dev/null || echo "Error")

    if [[ "${STATUS}" != "Running" ]]; then
        log "info" "Iniciando Minikube com ${MINIKUBE_CPUS} CPUs e ${MINIKUBE_MEMORY}MB RAM..."
        minikube start \
            --driver="${MINIKUBE_DRIVER}" \
            --cpus="${MINIKUBE_CPUS}" \
            --memory="${MINIKUBE_MEMORY}" \
            --addons=metrics-server \
            --embed-certs=true \
            --extra-config=apiserver.service-account-signing-key-file=/var/lib/minikube/certs/sa.key \
            --extra-config=apiserver.service-account-issuer=kubernetes/serviceaccount || fail "Falha ao iniciar Minikube"
    else
        log "info" "Minikube já está rodando. Reutilizando instância existente."
    fi

    eval "$(minikube docker-env)"
    kubectl config use-context minikube
}

build_images() {
    log "info" "Construindo imagens com BuildKit..."

    export DOCKER_BUILDKIT=1
    for image in "${!IMAGES[@]}"; do
        local TAG="${IMAGES[$image]}"
        local BUILD_CONTEXT="."

        if [[ "${image}" == "orion" ]]; then
            BUILD_CONTEXT="./infrastructure/prefect/orion"
        fi

        log "info" "Building ${TAG} from context ${BUILD_CONTEXT}"
        docker build \
            --progress=plain \
            --build-arg BUILDKIT_INLINE_CACHE=1 \
            --cache-from "${TAG}" \
            -t "${TAG}" \
            "${BUILD_CONTEXT}" || fail "Falha ao construir imagem ${TAG}"
    done
}

# --- Função para aplicar Deployments Prefect ---
deploy_prefect_flows() {
    local work_pool_name="kubernetes-pool"
    local secret_block_name="github-access-token"
    local db_block_name="postgres-pool"
    local redis_block_name="redis-cache"

    local prefect_api_url_ip="http://127.0.0.1:4200/api"

    # --- Verificação Prévia da Conexão com Orion ---
    log "debug" "Verificando acesso à API Prefect via IP (${prefect_api_url_ip}) antes de criar blocos..."
    sleep 5
    local health_check_url="${prefect_api_url_ip%/api}/health"
    local attempt=0
    local max_attempts=5
    while ! curl --fail --max-time 5 -s "${health_check_url}" > /dev/null; do
         attempt=$((attempt + 1))
         if [[ $attempt -ge $max_attempts ]]; then
              log "error" "Falha no teste de conexão com ${health_check_url} após ${max_attempts} tentativas."
              fail "Não foi possível conectar ao Prefect API via ${prefect_api_url_ip}. Impossível continuar."
         fi
         log "debug" "Tentativa ${attempt}/${max_attempts}: Falha ao conectar a ${health_check_url}. Aguardando 5s..."
         sleep 5
    done
     log "debug" "Teste de conexão com ${health_check_url} bem-sucedido."

    # --- Verificação das Variáveis de Ambiente para Blocos ---
    log "info" "Verificando variáveis de ambiente para criação dos blocos..."
    local required_block_vars=("GITHUB_PAT" "POSTGRES_USER" "POSTGRES_PASSWORD" "POSTGRES_DB")
    local missing_vars_msg=""
    for var in "${required_block_vars[@]}"; do
         if [[ -z "${!var:-}" ]]; then
              missing_vars_msg+="- ${var}\n"
         fi
    done
    if [[ -n "$missing_vars_msg" ]]; then
        log "error" "Variáveis de ambiente obrigatórias para criar blocos não definidas:\n${missing_vars_msg}"
        fail "Exporte as variáveis necessárias antes de rodar o script."
    else
        log "info" "Variáveis de ambiente para blocos parecem estar definidas."
    fi

    # --- Criação/Atualização dos Blocos Core E INFRA via Script Python ---
    log "info" "Executando script para criar/atualizar Blocos Prefect (${secret_block_name}, ${db_block_name}, ${redis_block_name}, k8s-jobs)..." 
    export PREFECT_API_URL="${prefect_api_url_ip}"
    log "debug" "PREFECT_API_URL configurada como ${PREFECT_API_URL} para script Python e CLI."

    # Executa o script que agora também cria os blocos K8sJob
    if ! python create_or_update_core_blocks.py; then
         fail "Falha ao executar create_or_update_core_blocks.py. Verifique os logs do script Python acima."
    fi
    log "info" "Blocos Prefect (incluindo K8sJob) criados/atualizados com sucesso."

    # --- Criação do Work Pool ---
    log "info" "Verificando/Criando Work Pool Prefect: ${work_pool_name}..."

    if ! prefect work-pool inspect "${work_pool_name}" > /dev/null 2>&1; then
         log "info" "Criando work pool '${work_pool_name}'..."
         if prefect work-pool create --type kubernetes "${work_pool_name}" --overwrite; then
             log "info" "Work Pool '${work_pool_name}' criado com sucesso."
         else
             fail "Falha ao criar work pool '${work_pool_name}'."
         fi
    else
         log "info" "Work Pool '${work_pool_name}' já existe."
    fi

    # --- Aplicação dos Deployments ---
    log "info" "Aplicando/Atualizando Deployments Prefect a partir do ${PREFECT_YAML_FILE}..."
    if prefect deploy --all --prefect-file "${PREFECT_YAML_FILE}"; then 
        log "success" "Deployments Prefect aplicados com sucesso via CLI."
    else
        fail "Falha ao aplicar deployments Prefect via CLI. Verifique os logs do comando."
    fi
}

deploy_infra() {
    log "info" "Aplicando configurações base..."

    kubectl apply -f infrastructure/k8s/observability-config.yaml --server-side=true || fail "Falha ao aplicar observability-config.yaml"
    kubectl apply -f infrastructure/k8s/db-secrets.yaml || fail "Falha ao aplicar db-secrets.yaml"
    if kubectl get pvc pgdata-pvc > /dev/null 2>&1 ; then
       log "info" "PVC pgdata-pvc já existe."
    else
       kubectl apply -f infrastructure/k8s/persistent-volume-claim.yaml || fail "Falha ao aplicar persistent-volume-claim.yaml"
    fi

    kubectl apply -f infrastructure/k8s/prometheus.yml || fail "Falha ao aplicar prometheus.yml"
    kubectl apply -f infrastructure/k8s/pushgateway.yaml || fail "Falha ao aplicar pushgateway.yaml"

    log "info" "Criando/Atualizando secret 'app-secrets' a partir do .env"
    kubectl create secret generic app-secrets \
        --from-env-file=.env \
        --dry-run=client \
        -o yaml | kubectl apply -f - || fail "Falha ao criar/atualizar app-secrets"

    log "info" "Aplicando manifesto principal (sem o Job 'etl' estático)..."
    kubectl apply -f infrastructure/k8s/pipedrive_metabase_integration.yaml || fail "Falha ao aplicar pipedrive_metabase_integration.yaml"
}

wait_for_rollout() {
    local DEPLOYMENTS=("prefect-orion" "prefect-agent" "pushgateway" "prometheus-deployment" "redis" "db" "grafana")

    for dep in "${DEPLOYMENTS[@]}"; do
        if kubectl get deployment "${dep}" > /dev/null 2>&1; then
             log "info" "Aguardando rollout do deployment/${dep}..."
             kubectl rollout status "deployment/${dep}" \
                 --timeout="${RESOURCE_TIMEOUT}s" \
                 --watch=true || fail "Timeout ou erro aguardando rollout do deployment/${dep}"
        else
             log "warning" "Deployment ${dep} não encontrado, pulando espera do rollout."
        fi
    done
    log "info" "Rollout de todos os deployments principais concluído."
}

setup_port_forwarding() {
     declare -A PORTS=(
        ["prefect-orion"]="4200"
        ["db"]="5432"
        ["metabase"]="3000"  
        ["grafana"]="3015"        
    )

     log "info" "Iniciando port-forward para os services..."

     for svc in "${!PORTS[@]}"; do
         local PORT="${PORTS[$svc]}"
         if kubectl get service "${svc}" > /dev/null 2>&1; then
             log "warn" "Removendo port-fowards existentes para ${svc}..."
             pkill -f "kubectl port-forward svc/${svc} ${PORT}:${PORT}" || true
             log "info" "Iniciando port-forward para ${svc} na porta ${PORT}"
             kubectl port-forward "svc/${svc}" "${PORT}:${PORT}" &
             sleep 2 
         else
             log "warning" "Serviço ${svc} não encontrado, pulando port-forward."
         fi
     done
     log "info" "Aguardando alguns segundos para estabilizar os port-forwards..."
     sleep 10

     log "info" "Port-forwards iniciados (se os serviços existirem)."
     for svc in "${!PORTS[@]}"; do
         local PORT="${PORTS[$svc]}"
         if pgrep -f "kubectl port-forward svc/${svc} ${PORT}:${PORT}" > /dev/null; then
             log "info" "[${svc}] Iniciado na porta ${PORT}, acesse: http://localhost:${PORT}"
         fi
     done
}

##############################
# Fluxo Principal
##############################
trap cleanup EXIT # Adiciona trap para limpeza em caso de erro ou interrupção

case "${1:-}" in
    stop)
        stop_resources
        ;;
    *)
        check_dependencies
        start_minikube
        build_images
        deploy_infra            # Aplica K8s manifests (inclui Agent)
        wait_for_rollout        # Espera Deployments (inclui Agent)
        setup_port_forwarding   # Habilita acesso local (ex: Orion UI)
        deploy_prefect_flows    

        log "success" "✅ Implantação da infraestrutura concluída!"
        log "info" "Prefect Agent está rodando."
        log "info" "Fluxos agendados (como o Sync) serão iniciados pelo agente."
        log "info" "Fluxos sob demanda (como o Backfill inicial) precisam ser iniciados via UI/API ou Automação."
        log "info" "Monitore em http://localhost:4200 (Orion)"
        log "info" "Para manter port-forwards ativos, este script precisa continuar rodando ou execute os port-forwards separadamente."
        # Mantém o script rodando para manter os port-forwards vivos (Ctrl+C para parar)
        wait
        ;;
esac