#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

##############################
# Configurações
##############################
declare -A IMAGES=(
    ["etl"]="pipedrive_metabase_integration-etl:latest"
    ["orion"]="pipedrive_metabase_integration-prefect-orion:latest"
    ["metrics"]="pipedrive_metabase_integration-metrics:latest"
)

RESOURCE_TIMEOUT=300
MINIKUBE_CPUS=2
MINIKUBE_MEMORY=8192
MINIKUBE_DRIVER=docker
CLEANUP_NAMESPACES="default,kube-system"

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
    kubectl delete pods --field-selector=status.phase!=Running -n default --wait=false || true
}

check_dependencies() {
    declare -A DEPS=(
        ["docker"]="Docker"
        ["kubectl"]="Kubernetes CLI"
        ["minikube"]="Minikube"
        ["curl"]="cURL"
    )
    
    for cmd in "${!DEPS[@]}"; do
        if ! command -v "${cmd}" &> /dev/null; then
            fail "${DEPS[$cmd]} não encontrado. Por favor instale primeiro."
        fi
    done
}

##############################
# Funções Principais
##############################
stop_resources() {
    log "info" "Parando todos os recursos..."
    
    declare -a KILL_PIDS=(
        "kubectl port-forward svc/db 5432:5432"
        "kubectl port-forward svc/prefect-orion 4200:4200"
    )
    
    for pid_pattern in "${KILL_PIDS[@]}"; do
        pkill -f "${pid_pattern}" || true
    done

    kubectl delete --all deployments,services,jobs,hpa,pvc,secrets,configmaps -n default --wait=true
    minikube addons disable metrics-server || true
    
    log "success" "Recursos parados com sucesso."
    exit 0
}

start_minikube() {
    local STATUS=$(minikube status -o json | jq -r '.Host')
    
    if [[ "${STATUS}" != "Running" ]]; then
        log "info" "Iniciando Minikube com ${MINIKUBE_CPUS} CPUs e ${MINIKUBE_MEMORY}MB RAM..."
        minikube start \
            --driver="${MINIKUBE_DRIVER}" \
            --cpus="${MINIKUBE_CPUS}" \
            --memory="${MINIKUBE_MEMORY}" \
            --addons=metrics-server \
            --embed-certs=true \
            --extra-config=apiserver.service-account-signing-key-file=/var/lib/minikube/certs/sa.key \
            --extra-config=apiserver.service-account-issuer=kubernetes/serviceaccount \
            --extra-config=apiserver.service-account-api-audiences=api
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
        
        docker build \
            --progress=plain \
            --build-arg BUILDKIT_INLINE_CACHE=1 \
            --cache-from "${TAG}" \
            -t "${TAG}" \
            "${BUILD_CONTEXT}"
    done
}

deploy_infra() {
    log "info" "Aplicando configurações base..."
    
    kubectl apply -f observability-config.yaml --server-side=true
    kubectl apply -f db-secrets.yaml
    kubectl apply -f persistent-volume-claim.yaml
    
    log "info" "Criando secret a partir do .env"
    kubectl create secret generic app-secrets \
        --from-env-file=.env \
        --dry-run=client \
        -o yaml | kubectl apply -f -
    
    log "info" "Aplicando manifesto principal..."
    kubectl apply -f pipedrive_metabase_integration.yaml
}

wait_for_rollout() {
    local DEPLOYMENTS=("prefect-orion" "metrics" "redis" "db")
    
    for dep in "${DEPLOYMENTS[@]}"; do
        log "info" "Aguardando rollout do deployment/${dep}..."
        kubectl rollout status "deployment/${dep}" \
            --timeout="${RESOURCE_TIMEOUT}s" \
            --watch=true
    done
}

setup_port_forwarding() {
    declare -A PORTS=(
        ["prefect-orion"]="4200"
        ["db"]="5432"
    )
    
    for svc in "${!PORTS[@]}"; do
        local PORT="${PORTS[$svc]}"
        log "info" "Iniciando port-forward para ${svc} na porta ${PORT}"
        kubectl port-forward "svc/${svc}" "${PORT}:${PORT}" &
        sleep 5
    done
}

run_etl_job() {
    log "info" "Executando Job ETL com timeout de ${RESOURCE_TIMEOUT}s..."
    kubectl wait --for=condition=complete \
        --timeout="${RESOURCE_TIMEOUT}s" \
        job/etl
    
    log "info" "Coletando métricas de execução..."
    kubectl logs job/etl --tail=100 | grep "ETL_COMPLETION_METRICS"
}

##############################
# Fluxo Principal
##############################
case "${1:-}" in
    stop)
        stop_resources
        ;;
    *)
        check_dependencies
        start_minikube
        build_images
        deploy_infra
        wait_for_rollout
        setup_port_forwarding
        run_etl_job
        log "success" "✅ Implantação concluída com sucesso!"
        ;;
esac