#!/bin/bash
set -euo pipefail
IFS=$'\n\t'

##############################
# Configurações
##############################
# Mapeia nomes amigáveis para detalhes dos componentes
declare -A COMPONENTS=(
    ["orion"]="image_base=pipedrive_metabase_integration-prefect-orion;context=./infrastructure/prefect/orion;type=Deployment;manifest_id=prefect-orion"
    ["metrics"]="image_base=pipedrive_metabase_integration-metrics;context=.;type=Deployment;manifest_id=metrics"
    ["etl"]="image_base=pipedrive_metabase_integration-etl;context=.;type=Job;manifest_id=etl"
    ["redis"]="image_base=redis;context=SKIP;type=Deployment;manifest_id=redis" # SKIP build context for external images
    ["db"]="image_base=postgres;context=SKIP;type=Deployment;manifest_id=db"
    ["metabase"]="image_base=metabase/metabase;context=SKIP;type=Deployment;manifest_id=metabase"
    ["grafana"]="image_base=grafana/grafana;context=SKIP;type=Deployment;manifest_id=grafana"
)

# Arquivo principal de manifesto
MANIFEST_FILE="pipedrive_metabase_integration.yaml"
# Timeout para esperar rollouts/jobs
RESOURCE_TIMEOUT=600 # Reduzido para atualizações, ajuste se necessário

##############################
# Funções Auxiliares
##############################
log() {
    local LEVEL="$1"; local MESSAGE="$2"
    printf "[%s] [%s] %s\n" "$(date '+%Y-%m-%d %H:%M:%S')" "${LEVEL^^}" "${MESSAGE}"
}

fail() {
    log "error" "$1"; exit 1;
}

check_dependencies() {
    # (Igual ao deploy.sh)
    declare -A DEPS=( ["docker"]="Docker" ["kubectl"]="Kubernetes CLI" ["minikube"]="Minikube" ["sed"]="sed" )
    for cmd in "${!DEPS[@]}"; do
        if ! command -v "${cmd}" &> /dev/null; then
            fail "${DEPS[$cmd]} não encontrado. Por favor instale primeiro."
        fi
    done
}

# Garante que estamos no ambiente docker do minikube
setup_minikube_env() {
    log "info" "Configurando ambiente Docker do Minikube..."
    eval "$(minikube -p minikube docker-env)" || fail "Falha ao configurar ambiente Docker do Minikube."
    log "info" "Verificando contexto kubectl..."
    kubectl config use-context minikube || fail "Falha ao definir contexto kubectl para minikube."
}

##############################
# Funções de Atualização
##############################

# Constrói uma imagem específica com uma nova tag
build_component_image() {
    local component_key="$1"
    local details="${COMPONENTS[$component_key]}"
    local new_tag="$2" # Tag única gerada
    local image_base context type manifest_id

    # Parse details string
    eval "$(echo "$details" | awk -F';' '{for(i=1;i<=NF;i++) print $i}')"

    if [[ "$context" == "SKIP" ]]; then
        log "info" "Skipping build for external image: ${component_key} ($image_base)"
        return 0 # Sucesso, pois não há o que construir
    fi

    local full_image_tag="${image_base}:${new_tag}"
    log "info" "Construindo imagem para ${component_key}: ${full_image_tag} (Contexto: ${context})"

    export DOCKER_BUILDKIT=1 # Habilita BuildKit
    docker build \
        --progress=plain \
        --build-arg BUILDKIT_INLINE_CACHE=1 \
        # Tenta usar cache da tag 'latest' se existir
        --cache-from "${image_base}:latest" \
        -t "${full_image_tag}" \
        "${context}" || fail "Falha ao construir imagem para ${component_key}"

    log "info" "Carregando imagem ${full_image_tag} no Minikube..."
    minikube image load "${full_image_tag}" || fail "Falha ao carregar imagem ${full_image_tag} no Minikube"

    log "success" "Imagem para ${component_key} construída e carregada: ${full_image_tag}"
}

# Atualiza a tag da imagem no arquivo MANIFEST_FILE usando sed (FRÁGIL!)
update_manifest_image_tag() {
    local component_key="$1"
    local details="${COMPONENTS[$component_key]}"
    local new_tag="$2"
    local image_base context type manifest_id

    eval "$(echo "$details" | awk -F';' '{for(i=1;i<=NF;i++) print $i}')"

    local full_image_base="${image_base}" # Assume que image_base já contém o nome completo até o ':'

    log "warning" "Tentando atualizar tag da imagem para ${manifest_id} em ${MANIFEST_FILE} para ${new_tag} usando sed."
    log "warning" "Este método é FRÁGIL! Mudanças na formatação do YAML podem quebrá-lo."
    log "warning" "Considere usar 'yq' para uma edição de YAML mais robusta."
    # Exemplo com yq (requer instalação):
    # yq e ".spec.template.spec.containers[] |= select(.name == \"${manifest_id}\").image = \"${full_image_base}:${new_tag}\"" -i "${MANIFEST_FILE}"

    # Tentativa com sed: Encontra o bloco do deployment/job pelo nome e atualiza a próxima linha 'image:'
    # Isso assume que 'image:' está logo após ou perto de 'name: <manifest_id>' dentro de 'containers:'
    # Precisamos de um padrão mais específico se a estrutura for complexa.
    # Tentativa: Encontra 'name: manifest_id', vai até a linha 'image:', e substitui a tag.
    # Este sed é complexo e pode falhar facilmente. Use com cautela.
    # sed -i.bak "/name: ${manifest_id}/,/image:/ s|^\(\s*image:\s*${image_base//\//\\/}:\)[\w.-]*|\1${new_tag}|" "${MANIFEST_FILE}"

    # Tentativa mais simples (e talvez mais perigosa): Substitui a primeira ocorrência após o nome do container
     # Encontra a linha 'name: manifest_id', depois busca a *primeira* linha 'image:' seguinte e troca a tag.
     # Funciona SE houver apenas um container ou se o container certo for o primeiro com essa linha 'image:'
     sed -i.bak "/name: ${manifest_id}/,/containers:/ { /image:/ { s|^\(\s*image:\s*${image_base//\//\\/}:\)[\w.-]*$|\1${new_tag}| ; T ; b end ; :end } ; }" "${MANIFEST_FILE}" || fail "Falha ao executar sed para atualizar ${MANIFEST_FILE}"

    # Verifica se a substituição realmente aconteceu (de forma básica)
    if ! grep -q "${image_base}:${new_tag}" "${MANIFEST_FILE}"; then
         log "error" "Falha ao verificar a atualização da tag no ${MANIFEST_FILE} após usar sed. Verifique ${MANIFEST_FILE}.bak e corrija manualmente."
         # Opcional: Restaurar backup?
         # mv "${MANIFEST_FILE}.bak" "${MANIFEST_FILE}"
         fail "Atualização da tag da imagem falhou."
    fi
    log "info" "Arquivo ${MANIFEST_FILE} atualizado (esperançosamente) com a nova tag ${new_tag} para ${component_key}."
    rm -f "${MANIFEST_FILE}.bak" # Remove backup se tudo parece ok
}

# Aplica as mudanças e espera o rollout (ou job)
apply_and_wait() {
    local component_key="$1"
    local details="${COMPONENTS[$component_key]}"
    local image_base context type manifest_id

    eval "$(echo "$details" | awk -F';' '{for(i=1;i<=NF;i++) print $i}')"

    if [[ "$type" == "Job" ]]; then
        log "info" "Deletando Job ${manifest_id} anterior (se existir)..."
        kubectl delete job "${manifest_id}" --ignore-not-found=true || log "warning" "Falha ao deletar job ${manifest_id} anterior, pode já não existir."
        # Pequena pausa antes de aplicar o novo
        sleep 3
    fi

    log "info" "Aplicando mudanças do manifesto ${MANIFEST_FILE}..."
    kubectl apply -f "${MANIFEST_FILE}" || fail "Falha ao aplicar ${MANIFEST_FILE}"

    if [[ "$type" == "Deployment" ]]; then
        log "info" "Aguardando rollout do deployment/${manifest_id}..."
        kubectl rollout status "deployment/${manifest_id}" \
            --timeout="${RESOURCE_TIMEOUT}s" \
            --watch=true || fail "Timeout ou erro aguardando rollout do deployment/${manifest_id}"
        log "success" "Rollout do deployment/${manifest_id} concluído."
    elif [[ "$type" == "Job" ]]; then
        log "info" "Aguardando conclusão do job/${manifest_id}..."
        kubectl wait --for=condition=complete \
            --timeout="${RESOURCE_TIMEOUT}s" \
            job/"${manifest_id}" || log "warning" "Job ${manifest_id} não completou dentro do timeout ou falhou. Verifique os logs do job."
        # Verifica status após o wait
        JOB_STATUS=$(kubectl get job "${manifest_id}" -o jsonpath='{.status.conditions[?(@.type=="Complete")].status}')
        if [[ "$JOB_STATUS" == "True" ]]; then
            log "success" "Job ${manifest_id} concluído com sucesso."
            log "info" "Coletando métricas de execução do Job ${manifest_id}..."
            kubectl logs "job/${manifest_id}" --tail=100 | grep "ETL_COMPLETION_METRICS" || true
        else
            JOB_FAILED_STATUS=$(kubectl get job "${manifest_id}" -o jsonpath='{.status.conditions[?(@.type=="Failed")].status}')
            if [[ "$JOB_FAILED_STATUS" == "True" ]]; then
                 log "error" "Job ${manifest_id} falhou. Verifique os logs: kubectl logs job/${manifest_id}"
            else
                 log "warning" "Status final do Job ${manifest_id} incerto após timeout."
            fi
        fi
    fi
}

##############################
# Fluxo Principal do Update
##############################

check_dependencies
setup_minikube_env

log "info" "Componentes disponíveis para atualização:"
COMPONENT_KEYS=("${!COMPONENTS[@]}") # Array com as chaves
for i in "${!COMPONENT_KEYS[@]}"; do
    key="${COMPONENT_KEYS[$i]}"
    details="${COMPONENTS[$key]}"
    eval "$(echo "$details" | awk -F';' '{for(i=1;i<=NF;i++) print $i}')" # Parse para obter 'type'
    printf "  %d) %s (%s)\n" "$((i+1))" "$key" "$type"
done
echo "  A) Todos os componentes com build (exceto imagens externas)"
echo "  N) Nenhum (Sair)"

read -rp "Digite os números dos componentes a atualizar (separados por espaço), 'A' para Todos, ou 'N' para Sair: " -a SELECTIONS

declare -a COMPONENTS_TO_UPDATE=()

if [[ " ${SELECTIONS[@]} " =~ " A " ]] || [[ " ${SELECTIONS[@]} " =~ " a " ]]; then
    log "info" "Selecionado: Todos os componentes aplicáveis."
    for key in "${COMPONENT_KEYS[@]}"; do
        details="${COMPONENTS[$key]}"
        eval "$(echo "$details" | awk -F';' '{for(i=1;i<=NF;i++) print $i}')"
        if [[ "$context" != "SKIP" ]]; then # Apenas os que têm build context
            COMPONENTS_TO_UPDATE+=("$key")
        else
             log "info" "Skipping ${key} (imagem externa) da seleção 'Todos'."
        fi
    done
elif [[ " ${SELECTIONS[@]} " =~ " N " ]] || [[ " ${SELECTIONS[@]} " =~ " n " ]]; then
    log "info" "Saindo sem atualizações."
    exit 0
else
    # Valida seleções numéricas
    for sel in "${SELECTIONS[@]}"; do
        if [[ "$sel" =~ ^[0-9]+$ ]] && (( sel > 0 && sel <= ${#COMPONENT_KEYS[@]} )); then
            COMPONENTS_TO_UPDATE+=("${COMPONENT_KEYS[$((sel-1))]}")
        else
            log "warning" "Seleção inválida ignorada: ${sel}"
        fi
    done
    # Remove duplicados se houver
    COMPONENTS_TO_UPDATE=($(printf "%s\n" "${COMPONENTS_TO_UPDATE[@]}" | sort -u))
fi

if [[ ${#COMPONENTS_TO_UPDATE[@]} -eq 0 ]]; then
    log "error" "Nenhum componente válido selecionado para atualização."
    exit 1
fi

echo # Linha em branco
log "info" "Componentes selecionados para atualização:"
for comp in "${COMPONENTS_TO_UPDATE[@]}"; do
    echo "  - $comp"
done
echo # Linha em branco

read -rp "Confirma a atualização destes componentes? (s/N): " CONFIRM
if [[ ! "$CONFIRM" =~ ^[Ss]$ ]]; then
    log "info" "Atualização cancelada."
    exit 0
fi

# Gera uma tag única para esta atualização
NEW_IMAGE_TAG=$(date +%Y%m%d%H%M%S)
log "info" "Usando a tag de imagem única para esta atualização: ${NEW_IMAGE_TAG}"

# Processa cada componente selecionado
for comp_key in "${COMPONENTS_TO_UPDATE[@]}"; do
    log "info" "--- Iniciando atualização para: ${comp_key} ---"
    build_component_image "$comp_key" "$NEW_IMAGE_TAG"
    update_manifest_image_tag "$comp_key" "$NEW_IMAGE_TAG"
    apply_and_wait "$comp_key"
    log "info" "--- Atualização para ${comp_key} concluída (ou tentativa feita) ---"
    echo 
done

log "success" "✅ Processo de atualização concluído para os componentes selecionados!"
log "info" "Verifique os logs e o status dos pods/jobs."
