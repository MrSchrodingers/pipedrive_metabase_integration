import os
from prefect.blocks.system import Secret, JSON
from prefect_kubernetes.jobs import KubernetesJob
from dotenv import load_dotenv
import structlog
from typing import Optional, Dict

try:
    structlog.configure(
        processors=[
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
except structlog.exceptions.AlreadyConfiguredError:
    pass 
log = structlog.get_logger(__name__)

load_dotenv()

print("--- Iniciando Criação/Atualização de Blocos Prefect ---")

# --- Bloco Secret ---
secret_name = "github-access-token"
github_pat = os.getenv("GITHUB_PAT")
print(f"Processando Bloco Secret '{secret_name}'...")
if github_pat:
    try:
        secret_block = Secret(value=github_pat)
        secret_block.save(name=secret_name, overwrite=True)
        print(f"-> Bloco Secret '{secret_name}' salvo com sucesso.")
    except Exception as e:
        print(f"Erro ao salvar Bloco Secret '{secret_name}': {e}")
else:
    print(f"AVISO: Variável de ambiente GITHUB_PAT não definida. Bloco '{secret_name}' não criado/atualizado.")

# --- Bloco JSON para DB Pool ---
db_block_name = "postgres-pool"
db_config = {
    "dsn": os.getenv("DATABASE_URL", "postgresql://user:password@db:5432/pipedrive"),
    "minconn": int(os.getenv("DB_MIN_CONN", 1)),
    "maxconn": int(os.getenv("DB_MAX_CONN", 10)) 
}
print(f"Processando Bloco JSON '{db_block_name}'...")
try:
    json_block_db = JSON(value=db_config)
    json_block_db.save(name=db_block_name, overwrite=True)
    print(f"-> Bloco JSON '{db_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"Erro ao salvar Bloco JSON '{db_block_name}': {e}")


# --- Bloco JSON para Redis Cache ---
redis_block_name = "redis-cache"
redis_config = {
    "connection_string": os.getenv("REDIS_URL", "redis://redis:6379/0")
}
print(f"Processando Bloco JSON '{redis_block_name}'...")
try:
    json_block_redis = JSON(value=redis_config)
    json_block_redis.save(name=redis_block_name, overwrite=True)
    print(f"-> Bloco JSON '{redis_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"Erro ao salvar Bloco JSON '{redis_block_name}': {e}")


# --- Blocos KubernetesJob ---
# Configurações comuns que podem ser reutilizadas
default_image = "pipedrive_metabase_integration-etl:latest"
default_namespace = "default"
default_env_from = [
    {"secretRef": {"name": "app-secrets"}},
    {"secretRef": {"name": "db-secrets"}},
]
default_env = {
    "PUSHGATEWAY_ADDRESS": os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091"),
    "PREFECT_API_URL": "http://prefect-orion:4200/api"
}
default_init_containers = [
     { "name": "wait-for-db", "image": "busybox:1.36", "command": ['sh', '-c', 'echo Waiting for db...; while ! nc -z -w 1 db 5432; do sleep 2; done; echo DB ready.'] },
     { "name": "wait-for-redis", "image": "busybox:1.36", "command": ['sh', '-c', 'echo Waiting for redis...; while ! nc -z -w 1 redis 6379; do sleep 2; done; echo Redis ready.'] },
     { "name": "wait-for-orion", "image": "curlimages/curl:latest", "command": ['sh', '-c', 'echo Waiting for orion...; until curl -sf http://prefect-orion:4200/api/health > /dev/null; do echo -n "."; sleep 3; done; echo Orion ready.'] }
]
default_job_watch_timeout = 120

# Função helper para criar o dicionário do Job Template
def create_job_spec_dict(image: str, resources: dict, pod_labels: Optional[dict] = None) -> dict:
    """Cria a estrutura dict completa do Job K8s."""
    labels = {"app.kubernetes.io/created-by": "prefect"}
    if pod_labels:
        labels.update(pod_labels)
    # Usar a função estática para gerar o dict corretamente estruturado
    return KubernetesJob.job_template(
        metadata={"labels": labels},
        spec={
            "template": {
                "spec": {
                    "initContainers": default_init_containers,
                    "containers": [
                        {
                            "name": "prefect-job",
                            "image": image, # A imagem vai aqui dentro
                            "resources": resources,
                            "envFrom": default_env_from,
                            "env": default_env,
                        }
                    ],
                }
            }
        }
    )

# 1. Bloco para Infraestrutura K8s Padrão
default_k8s_job_block_name = "default-k8s-job"
print(f"Processando Bloco KubernetesJob '{default_k8s_job_block_name}'...")
try:
    default_resources = {
        "requests": {"memory": "1Gi", "cpu": "500m"},
        "limits": {"memory": "4Gi", "cpu": "2"}
    }
    # Gerar o dicionário completo do Job
    default_job_dict = create_job_spec_dict(image=default_image, resources=default_resources)

    # Instanciar o Bloco passando o dicionário para o parâmetro 'job'
    # O __init__ do Bloco deve mapear isso internamente para o campo 'v1_job' esperado pelo schema
    default_k8s_job_block = KubernetesJob(
        namespace=default_namespace,
        job=default_job_dict, # Passar o dict completo aqui
        job_watch_timeout_seconds=default_job_watch_timeout
        # Não passar image aqui, pois já está dentro do job dict
    )
    default_k8s_job_block.save(name=default_k8s_job_block_name, overwrite=True)
    print(f"-> Bloco KubernetesJob '{default_k8s_job_block_name}' salvo com sucesso.")
except Exception as e:
    # Usar logger para capturar traceback completo
    log.error(f"Erro ao salvar Bloco KubernetesJob '{default_k8s_job_block_name}'", error=str(e), exc_info=True)


# 2. Bloco para Infraestrutura K8s do Experimento
experiment_k8s_job_block_name = "experiment-k8s-job"
print(f"Processando Bloco KubernetesJob '{experiment_k8s_job_block_name}'...")
try:
    experiment_resources = {
        "requests": {"memory": "2Gi", "cpu": "1"},
        "limits": {"memory": "8Gi", "cpu": "2"}
    }
    experiment_job_dict = create_job_spec_dict(image=default_image, resources=experiment_resources, pod_labels={"flow": "experiment"})

    experiment_k8s_job_block = KubernetesJob(
        namespace=default_namespace,
        job=experiment_job_dict,
        job_watch_timeout_seconds=default_job_watch_timeout
    )
    experiment_k8s_job_block.save(name=experiment_k8s_job_block_name, overwrite=True)
    print(f"-> Bloco KubernetesJob '{experiment_k8s_job_block_name}' salvo com sucesso.")
except Exception as e:
    log.error(f"Erro ao salvar Bloco KubernetesJob '{experiment_k8s_job_block_name}'", error=str(e), exc_info=True)


# 3. Bloco para Syncs Leves
light_sync_k8s_job_block_name = "light-sync-k8s-job"
print(f"Processando Bloco KubernetesJob '{light_sync_k8s_job_block_name}'...")
try:
    light_sync_resources = {
         "requests": {"memory": "512Mi", "cpu": "250m"},
         "limits": {"memory": "1Gi", "cpu": "500m"}
    }
    light_sync_job_dict = create_job_spec_dict(image=default_image, resources=light_sync_resources, pod_labels={"flow": "light-sync"})

    light_sync_k8s_job_block = KubernetesJob(
        namespace=default_namespace,
        job=light_sync_job_dict,
        job_watch_timeout_seconds=default_job_watch_timeout
    )
    light_sync_k8s_job_block.save(name=light_sync_k8s_job_block_name, overwrite=True)
    print(f"-> Bloco KubernetesJob '{light_sync_k8s_job_block_name}' salvo com sucesso.")
except Exception as e:
    log.error(f"Erro ao salvar Bloco KubernetesJob '{light_sync_k8s_job_block_name}'", error=str(e), exc_info=True)


print("--- Criação/Atualização de Blocos Prefect Concluída ---")