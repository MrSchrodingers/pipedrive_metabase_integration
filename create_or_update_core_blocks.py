import os
from prefect.blocks.system import Secret, JSON
from prefect_kubernetes.jobs import KubernetesJob
from dotenv import load_dotenv
import structlog

load_dotenv()
log = structlog.get_logger()

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

# 1. Bloco para Infraestrutura K8s Padrão
default_k8s_job_block_name = "default-k8s-job"
print(f"Processando Bloco KubernetesJob '{default_k8s_job_block_name}'...")
try:
    default_k8s_job_block = KubernetesJob(
        image=default_image,
        namespace=default_namespace,
        env=default_env,
        env_from=default_env_from,
        init_containers=default_init_containers,
        resources={ 
            "requests": {"memory": "1Gi", "cpu": "500m"},
            "limits": {"memory": "4Gi", "cpu": "2"}
        },
        job_watch_timeout_seconds=default_job_watch_timeout,
    )
    default_k8s_job_block.save(name=default_k8s_job_block_name, overwrite=True)
    print(f"-> Bloco KubernetesJob '{default_k8s_job_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"Erro ao salvar Bloco KubernetesJob '{default_k8s_job_block_name}': {e}", exc_info=True) 

# 2. Bloco para Infraestrutura K8s do Experimento (Recursos Maiores)
experiment_k8s_job_block_name = "experiment-k8s-job"
print(f"Processando Bloco KubernetesJob '{experiment_k8s_job_block_name}'...")
try:
    experiment_k8s_job_block = KubernetesJob(
        image=default_image,
        namespace=default_namespace,
        env=default_env,
        env_from=default_env_from,
        init_containers=default_init_containers,
        resources={ 
            "requests": {"memory": "2Gi", "cpu": "1"},
            "limits": {"memory": "8Gi", "cpu": "2"}
        },
        job_watch_timeout_seconds=default_job_watch_timeout,
    )
    experiment_k8s_job_block.save(name=experiment_k8s_job_block_name, overwrite=True)
    print(f"-> Bloco KubernetesJob '{experiment_k8s_job_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"Erro ao salvar Bloco KubernetesJob '{experiment_k8s_job_block_name}': {e}", exc_info=True)

# 3. Bloco para Syncs Leves (Recursos Menores)
light_sync_k8s_job_block_name = "light-sync-k8s-job"
print(f"Processando Bloco KubernetesJob '{light_sync_k8s_job_block_name}'...")
try:
    light_sync_k8s_job_block = KubernetesJob(
        image=default_image,
        namespace=default_namespace,
        env=default_env,
        env_from=default_env_from,
        init_containers=default_init_containers,
        resources={
             "requests": {"memory": "512Mi", "cpu": "250m"},
             "limits": {"memory": "1Gi", "cpu": "500m"}
        },
        job_watch_timeout_seconds=default_job_watch_timeout,
    )
    light_sync_k8s_job_block.save(name=light_sync_k8s_job_block_name, overwrite=True)
    print(f"-> Bloco KubernetesJob '{light_sync_k8s_job_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"Erro ao salvar Bloco KubernetesJob '{light_sync_k8s_job_block_name}': {e}", exc_info=True)

print("--- Criação/Atualização de Blocos Prefect Concluída ---")