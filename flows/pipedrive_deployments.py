import os
from datetime import timedelta
from prefect.deployments import Deployment
from prefect.server.schemas.schedules import IntervalSchedule 
from prefect_kubernetes.jobs import KubernetesJob


from flows.pipedrive_metabase_etl import main_etl_flow, backfill_stage_history_flow

# --- Configuração Comum da Infraestrutura K8s Job ---
K8S_IMAGE_NAME = "pipedrive_metabase_integration-etl:latest"
K8S_NAMESPACE = "default" 
WORK_QUEUE_NAME = "kubernetes" 

# Definir recursos padrão para os jobs de fluxo
DEFAULT_JOB_RESOURCES = {
    "requests": {"memory": "1Gi", "cpu": "500m"},
    "limits": {"memory": "4Gi", "cpu": "2"}
}

# Definir Init Containers padrão para aguardar dependências
DEFAULT_INIT_CONTAINERS = [
    {
        "name": "wait-for-db",
        "image": "busybox:1.36",
        "command": ['sh', '-c', 'echo Waiting for db service...; while ! nc -z -w 1 db 5432; do sleep 2; done; echo DB ready.'],
    },
    {
        "name": "wait-for-redis",
        "image": "busybox:1.36",
        "command": ['sh', '-c', 'echo Waiting for redis service...; while ! nc -z -w 1 redis 6379; do sleep 2; done; echo Redis ready.'],
    },
     {
        "name": "wait-for-orion",
        "image": "curlimages/curl:latest", 
        "command": ['sh', '-c', 'echo Waiting for orion service...; until curl -sf http://prefect-orion:4200/api/health > /dev/null; do echo -n "."; sleep 3; done; echo Orion ready.'],
    }
]

# Definir variáveis de ambiente e secrets comuns
DEFAULT_ENV_FROM = [
    {"secretRef": {"name": "app-secrets"}},
    {"secretRef": {"name": "db-secrets"}},
]
PUSHGATEWAY_SVC_ADDRESS = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")

DEFAULT_ENV = {
    "PUSHGATEWAY_ADDRESS": PUSHGATEWAY_SVC_ADDRESS,
    "PREFECT_API_URL": "http://prefect-orion:4200/api"
}

k8s_job_infra = KubernetesJob(
    image=K8S_IMAGE_NAME,
    namespace=K8S_NAMESPACE,
    job=KubernetesJob.job_template(
        metadata={
            "labels": {"app.kubernetes.io/created-by": "prefect"}
        },
        spec={
            "template": {
                "spec": {
                    "initContainers": DEFAULT_INIT_CONTAINERS,
                    "containers": [
                        {
                            "name": "prefect-job", 
                            "resources": DEFAULT_JOB_RESOURCES,
                            "envFrom": DEFAULT_ENV_FROM,       
                            "env": [ {"name": k, "value": v} for k, v in DEFAULT_ENV.items() ], 
                        }
                    ],
                }
            }
        }
    ),
    # Timeout para esperar o Job K8s INICIAR (não completar)
    job_watch_timeout_seconds=120,
)

# --- Deployment para Main Sync ---
main_sync_deployment = Deployment.build_from_flow(
    flow=main_etl_flow,
    name="Pipedrive Sync", 
    description="Sincroniza dados recentes do Pipedrive com o banco de dados.",
    version="1.2", 
    tags=["pipedrive", "sync", "etl"],
    # Schedule para rodar a cada 30 minutos
    schedule=IntervalSchedule(interval=timedelta(minutes=30)),
    # Parâmetros padrão para este deployment
    parameters={"run_batch_size": 1500},
    # Usar a infraestrutura K8s Job definida acima
    infrastructure=k8s_job_infra,
    # Especificar a fila que o agente escuta
    work_queue_name=WORK_QUEUE_NAME,
)

# --- Deployment para Backfill ---
backfill_deployment = Deployment.build_from_flow(
    flow=backfill_stage_history_flow,
    name="Pipedrive Backfill Stage History",
    description="Preenche o histórico de stages para deals antigos.",
    version="1.0",
    tags=["pipedrive", "backfill", "history"],
    # SEM schedule inicial, será disparado por automação
    schedule=None,
    # Parâmetros padrão para este deployment
    parameters={"daily_deal_limit": 10000, "db_batch_size": 1000},
    infrastructure=k8s_job_infra,
    work_queue_name=WORK_QUEUE_NAME,
)

# --- Deployment para Recents (Exemplo Futuro) ---
# Supondo que exista um fluxo recent_updates_flow
# from flows.pipedrive_recents import recent_updates_flow # Exemplo
# recent_updates_deployment = Deployment.build_from_flow(
#     flow=recent_updates_flow,
#     name="Pipedrive Recents",
#     version="1.0",
#     tags=["pipedrive", "recents", "etl"],
#     schedule=IntervalSchedule(interval=timedelta(minutes=15)),
#     is_schedule_active=False, # Começa desativado, ativado pela Automação 3
#     parameters={},
#     infrastructure=k8s_job_infra,
#     work_queue_name=WORK_QUEUE_NAME,
# )

# Bloco para aplicar os deployments
# Este script deve ser executado DEPOIS que o Prefect Orion estiver no ar
if __name__ == "__main__":
    print(f"Aplicando deployment para '{main_sync_deployment.name}'...")
    main_sync_deployment.apply()

    print(f"Aplicando deployment para '{backfill_deployment.name}'...")
    backfill_deployment.apply()

    # print(f"Aplicando deployment para '{recent_updates_deployment.name}' (schedule inativo)...")
    # recent_updates_deployment.apply()

    print("\nDeployments aplicados com sucesso!")
    print(f"Garanta que um agente Prefect esteja rodando e escutando a fila '{WORK_QUEUE_NAME}'.")
    print(f"Exemplo: prefect agent start -q {WORK_QUEUE_NAME}")
    print("Configure as Automações na UI do Prefect para orquestrar os fluxos.")