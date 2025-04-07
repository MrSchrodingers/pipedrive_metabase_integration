import os
from prefect_kubernetes import KubernetesJob

K8S_IMAGE_NAME = "pipedrive_metabase_integration-etl:latest"
K8S_NAMESPACE = "default"
PUSHGATEWAY_SVC_ADDRESS = os.getenv("PUSHGATEWAY_ADDRESS", "pushgateway:9091")
PREFECT_API_URL_FOR_JOB = "http://prefect-orion:4200/api" 

# Definição dos recursos e env vars 
DEFAULT_JOB_RESOURCES = {
    "requests": {"memory": "1Gi", "cpu": "500m"},
    "limits": {"memory": "4Gi", "cpu": "2"}
}
DEFAULT_INIT_CONTAINERS = [
    {
        "name": "wait-for-db",
        "image": "busybox:1.36",
        "command": ['sh', '-c', 'echo Waiting for db...; while ! nc -z -w 1 db 5432; do sleep 2; done; echo DB ready.'],
    },
    {
        "name": "wait-for-redis",
        "image": "busybox:1.36",
        "command": ['sh', '-c', 'echo Waiting for redis...; while ! nc -z -w 1 redis 6379; do sleep 2; done; echo Redis ready.'],
    },
     {
        "name": "wait-for-orion",
        "image": "curlimages/curl:latest",
        "command": ['sh', '-c', 'echo Waiting for orion...; until curl -sf http://prefect-orion:4200/api/health > /dev/null; do echo -n "."; sleep 3; done; echo Orion ready.'],
    }
]
DEFAULT_ENV_FROM = [
    {"secretRef": {"name": "app-secrets"}},
    {"secretRef": {"name": "db-secrets"}},
]
DEFAULT_ENV = {
    "PUSHGATEWAY_ADDRESS": PUSHGATEWAY_SVC_ADDRESS,
    "PREFECT_API_URL": PREFECT_API_URL_FOR_JOB,
}

k8s_job_block = KubernetesJob(
    image=K8S_IMAGE_NAME,
    image_pull_policy='NEVER',
    namespace=K8S_NAMESPACE,
    env=DEFAULT_ENV, 
    job=KubernetesJob.job_template(
         metadata={"labels": {"app.kubernetes.io/created-by": "prefect"}},
         spec={
             "template": {
                 "spec": {
                     "initContainers": DEFAULT_INIT_CONTAINERS,
                     "containers": [{
                         "name": "prefect-job",
                         "resources": DEFAULT_JOB_RESOURCES,
                         "envFrom": DEFAULT_ENV_FROM,
                     }],
                 }
             }
         }
    ),
    stream_output=True
)

block_name = "k8s-job-infra-block"
print(f"Salvando bloco de infraestrutura Kubernetes como '{block_name}'...")
k8s_job_block.save(block_name, overwrite=True)
print(f"Bloco '{block_name}' salvo com sucesso!")