# Pipedrive Metabase Integration

Uma solução completa para integrar os dados do Pipedrive com o Metabase através de um pipeline ETL, orquestrado pelo Prefect. Este projeto extrai dados de deals do Pipedrive, realiza transformações e validações, carrega os dados em um banco PostgreSQL e oferece monitoramento, logging estruturado e cache com Redis para garantir desempenho e resiliência.

---

## Visão Geral

- **Extração de Dados:**  
  Conecta à API do Pipedrive para buscar dados de deals utilizando técnicas de retentativas (com Tenacity) e cache (Redis) para atualizar incrementalmente somente os registros modificados desde a última execução.

- **Transformação e Carga (ETL):**  
  O pipeline ETL é orquestrado com Prefect e utiliza Apache Beam para transformar os dados de forma paralela. Os dados são validados, normalizados e carregados no PostgreSQL utilizando inserções em massa via comando COPY.

- **Observabilidade e Monitoramento:**  
  Implementa logging estruturado (em formato JSON) com a biblioteca `python-json-logger`, facilitando a centralização dos logs via ELK ou Loki. Além disso, o sistema coleta métricas de execução do ETL, latência e utilização de recursos com Prometheus.

- **Infraestrutura e Orquestração:**  
  A aplicação está empacotada em containers Docker utilizando um multi-stage build para otimização. O ambiente pode ser executado localmente com Docker Compose e, em produção, os manifests Kubernetes (incluindo ConfigMaps, Deployments, Services e HPAs) gerenciam o ambiente escalável.

---

## Arquitetura do Projeto

A estrutura do projeto está organizada em camadas para garantir separação de responsabilidades:

- **infrastructure:**  
  Contém as configurações e implementações de acesso a bancos de dados, cache (Redis), logging, monitoramento e clientes de API externos (Pipedrive).

- **application:**  
  Inclui os casos de uso, serviços (como o ETLService), utilitários de transformação de dados e portas (interfaces) para integração com repositórios e APIs.

- **core_domain:**  
  Define os conceitos de domínio, entidades, eventos e objetos de valor, isolando a lógica de negócio principal.

- **flows:**  
  Contém os fluxos de orquestração (Prefect) para a execução do pipeline ETL, bem como scripts de deploy.

- **tests:**  
  Possui os testes unitários e de integração para garantir a qualidade e a confiabilidade do sistema.

- **Outros Arquivos:**  
  Arquivos de configuração como `pyproject.toml`, `Dockerfile`, `docker-compose.yml`, e os manifests do Kubernetes (`pipedrive_metabase_integration.yaml`, `observability-config.yaml`).

---

## Pré-requisitos

- **Docker & Docker Compose:**  
  Para build e execução local dos containers.

- **Kubernetes:**  
  Um cluster Kubernetes (por exemplo, Minikube, Kind ou ambiente de produção) para deploy escalável.

- **Redis:**  
  Serviço de cache (já incluído no `docker-compose.yml`).

- **PostgreSQL:**  
  Banco de dados para armazenamento dos dados.

- **Prefect:**  
  Para orquestração dos fluxos ETL.

- **Variáveis de Ambiente:**  
  Configure as variáveis de ambiente necessárias em um arquivo `.env` na raiz do projeto.

---

## Instalação

1. **Clone o repositório:**

   ```bash
   git clone https://github.com/seu_usuario/pipedrive_metabase_integration.git
   cd pipedrive_metabase_integration
   ```

2. **Configure o arquivo .env:**  
   Edite o arquivo `.env` com suas configurações de banco de dados, chave do Pipedrive, Prefect, etc.

3. **Instale as dependências (opcional, para execução local via Poetry):**

   ```bash
   poetry install
   ```

---

## Execução Local

### Usando Docker Compose

1. **Suba os containers:**

   ```bash
   docker compose up --build -d
   ```

2. **Verifique os logs e status dos containers:**

   ```bash
   docker compose logs -f
   docker compose ps
   ```

### Usando o Script de Deploy

Um script shell chamado **run_project.sh** foi criado para orquestrar todo o ambiente, incluindo Docker Compose e Kubernetes:

```bash
#!/bin/bash
set -e

echo "=============================="
echo "Resetando containers com Docker Compose..."
echo "=============================="
docker compose down --volumes --remove-orphans
docker compose up --build -d

echo "Aguardando 10 segundos para os containers iniciarem..."
sleep 10

echo "=============================="
echo "Aplicando ConfigMap de Observabilidade no Kubernetes..."
echo "=============================="
kubectl apply -f observability-config.yaml

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

echo "=============================="
echo "Limpando deployments existentes no Prefect..."
echo "=============================="
DEPLOYMENT_IDS=$(PREFECT_API_URL=http://localhost:4200/api poetry run prefect deployment ls | grep "/" || true)
if [ -n "$DEPLOYMENT_IDS" ]; then
  for dep_id in $DEPLOYMENT_IDS; do
    echo "Deletando deployment: $dep_id"
    PREFECT_API_URL=http://localhost:4200/api poetry run prefect deployment delete "$dep_id" --yes
  done
else
  echo "Nenhum deployment encontrado."
fi

echo "=============================="
echo "Iniciando o fluxo ETL em modo serve..."
echo "=============================="
PREFECT_API_URL=http://localhost:4200/api poetry run python -m flows.pipedrive_metabase_etl

echo "=============================="
echo "Reset e deploy concluídos."
echo "=============================="
```

**Como usar:**

1. Dê permissão de execução:

   ```bash
   chmod +x run_project.sh
   ```

2. Execute o script:

   ```bash
   ./run_project.sh
   ```

---

## Deploy no Kubernetes

Os manifests Kubernetes para deploy, serviço e Horizontal Pod Autoscaler (HPA) estão definidos no arquivo `pipedrive_metabase_integration.yaml`. Para aplicá-los:

```bash
kubectl apply -f pipedrive_metabase_integration.yaml
```

Além disso, o ConfigMap para observabilidade é definido no arquivo `observability-config.yaml`:

```bash
kubectl apply -f observability-config.yaml
```

---

## Observabilidade e Monitoramento

- **Logging Estruturado:**  
  O módulo `infrastructure/logging_config.py` configura logs em formato JSON, permitindo a integração com ferramentas como ELK ou Loki.

- **Métricas:**  
  As métricas do ETL, como contadores de execuções, falhas e latência, estão definidas em `infrastructure/monitoring/metrics.py`. O servidor de métricas é iniciado via `infrastructure/monitoring/metrics_server.py`.

- **Monitoramento via Prometheus:**  
  As métricas são expostas em `/metrics` e podem ser coletadas pelo Prometheus.

---

## Estrutura do Projeto

```plaintext
pipedrive_metabase_integration/
├── Dockerfile
├── docker-compose.yml
├── observability-config.yaml
├── pipedrive_metabase_integration.yaml      # Manifests Kubernetes
├── run_project.sh                           # Script de deploy completo
├── .env
├── README.md
├── pyproject.toml
├── infrastructure/
│   ├── api_clients/
│   │   ├── pipedrive_api_client.py
│   │   └── __init__.py
│   ├── cache.py
│   ├── config/
│   │   ├── settings.py
│   │   └── __init__.py
│   ├── db.py
│   ├── db_pool.py
│   ├── logging_config.py
│   ├── monitoring/
│   │   ├── metrics.py
│   │   ├── metrics_server.py
│   │   └── __init__.py
│   ├── prefect/
│   │   └── orion/
│   │       └── Dockerfile
│   ├── repository_impl/
│   │   ├── pipedrive_repository.py
│   │   └── __init__.py
│   └── __init__.py
├── application/
│   ├── ports/
│   │   ├── data_repository_port.py
│   │   ├── pipedrive_client_port.py
│   │   └── __init__.py
│   ├── services/
│   │   ├── etl_service.py
│   │   └── __init__.py
│   ├── use_cases/
│   │   ├── process_pipedrive_data.py
│   │   └── __init__.py
│   ├── utils/
│   │   ├── data_transform.py
│   │   └── __init__.py
│   └── __init__.py
├── flows/
│   ├── deploy.py
│   ├── pipedrive_metabase_etl.py
│   └── __init__.py
├── core_domain/
│   ├── entities/
│   │   ├── pipedrive_entity.py
│   │   └── __init__.py
│   ├── events/
│   │   ├── data_updated.py
│   │   └── __init__.py
│   ├── value_objects/
│   │   ├── identifier.py
│   │   └── __init__.py
│   └── __init__.py
└── tests/
    ├── test_infrastructure.py
    ├── test_use_cases.py
    └── __init__.py
```