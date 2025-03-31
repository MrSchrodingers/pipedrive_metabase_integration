# Pipedrive Metabase Integration

Uma solução completa para integrar os dados do Pipedrive com o Metabase através de um pipeline ETL orquestrado pelo Prefect. Este projeto extrai dados de deals do Pipedrive, realiza transformações e validações, carrega os dados em um banco PostgreSQL e oferece monitoramento, logging estruturado e cache com Redis para garantir desempenho e resiliência.

---

## Visão Geral

- **Extração de Dados:**  
  Conecta à API do Pipedrive para buscar dados de deals utilizando técnicas de retentativas (com Tenacity) e cache (Redis) para atualizar incrementalmente somente os registros modificados desde a última execução.

- **Transformação e Carga (ETL):**  
  O pipeline ETL é orquestrado com Prefect e utiliza Apache Beam para transformar os dados de forma paralela. Os dados são validados, normalizados e carregados no PostgreSQL utilizando inserções em massa via comando COPY.

- **Observabilidade e Monitoramento:**  
  Implementa logging estruturado (em formato JSON) com a biblioteca `python-json-logger`, facilitando a centralização dos logs via ELK ou Loki. Além disso, o sistema coleta métricas de execução do ETL, latência e utilização de recursos com Prometheus.

- **Infraestrutura e Orquestração:**  
  A aplicação é empacotada em containers Docker utilizando multi-stage build para otimização. O ambiente pode ser executado localmente com Docker Compose e, em produção, os manifests Kubernetes (incluindo ConfigMaps, Deployments, Services e HPAs) gerenciam o ambiente escalável.

---

## Arquitetura do Projeto

A estrutura do projeto está organizada em camadas para garantir a separação de responsabilidades:

- **infrastructure:**  
  Contém as configurações e implementações para acesso a bancos de dados, cache (Redis), logging, monitoramento e clientes de API externos (Pipedrive).

- **application:**  
  Inclui casos de uso, serviços (como o ETLService), utilitários de transformação de dados e interfaces para integração com repositórios e APIs.

- **core_domain:**  
  Define os conceitos de domínio, entidades, eventos e objetos de valor, isolando a lógica de negócio principal.

- **flows:**  
  Contém os fluxos de orquestração (Prefect) para a execução do pipeline ETL, bem como scripts de deploy.

- **tests:**  
  Possui os testes unitários e de integração para garantir a qualidade e confiabilidade do sistema.

- **Outros Arquivos:**  
  Arquivos de configuração como `pyproject.toml`, `Dockerfile`, `docker-compose.yml` e os manifests do Kubernetes (`pipedrive_metabase_integration.yaml`, `observability-config.yaml`).

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
  **Observação:**  
  Por padrão, a imagem oficial do PostgreSQL escuta na porta 5432. Se for necessário rodar dois bancos simultaneamente (por exemplo, um na porta 5432 e outro na 5433), é preciso **alterar a configuração do container**. Basta definir a porta desejada (por exemplo, com a variável de ambiente `PGPORT` ou alterando o comando de inicialização para `postgres -p 5433`) e atualizar as referências nos manifests (readiness probes, service, etc.). Apenas mudar o `containerPort` e a porta do service não altera a porta interna em que o PostgreSQL está escutando, o que pode impedir o rollout do deployment.

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

2. **Verifique os logs e o status dos containers:**

   ```bash
   docker compose logs -f
   docker compose ps
   ```

### Usando o Script de Deploy

O script **run_project.sh** orquestra o ambiente completo, incluindo o reset de recursos no Kubernetes e o build dos containers sem usar cache.

#### Para forçar a reconstrução sem cache, o script utiliza:

```bash
docker build --no-cache -t pipedrive_metabase_integration-etl:latest .
docker build --no-cache -t pipedrive_metabase_integration-prefect-orion:latest ./infrastructure/prefect/orion
docker build --no-cache -t pipedrive_metabase_integration-metrics:latest .
```

Além disso, para limpar imagens e caches não utilizados, você pode executar:

```bash
docker system prune -a --volumes
```

#### Como usar o script:

1. Dê permissão de execução:

   ```bash
   chmod +x run_project.sh
   ```

2. Para reiniciar o ambiente (parar tudo e depois iniciar):

   ```bash
   ./run_project.sh stop && ./run_project.sh start
   ```

   *Observação:* Se você não quiser que o script utilize um _trap_ para encerrar os port-forwards (o que pode impedir o acesso contínuo ao Orion ou ao banco), remova ou comente a linha com o `trap`.

---

## Deploy no Kubernetes

Os manifests Kubernetes para deploy, serviço e Horizontal Pod Autoscaler (HPA) estão definidos no arquivo `pipedrive_metabase_integration.yaml`.

Para aplicá-los:

```bash
kubectl apply -f pipedrive_metabase_integration.yaml
```

Também aplique o ConfigMap para observabilidade:

```bash
kubectl apply -f observability-config.yaml
```

### Exemplo de configuração do Deployment do PostgreSQL

Se desejar rodar o PostgreSQL em uma porta diferente (por exemplo, 5433), **lembre-se de atualizar também o comando de inicialização do container**. Por exemplo, você pode alterar o manifest para:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: db
  labels:
    app: db
spec:
  replicas: 1
  selector:
    matchLabels:
      app: db
  template:
    metadata:
      labels:
        app: db
    spec:
      containers:
      - name: db
        image: postgres:14
        ports:
        - containerPort: XXXX
        env:
        - name: POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: db-secrets
              key: POSTGRES_USER
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: db-secrets
              key: POSTGRES_PASSWORD
        - name: POSTGRES_DB
          valueFrom:
            secretKeyRef:
              name: db-secrets
              key: POSTGRES_DB
        command: ["postgres", "-p", "XXXX"]
        volumeMounts:
        - name: pgdata
          mountPath: /var/lib/postgresql/data
        readinessProbe:
          tcpSocket:
            port: XXXX
          initialDelaySeconds: 5
          periodSeconds: 10
        livenessProbe:
          tcpSocket:
            port: XXXX
          initialDelaySeconds: 10
          periodSeconds: 20
      volumes:
      - name: pgdata
        persistentVolumeClaim:
          claimName: pgdata-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: db
spec:
  selector:
    app: db
  ports:
    - protocol: TCP
      port: XXXX
      targetPort: XXXX
```

> **Importante:** Alterar somente o `containerPort` ou o `service` sem atualizar o comando de inicialização **não fará o PostgreSQL escutar na nova porta**, o que fará com que o rollout nunca seja concluído, pois as sondas (readiness/liveness) não conseguirão se conectar.

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

---

## Considerações Finais

- **Cache no Build:**  
  Para evitar o uso de camadas em cache durante a construção da imagem, o script utiliza o parâmetro `--no-cache` no comando `docker build`. Você também pode executar:
  ```bash
  docker system prune -a --volumes
  ```
  para remover imagens e caches não utilizados.

- **Port-Forward para Acesso Local:**  
  O script de deploy inicia port-forwards para o Prefect Orion (na porta XXXX) e para o PostgreSQL (na porta definida – XXXX ou a nova porta que você configurar). Se desejar encerrar os port-forwards automaticamente ao sair, configure o _trap_ conforme necessário; caso contrário, remova-o para manter o acesso contínuo.

- **Banco de Dados:**  
  Se precisar rodar dois bancos (por exemplo, um em XXXX e outro em XXXX), certifique-se de que cada Deployment está configurado para forçar o PostgreSQL a escutar na porta desejada (utilizando variáveis de ambiente como `PGPORT` ou alterando o comando de inicialização) e que os serviços e sondas (readiness/liveness) são atualizados de acordo.

---

## Contato

Para dúvidas, sugestões ou problemas, abra uma issue ou entre em contato através do mrschrodingers@gmail.com ou o suprte da DEBT - Matheus Munhoz.
