---
# Pipedrive Metabase Integration

Uma solução **completa** para integrar dados do **Pipedrive** com o **Metabase** por meio de um pipeline **ETL** orquestrado pelo **Prefect** e implantado em **Kubernetes** (via Minikube localmente).
Este projeto extrai dados de **deals** do Pipedrive, realiza **transformações** e **validações**, carrega as informações em um banco **PostgreSQL** e oferece **monitoramento**, **logging** estruturado, além de utilizar **cache com Redis** para garantir desempenho e resiliência. A execução dos fluxos ETL é gerenciada por **Deployments Prefect**, que utilizam um **Agente Prefect** rodando no Kubernetes para buscar código de um repositório Git privado.

---

## Sumário

1.  [Objetivo do Projeto](#objetivo-do-projeto)
2.  [Visão Geral do Fluxo e Arquitetura](#visão-geral-do-fluxo-e-arquitetura)
3.  [Requisitos Mínimos](#requisitos-mínimos)
4.  [Configuração Inicial (Blocos Prefect)](#configuração-inicial-blocos-prefect)
5.  [Como Executar (Automatizado)](#como-executar-automatizado)
6.  [Variáveis de Ambiente e Configuração](#variáveis-de-ambiente-e-configuração)
7.  [Observabilidade e Monitoramento](#observabilidade-e-monitoramento)
8.  [Seção Técnica: Pontos Fortes e Otimizações](#seção-técnica-pontos-fortes-e-otimizações)
9.  [Estrutura do Projeto](#estrutura-do-projeto)
10. [Considerações sobre Alteração de Portas no PostgreSQL](#considerações-sobre-alteração-de-portas-no-postgresql)
11. [Contato](#contato)

---

## Objetivo do Projeto

-   **Centralizar dados** do Pipedrive em um banco PostgreSQL para análises, dashboards e relatórios no Metabase.
-   **Automatizar** a extração, transformação e carga (ETL) dos dados em **pipeline** único, escalável em Kubernetes.
-   **Viabilizar** um fluxo incremental de atualização, carregando apenas dados novos ou modificados.
-   **Prover** **observabilidade** (logs e métricas) para fácil **monitoramento** e **debug** do pipeline.
-   **Orquestrar** as execuções com **Prefect**, facilitando o agendamento, rastreabilidade e escalabilidade dentro do cluster Kubernetes, buscando o código do fluxo diretamente do Git.

---

## Visão Geral do Fluxo e Arquitetura

1.  **Implantação:** O script `run_project.sh` orquestra a implantação de toda a infraestrutura no Kubernetes (Minikube), incluindo: PostgreSQL, Redis, Prefect Orion (servidor), Prefect Agent, Prometheus, Grafana e Metabase.
2.  **Configuração Prefect:** Durante a execução, `run_project.sh` garante que um Bloco Prefect do tipo `Secret` (contendo um token de acesso ao Git) exista e aplica as definições de Deployments Prefect do arquivo `prefect.yaml`.
3.  **Armazenamento de Código:** O código Python dos fluxos ETL (`flows/`) reside em um repositório Git privado.
4.  **Execução do Fluxo:**
    * O **Prefect Agent** (rodando como um Deployment no Kubernetes) monitora um **Work Pool** (`kubernetes-pool`) definido no Prefect Orion.
    * Quando um fluxo agendado (`Pipedrive Sync`) deve rodar, ou um fluxo sob demanda (`Pipedrive Backfill Stage History`) é acionado pela UI/API do Prefect:
        * O Prefect Orion instrui o Agent a criar um **Job Kubernetes** temporário.
        * Este Job utiliza a infraestrutura definida no Bloco Prefect `KubernetesJob` (`k8s-job-infra-block`).
        * A configuração de `pull` no `prefect.yaml` instrui o Job a **clonar o código do fluxo** do repositório Git privado usando o token armazenado no Bloco `Secret`.
        * O Job executa o código Python do fluxo ETL (extração, transformação, carga).
5.  **ETL:**
    * **Extração:** Conecta-se à API do Pipedrive (incrementalmente), usando cache Redis para o último timestamp.
    * **Transformação:** Valida com Pydantic, normaliza e enriquece dados com Pandas.
    * **Carga:** Realiza upsert em lote no PostgreSQL via tabela de staging.
6.  **Observabilidade:** Logs estruturados e métricas Prometheus são coletados e podem ser visualizados no Grafana e na UI do Prefect Orion. O Metabase conecta-se ao PostgreSQL para visualização dos dados.

---

## Requisitos Mínimos

Para utilizar e implantar o projeto em Kubernetes, é necessário ter instalados localmente (ou em seu ambiente de CI/CD):

1.  **Docker** – Necessário para build das imagens.
2.  **kubectl** – Ferramenta de linha de comando para interação com o cluster Kubernetes.
3.  **minikube** (ou outro cluster Kubernetes) – Para executar o ambiente local.
4.  **curl** / **bash** – O script de implantação (`run_project.sh`) utiliza `curl` e funcionalidades de shell (bash).
5.  **Python 3.10+** e **Poetry** – Necessários para executar os scripts auxiliares (`create_secret_block.py`, `register_k8s_block.py`) e gerenciar dependências Python.
6.  **Prefect CLI (v3.x)** – Ferramenta de linha de comando do Prefect (`pip install prefect`).
7.  **Recursos de Hardware** – Recomenda-se:
    * **2+ CPUs** e **8GB+ de RAM** para rodar localmente via Minikube.
    * Espaço em disco suficiente para volumes persistentes (PostgreSQL, Prometheus).

> Observação: Você também pode usar **Kind**, **k3s** ou outro cluster Kubernetes semelhante. Ajuste as configurações de rede e storage conforme necessário.

---

## Configuração Inicial (Blocos Prefect)

Antes da primeira execução automatizada do `run_project.sh`, alguns blocos Prefect precisam existir ou ser criados.

1.  **Bloco de Infraestrutura Kubernetes (`KubernetesJob`):**
    * Este bloco define como os Jobs Kubernetes serão criados para executar os fluxos (imagem Docker, recursos, init containers, etc.).
    * Execute o script fornecido para registrar este bloco no seu servidor Prefect Orion (que estará rodando após as etapas iniciais do `run_project.sh`, ou você pode iniciá-lo separadamente para esta configuração):
        ```bash
        # Certifique-se que seu ambiente Python com 'prefect' está ativo
        # Certifique-se que PREFECT_API_URL aponta para seu Orion (ex: http://localhost:4200/api)
        python scripts/register_k8s_block.py
        ```
    * Este passo geralmente só precisa ser feito **uma vez**, ou se a definição do bloco (`scripts/register_k8s_block.py`) for alterada. O nome padrão do bloco criado é `k8s-job-infra-block`, que já está referenciado no `prefect.yaml`.

2.  **Bloco de Segredo (`Secret`) para Acesso ao Git:**
    * Este bloco armazena o token (ex: GitHub PAT) necessário para clonar o código dos fluxos do seu repositório Git privado.
    * A criação/atualização deste bloco é **automatizada** pelo script `run_project.sh`. Ele utilizará o script `create_secret_block.py` e uma variável de ambiente `GITHUB_PAT` que você **deve fornecer** antes de executar o script principal (veja a próxima seção).
    * O nome padrão do bloco criado automaticamente é `github-access-token`. Certifique-se que seu `prefect.yaml` referencia este nome na seção `pull` -> `access_token`.

---

## Como Executar (Automatizado)

Siga estes passos para implantar e executar o projeto de forma automatizada:

1.  **Clonar o repositório e entrar na pasta:**
    ```bash
    git clone [https://github.com/MrSchrodingers/pipedrive_metabase_integration.git](https://github.com/MrSchrodingers/pipedrive_metabase_integration.git)
    cd pipedrive_metabase_integration
    ```

2.  **Configurar variáveis de ambiente no `.env`:**
    * Copie `.env.example` para `.env` (se existir) ou crie o arquivo `.env`.
    * Preencha as variáveis **NECESSÁRIAS para a aplicação** (NÃO coloque o token do Git aqui):
        * `PIPEDRIVE_API_KEY`: Sua chave da API do Pipedrive.
        * `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB`: Credenciais para o banco de dados PostgreSQL que será criado.
        * (Opcional) `REDIS_CONNECTION_STRING` (padrão `redis://redis:6379/0` geralmente funciona no setup do script).
        * (Opcional) Outras variáveis usadas pela aplicação ou Kubernetes Secrets.
    * Estas variáveis serão usadas para criar os Kubernetes Secrets `db-secrets` e `app-secrets`.

3.  **Registrar o Bloco de Infraestrutura K8s (se for a primeira vez):**
    * Certifique-se que seu ambiente Python com Prefect está ativo.
    * Se o Prefect Orion já estiver rodando (ou após o `run_project.sh` iniciá-lo e configurar o port-forward), execute:
        ```bash
        # Defina a API URL se necessário (ajuste a porta se diferente)
        # export PREFECT_API_URL="http://localhost:4200/api"
        python scripts/register_k8s_block.py
        ```

4.  **Definir a Variável de Ambiente do Token Git:**
    * **MUITO IMPORTANTE:** Exporte seu token de acesso pessoal (GitHub PAT ou similar) como uma variável de ambiente. O script `run_project.sh` usará isso para criar o Bloco `Secret` no Prefect automaticamente.
        ```bash
        # Substitua ghp_... pelo seu token real
        export GITHUB_PAT="ghp_SEU_TOKEN_AQUI_12345abcde"
        ```

5.  **Verificar `prefect.yaml`:**
    * Confirme que a seção `pull` -> `access_token` em **ambos** os deployments (`Pipedrive Sync` e `Pipedrive Backfill Stage History`) referencia o nome correto do Bloco Secret que será criado (o padrão é `github-access-token`):
        ```yaml
        access_token: '{{ prefect.blocks.secret.github-access-token }}'
        ```

6.  **Executar o script principal de deploy (`run_project.sh`):**
    ```bash
    chmod +x run_project.sh
    ./run_project.sh
    ```
    * **O que o script faz:**
        * Verifica dependências (Docker, kubectl, Minikube, Prefect CLI, etc.).
        * Inicia o Minikube (se necessário).
        * Constrói as imagens Docker (`etl` e `orion`).
        * Aplica os manifestos Kubernetes base (`observability-config.yaml`, `db-secrets.yaml`, `prometheus.yml`, etc.).
        * Cria/Atualiza os Kubernetes Secrets `app-secrets` a partir do seu arquivo `.env`.
        * Aplica os manifestos principais (`pipedrive_metabase_integration.yaml`), criando Deployments (Postgres, Redis, Orion, Agent, Metabase, Grafana, etc.) e Services.
        * Aguarda o rollout dos Deployments principais (incluindo `prefect-orion`).
        * Configura `kubectl port-forward` em segundo plano para permitir acesso local aos serviços (Orion, Grafana, Metabase, DB).
        * **Executa `python create_secret_block.py` usando a variável `GITHUB_PAT` para criar/atualizar o Bloco `Secret` `github-access-token` no Prefect Orion.**
        * Cria o Work Pool `kubernetes-pool` no Prefect (se não existir).
        * **Executa `prefect deploy --all` para registrar/atualizar os Deployments definidos no `prefect.yaml` no servidor Orion.**
        * Mantém o script em execução (`wait`) para que os port-forwards permaneçam ativos. **Use Ctrl+C para parar o script e os port-forwards.**

7.  **Acessar e Monitorar:**
    * **Prefect Orion UI:** `http://localhost:4200` - Monitore as execuções dos fluxos, logs, schedules, etc.
    * **Grafana:** `http://localhost:3015` - Visualize métricas de infraestrutura e aplicação (requer configuração inicial de datasource Prometheus).
    * **Metabase:** `http://localhost:3000` - Conecte ao banco de dados e crie dashboards.
    * **PostgreSQL:** `localhost:5432` (via psql ou DBeaver, use as credenciais do `.env`).

8.  **Execução dos Fluxos:**
    * O fluxo `Pipedrive Sync` está agendado para rodar periodicamente (conforme `prefect.yaml`) e será iniciado automaticamente pelo Prefect Agent.
    * O fluxo `Pipedrive Backfill Stage History` não tem schedule e precisa ser acionado manualmente via UI do Prefect Orion ou API/CLI.

9.  **Para Parar Tudo:**
    ```bash
    ./run_project.sh stop
    ```
    * Este comando tentará remover todos os recursos Kubernetes criados (Deployments, Services, PVCs, Secrets, etc.) e parar os processos de port-forward.

---

## Variáveis de Ambiente e Configuração

Resumo dos principais pontos de configuração:

* **`.env` (Arquivo):** Contém segredos para a **aplicação** e **banco de dados**. Usado para criar Kubernetes Secrets (`app-secrets`, `db-secrets`).
    * `PIPEDRIVE_API_KEY` (Obrigatório)
    * `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` (Obrigatório)
    * `REDIS_CONNECTION_STRING` (Opcional, padrão funciona)
* **`GITHUB_PAT` (Variável de Ambiente):** Contém o token de acesso ao Git (GitHub PAT). **Deve ser exportada** no terminal *antes* de rodar `./run_project.sh`. Usada para criar o Bloco `Secret` no Prefect.
* **`prefect.yaml` (Arquivo):** Define os **Deployments Prefect**.
    * `entrypoint`: Aponta para a função Python do fluxo.
    * `work_pool`: Especifica qual pool o Agent deve monitorar.
    * `infrastructure`: Refere-se ao Bloco `KubernetesJob` (`k8s-job-infra-block`) que define como rodar o fluxo no K8s.
    * `pull`: Configura como o código do fluxo é obtido (Git clone).
        * `repository`, `branch`: Detalhes do repositório Git.
        * `access_token`: Referência ao Bloco `Secret` (ex: `{{ prefect.blocks.secret.github-access-token }}`).
    * `schedules`: Define agendamentos (para o `Pipedrive Sync`).
    * `parameters`: Parâmetros padrão para cada fluxo.
* **`pipedrive_metabase_integration.yaml` e outros `.yaml` / `.yml`:** Manifestos Kubernetes para definir os Deployments, Services, ConfigMaps, PVCs, etc., da infraestrutura base (Postgres, Redis, Orion, Agent, Metabase, Grafana, Prometheus, Pushgateway).
* **`create_secret_block.py` (Script):** Script Python auxiliar chamado por `run_project.sh` para criar/atualizar o Bloco `Secret` do token Git no Prefect.
* **`scripts/register_k8s_block.py` (Script):** Script Python para criar/atualizar o Bloco `KubernetesJob` (`k8s-job-infra-block`) no Prefect. Deve ser executado manualmente uma vez ou quando a definição da infraestrutura do Job mudar.

---

## Observabilidade e Monitoramento

1.  **Prefect Orion UI (`http://localhost:4200`):**
    * **Principal interface** para monitorar execuções de fluxos (sucesso, falha, logs).
    * Visualizar e gerenciar Deployments, Work Pools, Blocos e Schedules.
    * Acionar execuções manuais (ex: Backfill).
2.  **Logging Estruturado (JSON):**
    * Logs formatados em JSON para fácil parseamento e agregação.
    * Visíveis nos logs dos pods Kubernetes (`kubectl logs <pod-name>`) e na UI do Prefect Orion para execuções de fluxos.
    * Integrável com ferramentas como Loki, Elasticsearch, etc.
3.  **Métricas Prometheus:**
    * Coleta métricas de latência (ETL, DB, API), contadores (erros, sucessos), gauges (memória, tamanho de lote), etc.
    * Expostas pelo `metrics-server` (se aplicável) e enviadas ao **Pushgateway** por jobs de curta duração (fluxos ETL).
    * Prometheus coleta do Pushgateway e de endpoints diretos (como o `metrics-deployment` se existir).
4.  **Grafana (`http://localhost:3015`):**
    * Plataforma para visualização de métricas.
    * Requer configuração manual da fonte de dados Prometheus (`http://prometheus-service:9090`).
    * Permite criar dashboards para monitorar a saúde da infraestrutura e o desempenho do ETL.
5.  **Metabase (`http://localhost:3000`):**
    * Ferramenta de BI para explorar os dados carregados no PostgreSQL.
    * Requer configuração manual da conexão com o banco de dados (Host: `db`, Porta: `5432`, use credenciais do `.env`).

---

## Seção Técnica: Pontos Fortes e Otimizações

1.  **Upsert Otimizado (PostgreSQL) com COPY + Tabela de Staging:**
    * Os dados são copiados para uma **tabela temporária** via `COPY FROM STDIN`, muito mais rápido do que inserts individuais.
    * Ao final, usa `ON CONFLICT (id) DO UPDATE` para conciliar as atualizações.
2.  **Uso de Pandas para Batches de Transformação:**
    * O `ETLService` transforma dados em lote (batch) com Pandas, reduzindo overhead de loops.
    * Validações e conversões (datas, floats, etc.) são feitas de forma vetorizada.
3.  **Incrementalidade e Cache (Redis):**
    * Busca somente dados novos/alterados (`updated_since`) na API do Pipedrive.
    * Armazena o timestamp incremental no Redis, economizando chamadas na API. Mapas (usuários, stages, pipelines) e lookups de pessoas também são cacheados.
4.  **Campos Customizados Dinâmicos:**
    * O pipeline descobre campos customizados (`dealFields`) via API e os converte em colunas normalizadas no PostgreSQL.
    * Permite adaptação a variações de cada instância do Pipedrive.
5.  **Retentativas (Tenacity) e Circuit Breaker (Pybreaker):**
    * O cliente da API Pipedrive lida com oscilações na rede e na API (timeouts, erros 5xx, rate limits 429).
    * Evita repetir falhas continuamente graças ao **Circuit Breaker**.
6.  **Métricas Granulares e Logging Estruturado:**
    * Facilita a **observabilidade** e **depuração** de problemas.
    * Integra-se rapidamente a painéis de análise e alerta (Grafana, Loki, etc.).
7.  **Orquestração com Prefect em Kubernetes:**
    * Utiliza Deployments Prefect para definir como os fluxos são executados.
    * Prefect Agent gerencia a criação de Jobs Kubernetes sob demanda.
    * Código do fluxo obtido via Git clone diretamente no Job, facilitando atualizações (basta commitar e, se necessário, re-deployar com `prefect deploy`).
8.  **Separação de Responsabilidades e Código Organizado:**
    * Estrutura segue princípios de Ports & Adapters (aproximadamente), separando lógica de aplicação, infraestrutura e domínio.

---

## Estrutura do Projeto

```
pipedrive_metabase_integration/
├── Dockerfile                     # Define a imagem Docker principal (ETL/Agent)
├── pipedrive_metabase_integration.yaml # Manifestos K8s (Deployments, Services principais)
├── run_project.sh                 # Script principal de automação (deploy, stop)
├── create_secret_block.py         # Script Python para criar/atualizar Bloco Secret (chamado por run_project.sh)
├── prefect.yaml                   # Definição dos Deployments Prefect (fluxos, schedule, pull)
├── pyproject.toml                 # Dependências Python (Poetry)
├── poetry.lock                    # Lock file das dependências
├── .env                           # Arquivo para variáveis de ambiente (NÃO versionar segredos!)
├── .gitignore
├── README.md                      # Este arquivo
├── infrastructure/                # Código relacionado a tecnologias externas (DB, API, Cache, K8s, Prefect)
│   ├── api_clients/
│   │   └── pipedrive_api_client.py  # Cliente para API Pipedrive com retry, cache, circuit breaker
│   ├── cache.py                     # Implementação do Cache Redis
│   ├── config/
│   │   └── settings.py              # Configurações carregadas via Pydantic Settings
│   ├── db.py                        # (Pode conter definições de schema SQLAlchemy se usado)
│   ├── db_pool.py                   # Pool de conexões PostgreSQL (psycopg)
│   ├── k8s/                         # Scripts de entrypoint/wait para containers K8s
│   │   ├── entrypoint.sh
│   │   └── wait-for-it.sh
│   ├── logging_config.py            # Configuração do logging estruturado (Structlog)
│   ├── monitoring/                  # Métricas Prometheus e servidor de métricas
│   │   ├── metrics.py
│   │   └── metrics_server.py
│   ├── prefect/                     # Configurações específicas do Prefect (ex: Dockerfile do Orion)
│   │   └── orion/
│   │       └── Dockerfile             # Dockerfile para a imagem do Prefect Orion
│   └── repository_impl/
│       └── pipedrive_repository.py    # Implementação do repositório de dados (PostgreSQL)
├── application/                   # Lógica de negócio principal, schemas, ports
│   ├── ports/                       # Interfaces (Portas) para a infraestrutura
│   │   ├── data_repository_port.py
│   │   └── pipedrive_client_port.py
│   ├── schemas/                     # Schemas Pydantic para validação de dados
│   │   └── deal_schema.py
│   ├── services/                    # Serviços que orquestram a lógica (ETLService)
│   │   └── etl_service.py
│   └── utils/                       # Funções utilitárias de transformação/processamento
│       ├── column_utils.py
│       └── ...
├── flows/                         # Definições dos fluxos Prefect
│   ├── pipedrive_deployments.py     # Script para gerar/aplicar Deployments Prefect via Python (alternativa ao prefect.yaml)
│   └── pipedrive_metabase_etl.py    # Contém as funções @flow (main_etl_flow, backfill_stage_history_flow)
├── core_domain/                   # (Opcional) Entidades e objetos de valor centrais do domínio
│   └── ...
├── scripts/                       # Scripts auxiliares de configuração/manutenção
│   └── register_k8s_block.py      # Script para registrar o Bloco KubernetesJob no Prefect
├── tests/                         # Testes unitários e de integração
│   └── ...
├── db-secrets.yaml                # Exemplo/Template para segredos K8s do DB
├── observability-config.yaml      # ConfigMap K8s para configurações de observabilidade
├── persistent-volume-claim.yaml   # Definição dos PVCs K8s (Postgres, Prometheus)
├── prometheus.yml                 # Configuração K8s do Prometheus (Deployment, Service, RBAC, ConfigMap)
└── pushgateway.yaml               # Configuração K8s do Prometheus Pushgateway

```

---

## Considerações sobre Alteração de Portas no PostgreSQL

Se você precisar rodar **dois** bancos PostgreSQL no mesmo cluster (por exemplo, um usando a porta **5432** e outro **5433**), deve-se:

1.  Alterar o comando de inicialização do container para a nova porta:
    ```yaml
    command: ["postgres", "-p", "5433"]
    ```
2.  Ajustar as sondas (`readinessProbe`, `livenessProbe`) para a mesma porta:
    ```yaml
    readinessProbe:
      exec: # Atualizar o comando para usar a nova porta
        command: ["pg_isready", "-U", "$(POSTGRES_USER)", "-d", "$(POSTGRES_DB)", "-h", "127.0.0.1", "-p", "5433"]
    livenessProbe:
      tcpSocket:
        port: 5433 # Ou atualizar exec se usar pg_isready
    ```
3.  Atualizar o `Service` para expor corretamente a nova porta:
    ```yaml
    ports:
      - port: 5433 # Porta que outros serviços no cluster usarão
        targetPort: 5433 # Porta onde o container está escutando
    ```

Apenas mudar `containerPort` ou `service.port` **não** faz o PostgreSQL escutar internamente em outra porta. É preciso atualizar o comando do processo PostgreSQL e as verificações de saúde.

---

## Contato

-   Para dúvidas, sugestões ou problemas, abra uma **issue** neste repositório.
-   E-mail de suporte e feedback: **mrschrodingers@gmail.com** ou **suporte da DEBT - Matheus Munhoz**.