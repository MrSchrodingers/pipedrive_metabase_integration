# Pipedrive ⇆ Metabase Integration
> **Tags:** ETL · Prefect · Pipedrive · Metabase · PostgreSQL · Prometheus · Grafana · Kubernetes · Python

&#x20; &#x20;

> **Integra dados de negócios (Deals) e entidades auxiliares do Pipedrive em um banco Postgres, tornando-os disponíveis no Metabase quase em tempo real; o fluxo é orquestrado pelo Prefect e monitorado via Prometheus + Grafana.**

---

## Índice&#x20;

1. [Visão Geral](#visão-geral)
2. [Arquitetura](#arquitetura)
3. [Stack Tecnológico](#stack-tecnológico)
4. [Estrutura do Repositório](#estrutura-do-repositório)
5. [Pré‑requisitos](#pré‑requisitos)
6. [Guia Rápido (Minikube)](#guia-rápido-minikube)
7. [Configuração Detalhada](#configuração-detalhada)
   1. [Variáveis de Ambiente](#variáveis-de-ambiente)
   2. [Build das Imagens](#build-das-imagens)
   3. [Deploy no Kubernetes](#deploy-no-kubernetes)
   4. [Deploy dos Flows Prefect](#deploy-dos-flows-prefect)
   5. [Observabilidade](#observabilidade)
8. [Operações do Dia‑a‑dia](#operações-do-dia‑a‑dia)
9. [Solução de Problemas](#solução-de-problemas)
10. [Como Contribuir](#como-contribuir)
11. [Licença](#licença)

---

## Visão Geral

O projeto oferece um pipeline de dados **Kubernetes‑nativo**, totalmente automatizado e observável:

- **ETL de alta performance** usando tabelas de *staging* UNLOGGED e `COPY`, com *schema evolution* automático.
- **Atualização quase em tempo‑real** das dashboards do Metabase (intervalo padrão: 30 min).
- **Métricas detalhadas**: custos de tokens Pipedrive, latência da API, tamanhos de *batch*, qualidade dos dados, uso de CPU/memória e muito mais.
- **Autoscaling** via HPA e *self‑healing* de Pods com `livenessProbe`/`readinessProbe`.

## Arquitetura

```
┌────────────┐     API            ┌──────────────┐
│  Pipedrive │ ─────────────────► │  ETL Flows   │
└────────────┘                    │ (Prefect)    │
        ▲                         └──────┬───────┘
        │   Lookups / back‑fill          │
        │                                 ▼
┌────────────┐  Queries        ┌────────────────┐
│  Metabase  │ ◄────────────── │   Postgres     │
└────────────┘                 └────────────────┘
        ▲                                 │
        │  Dashboards / alerts            │ pushgateway
        │                                 ▼
┌────────────┐ <── PromQL ────┌────────────────┐
│  Grafana   │                │ Prometheus     │
└────────────┘                └────────────────┘
```

Todos os componentes são descritos em `pipedrive_metabase_integration.yaml`, implantando **7 Deployments** e seus respectivos Services/HPAs.

## Stack Tecnológico

| Camada          | Componentes                            | Observações                                                                       |
| --------------- | -------------------------------------- | --------------------------------------------------------------------------------- |
| Orquestração    | **Prefect 3 (Orion)**                  | `prefect.yaml` define 6 deployments (ETL, back‑fill, syncs, experimento de batch) |
| Processamento   | Python 3.12, Pandas, Pydantic          | Imagem multi‑stage via Poetry                                                     |
| Armazenamento   | **Postgres 14**, **Redis 7**           | PVC de 5 GiB para dados                                                           |
| Observabilidade | **Prometheus + Pushgateway + Grafana** | Coleta métricas em `/metrics` por anotações                                       |
| Apresentação    | **Metabase**                           | Exposto na porta `3000` no cluster                                                |

## Estrutura do Repositório

```text
.
├─ flows/                    # Flows Prefect (ETL principal, back‑fill, syncs…)
├─ infrastructure/
│  ├─ k8s/                   # Manifests + scripts entrypoint/wait
│  ├─ monitoring/            # Helpers de métricas Prometheus
│  └─ prefect/orion/         # Imagem ultraleve do Orion
├─ application/              # Domínio + ports/adapters
├─ run_project                 # Build + deploy tudo em um comando
├─ create_secret_block.py    # Registra Secret no Prefect
├─ Dockerfile                # Build da imagem ETL
└─ prefect.yaml              # Deployments do Prefect
```

## Pré‑requisitos

- **Docker** ≥ 20.x
- **Minikube** ≥ 1.31
- **kubectl** ≥ 1.26
- **Poetry** ≥ 1.5
- **Prefect CLI** ≥ 2.14
- GNU `bash`, `make` (para scripts auxiliares)

## Guia Rápido (Minikube)

```bash
# 1. Clone o repositório e ajuste as variáveis
git clone https://github.com/SUA_ORG/pipedrive_metabase_integration.git
cd pipedrive_metabase_integration
cp .env.template .env           # preencha PIPEDRIVE_API_KEY, POSTGRES_*, ...

# 2. Construa e faça o deploy de tudo
./run_project                     # ~10‑15 min na primeira execução

# 3. Registre o token GitHub como Secret Prefect (opcional)
python create_secret_block.py ghp_<token>

# 4. Inicie um agente Prefect local (caso não rode dentro do cluster)
prefect agent start -q kubernetes
```

Quando o script terminar, acesse:

| Serviço    | URL                                            | Credenciais padrão  |
| ---------- | ---------------------------------------------- | ------------------- |
| Prefect UI | [http://localhost:4200](http://localhost:4200) | (sem auth)          |
| Postgres   | `localhost:5432`                               | definidas no `.env` |
| Metabase   | [http://localhost:3000](http://localhost:3000) | definir no 1º login |
| Grafana    | [http://localhost:3015](http://localhost:3015) | `admin` / `admin`   |

## Configuração Detalhada

### Variáveis de Ambiente

`entrypoint.sh` aborta se alguma estiver faltando:

| Variável                                            | Descrição                                   |
| --------------------------------------------------- | ------------------------------------------- |
| `PIPEDRIVE_API_KEY`                                 | Token pessoal do Pipedrive                  |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Credenciais do banco                        |
| `REDIS_URL`                                         | *Optional* – default `redis://redis:6379/0` |
| `PUSHGATEWAY_ADDRESS`                               | default `pushgateway:9091`                  |

O script `run_project` converte o `.env` em um **Secret** Kubernetes (`app-secrets`) para evitar vazamento de chaves.

### Build das Imagens

Três imagens são construídas:

| Contexto build                   | Tag gerada                                            |
| -------------------------------- | ----------------------------------------------------- |
| repositório raiz                 | `pipedrive_metabase_integration-etl:latest`           |
| `infrastructure/prefect/orion`   | `pipedrive_metabase_integration-prefect-orion:latest` |
| (metrics reutiliza a imagem ETL) | —                                                     |

### Deploy no Kubernetes

`run_project deploy_infra` aplica em ordem:

1. ConfigMap `observability-config` (nível de log, path métricas)
2. Secrets `app-secrets`, `db-secrets`
3. PVC `pgdata-pvc` (5 GiB)
4. Stack de monitoramento (Prometheus, Pushgateway, Grafana)
5. Manifesto principal `pipedrive_metabase_integration.yaml`

### Deploy dos Flows Prefect

- **ETL principal:** intervalo padrão 30 min.
- **Back‑fill:** gatilho manual.
- **Syncs auxiliares:** usuários, pessoas/orgs, stages/pipelines em CRON.
- **Experimento de batch‑size:** otimiza throughput.

`run_project deploy_prefect_flows` executa `prefect deploy --all` e garante o `work_pool` *kubernetes-pool*.

### Observabilidade

- Cada task ETL faz `push_to_gateway` de métricas: latência, custo de tokens, erros, etc.
- Prometheus descobre serviços com `prometheus.io/scrape: "true"`.
- Dashboard pronto em `dashboards/metabase_pipeline.json`.

## Operações do Dia‑a‑dia

| Tarefa              | Comando / UI                                      |
| ------------------- | ------------------------------------------------- |
| Monitorar pipeline  | Grafana → dashboard **Metabase Pipeline**         |
| Disparar sync agora | Prefect UI → Deployments → *Run*                  |
| Escalonar workers   | `kubectl scale deploy/prefect-agent --replicas=N` |
| Ver logs de um Pod  | `kubectl logs -f deploy/metrics`                  |
| Atualizar código    | `git pull && ./run_project`                       |

## Solução de Problemas

| Sintoma                        | Verificações                                                      |
| ------------------------------ | ----------------------------------------------------------------- |
| `create_secret_block.py` falha | Orion acessível em `localhost:4200/api/health`? Secret já existe? |
| Flows **Pending**              | Agente online? `work_pool` correto?                               |
| ETL aborta por env ausente     | `.env` e Secret `app-secrets` válidos?                            |
| Métricas não aparecem          | Serviço tem anotações `prometheus.io/…`? Pushgateway ativo?       |

## Como Contribuir

1. Fork → branch → commit → PR.
2. Descreva claramente o problema/feature.

## Licença

Distribuído sob licença **MIT**. Consulte `LICENSE` para mais detalhes.


