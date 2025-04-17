# Pipedrive ⇆ Metabase Integration

![License](https://img.shields.io/badge/license-MIT-blue.svg)
![Build](https://img.shields.io/badge/status-production-green)
![Maintained](https://img.shields.io/badge/maintained-yes-brightgreen)

> Integração robusta entre o CRM Pipedrive e a ferramenta de BI Metabase, com ETL assíncrono, orquestração Kubernetes e observabilidade com Grafana.

---

## 🚀 Features

- ⚡️ ETL de alta performance com staging tables UNLOGGED e `COPY`
- ⏱️ Atualizações quase em tempo real (padrão: 30 min)
- 🧠 Schema dinâmico e suporte a campos customizados
- 📊 Dashboards plug-and-play no Metabase
- 📈 Observabilidade com Prometheus + Grafana
- 🛠️ Deploy automatizado com Minikube + Kubernetes
- 🔁 Sincronizações auxiliares (usuários, pipelines, etc.)
- 🔬 Experimentos de otimização de batch-size

---

## 📚 Índice

1. [Visão Geral](#visão-geral)  
2. [Arquitetura](#arquitetura)  
3. [Stack Tecnológico](#stack-tecnológico)  
4. [Estrutura do Repositório](#estrutura-do-repositório)  
5. [Pré‑requisitos](#pré‑requisitos)  
6. [Guia Rápido (Minikube)](#guia-rápido-minikube)  
7. [Configuração Detalhada](#configuração-detalhada)  
8. [Operações do Dia‑a‑dia](#operações-do-dia‑a‑dia)  
9. [Solução de Problemas](#solução-de-problemas)  
10. [Como Contribuir](#como-contribuir)  
11. [Licença](#licença)  

---

## 🌐 Visão Geral

O projeto oferece um pipeline de dados **Kubernetes‑nativo**, totalmente automatizado e observável:

- **ETL de alta performance** com `COPY` + staging tables
- **Atualização contínua** para dashboards Metabase
- **Métricas detalhadas** de uso, custo de tokens e performance
- **Autoescala e autorecuperação** via HPA e probes

---

## 🧩 Arquitetura

```text
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

---

## 🧪 Stack Tecnológico

| Camada          | Componentes                            | Observações                                                                       |
| --------------- | -------------------------------------- | --------------------------------------------------------------------------------- |
| Orquestração    | **Prefect 3 (Orion)**                  | `prefect.yaml` define 6 deployments                                               |
| Processamento   | Python 3.12, Pandas, Pydantic          | Imagem multi‑stage via Poetry                                                     |
| Armazenamento   | **Postgres 14**, **Redis 7**           | PVC de 5 GiB para dados                                                           |
| Observabilidade | **Prometheus + Pushgateway + Grafana** | Coleta métricas em `/metrics` por anotações                                       |
| Apresentação    | **Metabase**                           | Exposto na porta `3000` no cluster                                                |

---

## 🗂️ Estrutura do Repositório

```text
.
├─ flows/                    # Flows Prefect (ETL principal, back‑fill, syncs…)
├─ infrastructure/
│  ├─ k8s/                   # Manifests + scripts entrypoint/wait
│  ├─ monitoring/            # Helpers de métricas Prometheus
│  └─ prefect/orion/         # Imagem ultraleve do Orion
├─ application/              # Domínio + ports/adapters
├─ run_project               # Build + deploy tudo em um comando
├─ create_secret_block.py    # Registra Secret no Prefect
├─ Dockerfile                # Build da imagem ETL
└─ prefect.yaml              # Deployments do Prefect
```

---

## 🧰 Pré‑requisitos

- **Docker** ≥ 20.x  
- **Minikube** ≥ 1.31  
- **kubectl** ≥ 1.26  
- **Poetry** ≥ 1.5  
- **Prefect CLI** ≥ 2.14  
- GNU `bash`, `make` (para scripts auxiliares)

---

## ⚡ Guia Rápido (Minikube)

```bash
# 1. Clone o repositório e ajuste as variáveis
git clone https://github.com/SUA_ORG/pipedrive_metabase_integration.git
cd pipedrive_metabase_integration
cp .env.template .env           # preencha PIPEDRIVE_API_KEY, POSTGRES_*, ...

# 2. Construa e faça o deploy de tudo
./run_project                   # ~10‑15 min na primeira execução

# 3. Registre o token GitHub como Secret Prefect (opcional)
python create_secret_block.py ghp_<token>

# 4. Inicie um agente Prefect local (caso não rode dentro do cluster)
prefect agent start -q kubernetes
```

| Serviço    | URL                                            | Credenciais padrão  |
| ---------- | ---------------------------------------------- | ------------------- |
| Prefect UI | [http://localhost:4200](http://localhost:4200) | (sem auth)          |
| Postgres   | `localhost:5432`                               | definidas no `.env` |
| Metabase   | [http://localhost:3000](http://localhost:3000) | definir no 1º login |
| Grafana    | [http://localhost:3015](http://localhost:3015) | `admin` / `admin`   |

---

## ⚙️ Configuração Detalhada

### 🔐 Variáveis de Ambiente

| Variável                                            | Descrição                                   |
| --------------------------------------------------- | ------------------------------------------- |
| `PIPEDRIVE_API_KEY`                                 | Token pessoal do Pipedrive                  |
| `POSTGRES_USER`, `POSTGRES_PASSWORD`, `POSTGRES_DB` | Credenciais do banco                        |
| `REDIS_URL`                                         | *Optional* – default `redis://redis:6379/0` |
| `PUSHGATEWAY_ADDRESS`                               | default `pushgateway:9091`                  |

> `entrypoint.sh` aborta se alguma estiver ausente. O script `run_project` gera automaticamente o Secret `app-secrets`.

---

### 🐳 Build das Imagens

| Contexto build                   | Tag gerada                                            |
| -------------------------------- | ----------------------------------------------------- |
| repositório raiz                 | `pipedrive_metabase_integration-etl:latest`           |
| `infrastructure/prefect/orion`   | `pipedrive_metabase_integration-prefect-orion:latest` |
| (metrics reutiliza a imagem ETL) | —                                                     |

---

### ☸️ Deploy no Kubernetes

Executado via:

```bash
./run_project deploy_infra
```

Inclui:

1. ConfigMap de observabilidade  
2. Secrets `app-secrets`, `db-secrets`  
3. PVC `pgdata-pvc`  
4. Stack de monitoramento  
5. Deployments + Services via `pipedrive_metabase_integration.yaml`

---

### 🧠 Deploy dos Flows Prefect

```bash
./run_project deploy_prefect_flows
```

Inclui:

- ETL principal (cada 30 min)
- Back‑fill (manual)
- Syncs auxiliares em CRON
- Experimento de batch-size
- Deploy no pool `kubernetes-pool`

---

### 📈 Observabilidade

- Todas as tasks fazem `push_to_gateway` com métricas
- Prometheus faz scrape automático via `prometheus.io/scrape: true`
- Dashboard em `dashboards/metabase_pipeline.json`

---

## 🛠️ Operações do Dia‑a‑dia

| Ação                | Comando / UI                                      |
| ------------------- | ------------------------------------------------- |
| Ver pipeline        | Grafana → Metabase Pipeline                       |
| Disparar sync       | Prefect UI → Deployments → Run                    |
| Escalar agent       | `kubectl scale deploy/prefect-agent --replicas=N` |
| Logs de métricas    | `kubectl logs -f deploy/metrics`                  |
| Atualizar código    | `git pull && ./run_project`                       |

---

## 🧯 Solução de Problemas

| Sintoma                        | Diagnóstico                                                    |
| ------------------------------ | -------------------------------------------------------------- |
| `create_secret_block.py` falha | Orion está online? O secret já existe?                         |
| Flows Pending                  | Agente online? `work_pool` correto?                            |
| ETL aborta por env ausente     | `.env` e Secret `app-secrets` estão válidos?                   |
| Métricas não aparecem          | Tem annotation `prometheus.io/scrape`? Pushgateway funcionando?|

---

## 🤝 Como Contribuir

1. Faça um fork  
2. Crie uma branch de feature ou fix  
3. Commit → PR explicando claramente o problema ou melhoria

---

## 📄 Licença

Distribuído sob licença **MIT**. Veja `LICENSE` para mais detalhes.
