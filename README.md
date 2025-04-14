# Pipeboard - Integração Pipedrive + Metabase

![Status](https://img.shields.io/badge/status-em%20execu%C3%A7%C3%A3o-green)
![License](https://img.shields.io/badge/license-Propriet%C3%A1rio-red)
![Python](https://img.shields.io/badge/python-3.10+-blue)
![Kubernetes](https://img.shields.io/badge/k8s-minikube%20%7C%20prod%20ready-orange)
![Prefect](https://img.shields.io/badge/prefect-v3.x-brightgreen)
![PostgreSQL](https://img.shields.io/badge/postgres-15+-blueviolet)

---

## 💡 Visão Geral

O **Pipeboard** é uma solução robusta e extensível de ETL desenvolvida internamente na **Pavcob**, com o objetivo de extrair, transformar e carregar dados do **Pipedrive** para um banco **PostgreSQL** com visualização no **Metabase**. Utiliza o **Prefect** para orquestração e o **Kubernetes** para escalabilidade. Conta com monitoramento por **Prometheus/Grafana**, logging estruturado e cache com **Redis**.

Este repositório foi projetado para ser acessível para qualquer desenvolvedor interno da Pavcob, com execução automatizada e estrutura de pastas intuitiva. Tudo pode ser reproduzido localmente com Minikube.

---

## 🌍 Propósito do Projeto

- Centralizar dados do Pipedrive para BI.
- Garantir fluxo incremental e com tolerância a falhas.
- Executar ETL robusto com métricas, logs e cache.
- Permitir escala futura e manutenção simplificada.

---

## 🚀 Como Executar (Resumo)

```bash
# 1. Clonar o projeto
$ git clone git@gitlab.pavcob.internal/pipeboard.git && cd pipeboard

# 2. Definir variáveis de ambiente (.env)
$ cp .env.example .env && nano .env

# 3. Exportar token do GitHub (usado para pull dos fluxos Prefect)
$ export GITHUB_PAT="<seu-token-aqui>"

# 4. Executar o deploy automatizado
$ chmod +x run_project.sh
$ ./run_project.sh
```

---

## 🧳 Infraestrutura Provisionada

- PostgreSQL 15
- Redis 6
- Prefect Orion + Agent
- Prometheus + Pushgateway
- Grafana
- Metabase
- Metrics Server customizado

---

## ⚖️ Tecnologias e Arquitetura

- **Python 3.10+** com **Poetry**
- **Prefect v3** para orquestração
- **Kubernetes + Minikube** (com suporte a ambientes cloud)
- **PostgreSQL** para armazenamento estruturado
- **Redis** para cache incremental
- **Prometheus + Pushgateway** para métricas
- **Grafana** e **Metabase** para visualização

---

## 🔍 Estrutura do Projeto

```
pipeboard/
├── application/         # Código de negócio e ETLService
├── infrastructure/      # Repositórios, clients API, cache, DB
├── flows/               # Fluxos Prefect com @flow
├── scripts/             # Scripts auxiliares para registrar blocos Prefect
├── run_project.sh       # Script principal de execução (automático)
├── prefect.yaml         # Definição dos Deployments Prefect
├── .env                 # Segredos e configuração (criar manualmente)
└── pipedrive_metabase_integration.yaml  # Manifests K8s
```

---

## 🔔 Observabilidade e Monitoramento

- **Prometheus:** Coleta métricas customizadas do ETL e Pushgateway.
- **Grafana:** Visualização de dashboards de ETL, batch, memória, falhas.
- **Prefect UI:** Logs, schedules, execuções e status dos fluxos.
- **Logging:** Formatado com Structlog (JSON, contexto estruturado).

---

## 🏆 Diferenciais da Solução

1. **Upsert via COPY + staging:** Alta performance e segurança de dados.
2. **Streaming e batching:** Memória otimizada, mesmo com 1M+ registros.
3. **Campos customizados dinâmicos:** Schema auto-adaptável.
4. **Histórico de Stage:** Permite analisar pipeline ao longo do tempo.
5. **Cache Redis + API resiliente:** Busca incremental, circuit breaker.
6. **Métricas granulares:** Latência, falhas, memória, qualidade dos dados.
7. **Backfill robusto:** Para recuperar histórico retroativo sob demanda.
8. **Deploy 100% automatizado:** Com rollback e port-forward incluído.

---

## 🔧 Manutenção e Acesso

- Para adicionar novos campos ou entidades:
  - Atualize os campos em `etl_service.py` e `deal_schema.py`
  - Rode `main_etl_flow` ou `backfill_stage_history_flow`
- Para execuções manuais:
  - Use a UI Prefect (http://localhost:4200)
  - Ou `prefect deployments run` via CLI

---

## 🤝 Contato Interno

- **Responsável técnico:** Matheus Munhoz - mrschrodingers@gmail.com
- **Sugestões:** Abra uma issue no repositório interno GitLab

---