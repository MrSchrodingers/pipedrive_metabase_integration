## Pipedrive Metabase Integration

Uma solução **completa** para integrar dados do **Pipedrive** com o **Metabase** por meio de um pipeline **ETL** orquestrado pelo **Prefect** e implantado em **Kubernetes**.  
Este projeto extrai dados de **deals** do Pipedrive, realiza **transformações** e **validações**, carrega as informações em um banco **PostgreSQL** e oferece **monitoramento**, **logging** estruturado, além de utilizar **cache com Redis** para garantir desempenho e resiliência.

---

## Sumário

1. [Objetivo do Projeto](#objetivo-do-projeto)  
2. [Visão Geral do Fluxo e Arquitetura](#visão-geral-do-fluxo-e-arquitetura)  
3. [Requisitos Mínimos](#requisitos-mínimos)  
4. [Como Executar no Kubernetes](#como-executar-no-kubernetes)  
5. [Variáveis de Ambiente e Configuração](#variáveis-de-ambiente-e-configuração)  
6. [Observabilidade e Monitoramento](#observabilidade-e-monitoramento)  
7. [Seção Técnica: Pontos Fortes e Otimizações](#seção-técnica-pontos-fortes-e-otimizações)  
8. [Estrutura do Projeto](#estrutura-do-projeto)  
9. [Considerações sobre Alteração de Portas no PostgreSQL](#considerações-sobre-alteração-de-portas-no-postgresql)  
10. [Contato](#contato)

---

## Objetivo do Projeto

- **Centralizar dados** do Pipedrive em um banco PostgreSQL para análises, dashboards e relatórios no Metabase.  
- **Automatizar** a extração, transformação e carga (ETL) dos dados em **pipeline** único, escalável em Kubernetes.  
- **Viabilizar** um fluxo incremental de atualização, carregando apenas dados novos ou modificados.  
- **Prover** **observabilidade** (logs e métricas) para fácil **monitoramento** e **debug** do pipeline.  
- **Orquestrar** as execuções com Prefect, facilitando o agendamento, rastreabilidade e escalabilidade dentro do cluster.

---

## Visão Geral do Fluxo e Arquitetura

1. **Extração de Dados (Extract):**  
   - Conecta-se à **API do Pipedrive** para buscar deals de maneira incremental (com `updated_since`).  
   - Utiliza **retentativas automáticas** (Tenacity) e **circuit breaker** para robustez.  
   - Armazena o último timestamp no **Redis** para evitar leituras redundantes e otimizar chamadas à API.

2. **Transformação (Transform):**  
   - Dados validados com **Pydantic** (`DealSchema`).  
   - Normalizações de datas, valores monetários, nomes de usuários, pipelines e campos customizados.  
   - Utiliza **Pandas** para manipulação em lote, aproveitando vetorização e alto desempenho.

3. **Carga (Load):**  
   - Realiza **upsert** em lote (bulk) no PostgreSQL usando uma tabela de _staging_ temporária (COPY), reduzindo tempo de escrita.  
   - Ao final, atualiza o “último timestamp processado” no Redis, para a próxima execução pegar apenas o que mudou.

4. **Observabilidade:**  
   - **Logs estruturados** (JSON) integráveis a ELK/Loki.  
   - **Métricas Prometheus** para acompanhar tempo de execução, uso de memória, tamanho de lote, etc.  
   - **Grafana** e **Metabase** para dashboards e visualizações.

---

## Requisitos Mínimos

Para utilizar e implantar o projeto em Kubernetes, é necessário ter instalados localmente (ou em seu ambiente de CI/CD):

1. **Docker** – Necessário para build das imagens.  
2. **kubectl** – Ferramenta de linha de comando para interação com o cluster Kubernetes.  
3. **minikube** (ou outro cluster Kubernetes) – Para executar o ambiente local de desenvolvimento/testes.  
4. **curl** / **bash** – O script de implantação (`run_project.sh`) utiliza `curl` e funcionalidades de shell (bash).  
5. **Recursos de Hardware** – Recomenda-se:  
   - **2 CPUs** e **8GB de RAM** para rodar localmente via Minikube sem problemas de desempenho.  
   - Espaço em disco suficiente para persistir dados em volumes (PostgreSQL, Redis, etc.).

> Observação: Você também pode usar **Kind**, **k3s** ou outro cluster Kubernetes semelhante. Basta ajustar as configurações de rede e storage de acordo.

---

## Como Executar no Kubernetes

### Passo a Passo

1. **Clonar o repositório e entrar na pasta:**
   ```bash
   git clone https://github.com/seu_usuario/pipedrive_metabase_integration.git
   cd pipedrive_metabase_integration
   ```

2. **Configurar as variáveis de ambiente (.env):**  
   Crie ou edite o arquivo `.env` com suas credenciais do PostgreSQL, Redis e `PIPEDRIVE_API_KEY`.  
   - Você também pode criar secrets no Kubernetes via `kubectl create secret ...`, conforme os exemplos do manifesto (`db-secrets.yaml`, `app-secrets`, etc.).

3. **Executar o script de deploy (`run_project.sh`):**  
   ```bash
   chmod +x run_project.sh
   ./run_project.sh
   ```  
   Este script fará:
   - **start** do Minikube (se não estiver rodando)  
   - build das imagens Docker  
   - aplicação dos manifests Kubernetes (Services, Deployments, Jobs, etc.)  
   - configuração de *port-forwards* para você acessar localmente o **Prefect Orion**, **PostgreSQL**, **Metabase**, **Grafana**, etc.

4. **Verificar o rollout de todos os deployments:**  
   O script já aguarda o rollout. Caso queira checar manualmente:  
   ```bash
   kubectl get pods
   kubectl get deployments
   ```

5. **Executar o Job ETL:**  
   O manifesto `pipedrive_metabase_integration.yaml` já inclui um Job `etl`. Ele pode rodar automaticamente ou você pode acionar manualmente:  
   ```bash
   kubectl create job --from=cronjob/etl ...
   ```  
   (ou conforme suas necessidades; o script costuma rodar o job e aguardar a finalização).

> Para **parar** tudo e remover os recursos, execute:  
> ```bash
> ./run_project.sh stop
> ```  
> Isso tentará deletar todos os deployments, services, jobs e secrets. Também encerrará o Minikube se estiver rodando localmente.

---

## Variáveis de Ambiente e Configuração

No arquivo `.env`, você pode definir:

- **PIPEDRIVE_API_KEY**: chave da API do Pipedrive.  
- **POSTGRES_USER**, **POSTGRES_PASSWORD**, **POSTGRES_HOST**, **POSTGRES_DB**: credenciais e host do PostgreSQL.  
- **REDIS_CONNECTION_STRING**: string de conexão do Redis (ex. `redis://redis:6379/0`).  
- **APP_METRICS_PORT**: porta em que o servidor de métricas expõe `/metrics`.  
- **OUTRAS VARIÁVEIS** relevantes ao seu ambiente, vistas em `settings.py` ou nos manifests de Kubernetes (`.yaml`).

> Os secrets relacionados ao DB podem ser armazenados em `db-secrets.yaml`; basta atualizar as chaves. O script `run_project.sh` também cria secrets a partir de `.env` quando executado.

---

## Observabilidade e Monitoramento

1. **Logging Estruturado (JSON):**  
   - Converte logs para JSON (via [python-json-logger](https://github.com/madzak/python-json-logger)).  
   - Fácil integração com ELK/Loki para centralizar logs.

2. **Métricas Prometheus:**  
   - Coleta métricas de latência, contagem de falhas e tamanho de lote do ETL, entre outras.  
   - Expostas por padrão na porta `8082` (endpoint `/metrics`).  
   - O `metrics_server.py` gerencia a publicação dessas métricas.

3. **Grafana e Metabase:**  
   - **Grafana** conecta-se ao Prometheus para dashboards de monitoramento.  
   - **Metabase** conecta-se ao PostgreSQL para dashboards de dados de vendas, funil, previsões etc.

4. **Prefect Orion:**  
   - Interface de orquestração para o ETL.  
   - Geralmente acessível em `http://localhost:4200` quando rodando via port-forward.

---

## Seção Técnica: Pontos Fortes e Otimizações

1. **Upsert Otimizado (PostgreSQL) com COPY + Tabela de Staging:**  
   - Os dados são copiados para uma **tabela temporária** via `COPY FROM STDIN`, muito mais rápido do que inserts individuais.  
   - Ao final, usa `ON CONFLICT (id) DO UPDATE` para conciliar as atualizações.

2. **Uso de Pandas para Batches de Transformação:**  
   - O `ETLService` transforma dados em lote (batch) com Pandas, reduzindo overhead de loops.  
   - Validações e conversões (datas, floats, etc.) são feitas de forma vetorizada.

3. **Incrementalidade e Cache (Redis):**  
   - Busca somente dados novos/alterados (`updated_since`).  
   - Armazena o timestamp incremental no Redis, economizando chamadas na API do Pipedrive.

4. **Campos Customizados Dinâmicos:**  
   - O pipeline descobre campos customizados (`dealFields`) e os converte em colunas normalizadas no PostgreSQL.  
   - Permite adaptação a variações de cada instância do Pipedrive.

5. **Retentativas (Tenacity) e Circuit Breaker:**  
   - Lida com oscilações na rede e na API.  
   - Evita repetir falhas continuamente graças ao **Circuit Breaker** (pybreaker).

6. **Métricas Granulares e Logging Estruturado:**  
   - Facilita a **observabilidade** e **depuração** de problemas.  
   - Integra-se rapidamente a painéis de análise e alerta.

---

## Estrutura do Projeto

```
pipedrive_metabase_integration/
├── Dockerfile
├── pipedrive_metabase_integration.yaml    # Manifests principais do K8s
├── run_project.sh                         # Script de deploy e controle
├── .env
├── infrastructure/
│   ├── monitoring/
│   │   ├── metrics.py
│   │   └── metrics_server.py
│   ├── api_clients/
│   │   └── pipedrive_api_client.py
│   ├── repository_impl/
│   │   └── pipedrive_repository.py
│   ├── db_pool.py
│   ├── cache.py
│   └── ...
├── application/
│   ├── services/
│   │   └── etl_service.py
│   ├── schemas/
│   │   └── deal_schema.py
│   ├── ports/
│   │   ├── data_repository_port.py
│   │   └── pipedrive_client_port.py
│   └── utils/
│       ├── replace_nan_with_none_recursive.py
│       └── column_utils.py
├── flows/
│   └── pipedrive_metabase_etl.py
├── core_domain/
│   └── ...
└── tests/
    └── ...
```

---

## Considerações sobre Alteração de Portas no PostgreSQL

Se você precisar rodar **dois** bancos PostgreSQL no mesmo cluster (por exemplo, um usando a porta **5432** e outro **5433**), deve-se:

1. Alterar o comando de inicialização do container para a nova porta:  
   ```yaml
   command: ["postgres", "-p", "5433"]
   ```
2. Ajustar as sondas (`readinessProbe`, `livenessProbe`) para a mesma porta:  
   ```yaml
   readinessProbe:
     tcpSocket:
       port: 5433
   livenessProbe:
     tcpSocket:
       port: 5433
   ```
3. Atualizar o `Service` para expor corretamente a nova porta:  
   ```yaml
   ports:
     - port: 5433
       targetPort: 5433
   ```

Apenas mudar `containerPort` ou `service.port` **não** faz o PostgreSQL escutar internamente em outra porta. É preciso atualizar o binário para escutar na nova porta também.

---

## Contato

- Para dúvidas, sugestões ou problemas, abra uma **issue** neste repositório.  
- E-mail de suporte e feedback: **mrschrodingers@gmail.com** ou **suprte da DEBT - Matheus Munhoz**.
