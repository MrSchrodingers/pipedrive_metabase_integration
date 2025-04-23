# ─── Etapa 1: Builder ───────────────────────────────────────────
FROM python:3.12-slim AS builder

WORKDIR /app
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_HOME='/usr/local' \
    POETRY_VERSION=2.1.1

RUN apt-get update && \
    apt-get install -y curl gcc libpq-dev netcat-openbsd git jq && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="$POETRY_HOME/bin:$PATH"

COPY ../../pyproject.toml ../../poetry.lock* ./
RUN poetry install --no-root --no-ansi && \
    poetry run pip install prefect-sqlalchemy

COPY ../../ . 

# ─── Etapa 2: Imagem Final ─────────────────────────────────────
FROM python:3.12-slim AS final

WORKDIR /app
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on

RUN apt-get update && \
    apt-get install -y curl gcc libpq-dev netcat-openbsd git jq && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local /usr/local
COPY --from=builder /app /app

# garante que o entrypoint esteja no caminho certo
COPY ../../infrastructure/k8s/entrypoint.sh /app/infrastructure/k8s/entrypoint.sh
RUN chmod +x /app/infrastructure/k8s/entrypoint.sh

RUN adduser --disabled-password --gecos '' app && chown -R app /app
USER app

# Healthcheck
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s \
  CMD curl -f http://localhost:8082/metrics || exit 1

# Entrypoint atualizado
ENTRYPOINT ["/app/infrastructure/k8s/entrypoint.sh"]
