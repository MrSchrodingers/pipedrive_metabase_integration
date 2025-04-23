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
    apt-get install -y curl gcc libpq-dev netcat-openbsd git && \
    rm -rf /var/lib/apt/lists/*

RUN curl -sSL https://install.python-poetry.org | python3 -
ENV PATH="$POETRY_HOME/bin:$PATH"

COPY pyproject.toml poetry.lock* ./
RUN poetry install --no-root --no-ansi && \
    poetry run pip install prefect-sqlalchemy --no-cache-dir

COPY . .

# ─── Etapa 2: Imagem Final ─────────────────────────────────────
FROM python:3.12-slim

WORKDIR /app
ENV PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=on

RUN apt-get update && \
    apt-get install -y curl gcc libpq-dev netcat-openbsd git && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /usr/local /usr/local
COPY --from=builder /app /app

# scripts utilitários
COPY infrastructure/k8s/wait-for-it.sh /usr/local/bin/wait-for-it.sh
RUN chmod +x /usr/local/bin/wait-for-it.sh

# ─── Segurança: roda como usuário não-root ─────────────────────
RUN adduser --disabled-password --gecos '' app && chown -R app /app
USER app

# ─── Healthcheck genérico ──────────────────────────────────────
HEALTHCHECK --interval=30s --timeout=3s --start-period=30s \
  CMD curl -f http://localhost:8082/metrics || exit 1

CMD ["python", "-m", "pipeldummy"]
