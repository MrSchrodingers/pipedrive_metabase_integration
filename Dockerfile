# ---- Etapa 1: Builder ----
    FROM python:3.12-slim as builder

    WORKDIR /app
    ENV INTERNAL_ENV=${INTERNAL_ENV} \
        PYTHONFAULTHANDLER=1 \
        PYTHONUNBUFFERED=1 \
        PYTHONHASHSEED=random \
        PYTHONDONTWRITEBYTECODE=1 \
        PIP_NO_CACHE_DIR=off \
        PIP_DISABLE_PIP_VERSION_CHECK=on \
        PIP_DEFAULT_TIMEOUT=100 \
        POETRY_NO_INTERACTION=1 \
        POETRY_VIRTUALENVS_CREATE=false \
        POETRY_CACHE_DIR='/var/cache/pypoetry' \
        POETRY_HOME='/usr/local' \
        POETRY_VERSION=2.1.1
    
    RUN apt-get update && \
        apt-get install -y curl gcc libpq-dev netcat-openbsd && \
        rm -rf /var/lib/apt/lists/*
    
    # Instalar o Poetry e configurá-lo para não criar virtualenv
    RUN curl -sSL https://install.python-poetry.org | python3 -
    ENV PATH="$POETRY_HOME/bin:$PATH"
    RUN poetry config virtualenvs.create false
    
    # Copiar os arquivos de dependências e instalar os pacotes
    COPY pyproject.toml poetry.lock* ./
    RUN poetry install --no-root --no-interaction --no-ansi && \
    poetry run pip install prefect-sqlalchemy --no-cache-dir

    # Copiar o código-fonte completo
    COPY . .
    
    # ---- Etapa 2: Imagem Final ----
    FROM python:3.12-slim
    
    WORKDIR /app
    ENV INTERNAL_ENV_FINAL=${INTERNAL_ENV_FINAL} \
    PYTHONPATH=/app \
    PYTHONFAULTHANDLER=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONHASHSEED=random \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=off \
    PIP_DISABLE_PIP_VERSION_CHECK=on \
    PIP_DEFAULT_TIMEOUT=100 \
    POETRY_NO_INTERACTION=1 \
    POETRY_VIRTUALENVS_CREATE=false
    
    RUN apt-get update && \
        apt-get install -y libpq-dev curl netcat-openbsd && \
        rm -rf /var/lib/apt/lists/*
    
    # Copiar os pacotes instalados e o código da etapa builder
    COPY --from=builder /usr/local /usr/local
    COPY --from=builder /app /app
    
    # Garantir que os scripts tenham permissão de execução
    COPY infrastructure/k8s/wait-for-it.sh /app/wait-for-it.sh
    COPY infrastructure/k8s/entrypoint.sh /app/entrypoint.sh
    RUN chmod +x /app/wait-for-it.sh
    RUN chmod +x /app/entrypoint.sh
    
    # Comando de entrada
    CMD ["/app/entrypoint.sh"]
    