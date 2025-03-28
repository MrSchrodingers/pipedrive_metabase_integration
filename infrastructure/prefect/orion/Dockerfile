FROM prefecthq/prefect:3.2.14-python3.12
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

CMD ["prefect", "server", "start", "--host", "0.0.0.0"]
