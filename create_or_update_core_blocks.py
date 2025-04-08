from prefect.blocks.system import JSON, Secret
import os
import sys

print("--- Iniciando Criação/Atualização de Blocos Prefect ---")

# --- Variáveis de Ambiente Necessárias ---
required_env_vars = [
    "GITHUB_PAT",
    "POSTGRES_USER",
    "POSTGRES_PASSWORD",
    "POSTGRES_DB",
    "REDIS_URL"
]
missing_vars = [var for var in required_env_vars if not os.getenv(var)]
if missing_vars:
    print(f"!! ERRO: Variáveis de ambiente obrigatórias ausentes: {', '.join(missing_vars)}")
    print("!! Exporte essas variáveis antes de continuar.")
    sys.exit(1)

# --- 1. Bloco Secret do GitHub ---
secret_block_name = "github-access-token"
github_token = os.getenv("GITHUB_PAT")
print(f"Processando Bloco Secret '{secret_block_name}'...")
try:
    secret_block = Secret(value=github_token)
    secret_block.save(name=secret_block_name, overwrite=True)
    print(f"-> Bloco Secret '{secret_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"!! ERRO ao salvar Bloco Secret '{secret_block_name}': {e}")
    sys.exit(1)

# --- 2. Bloco JSON do PostgreSQL ---
db_block_name = "postgres-pool"
db_user = os.getenv("POSTGRES_USER")
db_pass = os.getenv("POSTGRES_PASSWORD")
db_name = os.getenv("POSTGRES_DB")
db_host = "db"
db_port = "5432"
db_dsn = f"postgresql://{db_user}:{db_pass}@{db_host}:{db_port}/{db_name}"
db_config = {
    "dsn": db_dsn,
    "minconn": 1, 
    "maxconn": 5  
}
print(f"Processando Bloco JSON '{db_block_name}'...")
try:
    json_block_db = JSON(value=db_config)
    json_block_db.save(name=db_block_name, overwrite=True)
    print(f"-> Bloco JSON '{db_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"!! ERRO ao salvar Bloco JSON '{db_block_name}': {e}")
    sys.exit(1) 

# --- 3. Bloco JSON do Redis ---
redis_block_name = "redis-cache"
redis_connection_string = os.getenv('REDIS_URL')
redis_config = {
    "connection_string": redis_connection_string
}
print(f"Processando Bloco JSON '{redis_block_name}'...")
try:
    json_block_redis = JSON(value=redis_config)
    json_block_redis.save(name=redis_block_name, overwrite=True)
    print(f"-> Bloco JSON '{redis_block_name}' salvo com sucesso.")
except Exception as e:
    print(f"!! ERRO ao salvar Bloco JSON '{redis_block_name}': {e}")
    sys.exit(1)

print("--- Criação/Atualização de Blocos Prefect Concluída ---")
sys.exit(0) 