import sys
from prefect.blocks.system import Secret
import argparse
import os

# --- Configuration ---
default_block_name = "github-access-token"

# --- Argument Parser ---
parser = argparse.ArgumentParser(description="Cria ou atualiza um Bloco Prefect do tipo Secret.")
parser.add_argument("-n", "--name", default=default_block_name,
                    help=f"Nome para o Bloco Secret (padrão: {default_block_name})")
parser.add_argument("token", help="O valor do token secreto a ser armazenado")

args = parser.parse_args()

block_name = args.name
secret_value = args.token

print(f"Tentando criar/atualizar Bloco Secret chamado '{block_name}'...")

try:
    secret_block = Secret(value=secret_value)

    block_doc_id = secret_block.save(name=block_name, overwrite=True)

    print(f"Bloco Secret '{block_name}' salvo com sucesso!")
    print(f"ID do Documento do Bloco: {block_doc_id}")

    print("\n--- Próximo Passo ---")
    print("Verifique se o seu arquivo 'prefect.yaml' está referenciando o nome de bloco correto:")
    print(f"access_token: '{{{{ prefect.blocks.secret.{block_name} }}}}'")
    print("Certifique-se de que esta linha está correta em TODOS os deployments que precisam do token.")


except Exception as e:
    print(f"\nErro ao salvar o bloco: {e}")
    print("\nPossíveis causas:")
    print("- O servidor Prefect Orion não está acessível.")
    print("- A variável de ambiente PREFECT_API_URL não está definida ou está incorreta.")
    print("  (Certifique-se que aponta para: http://localhost:4200/api)")
    sys.exit(1)