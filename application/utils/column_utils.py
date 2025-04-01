import unicodedata
import re

def normalize_column_name(name: str) -> str:
    # Converte para minúsculas
    name = name.lower()
    # Remove acentuação
    name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    # Substitui qualquer caractere não alfanumérico por underscore
    name = re.sub(r'[^\w]+', '_', name)
    # Remove underscores no início ou fim
    name = name.strip('_')
    return name
