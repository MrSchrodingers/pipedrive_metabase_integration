import unicodedata
import re

def normalize_column_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    # Converte para minúsculas
    name = name.lower()
    # Remove acentuação
    try:
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    except Exception:
        pass
    # Substitui qualquer caractere não alfanumérico (e não _) por underscore
    # Preserva underscores existentes
    name = re.sub(r'[^\w_]+', '_', name)
    # Remove múltiplos underscores seguidos
    name = re.sub(r'_+', '_', name)
    # Remove underscores no início ou fim
    name = name.strip('_')
    # Previne nomes que começam com números (inválido em alguns DBs)
    if name and name[0].isdigit():
        name = '_' + name
    # Handle potential empty names after normalization
    if not name:
        # Generate a fallback name or raise error
        return "_invalid_normalized_name"
    return name