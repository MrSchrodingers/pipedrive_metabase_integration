from typing import Any, Dict
import unicodedata
import re
import pandas as pd

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

def flatten_custom_fields(custom_fields: Dict[str, Any], repo_custom_mapping: Dict[str, str]) -> Dict[str, Any]:
    flat_dict = {}
    for api_key, normalized_name in repo_custom_mapping.items():
        field_data = custom_fields.get(api_key)

        if field_data is None:
            flat_dict[normalized_name] = None
            continue

        if isinstance(field_data, dict):
            flattened_subfields = pd.json_normalize(field_data, sep='_').to_dict(orient='records')[0]
            for sub_key, sub_val in flattened_subfields.items():
                flat_dict[f"{normalized_name}_{sub_key}"] = sub_val
        else:
            flat_dict[normalized_name] = field_data

    return flat_dict