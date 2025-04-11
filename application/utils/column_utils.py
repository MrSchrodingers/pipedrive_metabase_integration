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

import json
import pandas as pd

def explode_address_field(df: pd.DataFrame, source_column: str, prefix: str) -> pd.DataFrame:
    def parse_json_safe(val):
        try:
            return json.loads(val) if isinstance(val, str) else val
        except Exception:
            return {}

    parsed = df[source_column].apply(parse_json_safe)

    df[f"endereco_completo_combinado_de_{prefix}"] = parsed.apply(lambda x: x.get("formatted_address"))
    df[f"cidade_municipio_vila_localidade_de_{prefix}"] = parsed.apply(lambda x: x.get("admin_area_level_2"))
    df[f"estado_de_{prefix}"] = parsed.apply(lambda x: x.get("admin_area_level_1"))
    df[f"pais_de_{prefix}"] = parsed.apply(lambda x: x.get("country"))
    df[f"cep_codigo_postal_de_{prefix}"] = parsed.apply(lambda x: x.get("postal_code"))
    df[f"nome_da_rua_de_{prefix}"] = parsed.apply(lambda x: x.get("route"))
    df[f"distrito_sub_localidade_de_{prefix}"] = parsed.apply(lambda x: x.get("sublocality"))
    df[f"numero_da_casa_de_{prefix}"] = parsed.apply(lambda x: x.get("street_number"))
    df[f"latitude_de_{prefix}"] = parsed.apply(lambda x: x.get("latitude"))
    df[f"longitude_de_{prefix}"] = parsed.apply(lambda x: x.get("longitude"))

    return df


def explode_currency_field(df: pd.DataFrame, source_column: str, value_col: str, currency_col: str) -> pd.DataFrame:
    def parse_currency(val):
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            return parsed.get("value"), parsed.get("currency")
        except Exception:
            return None, None

    exploded = df[source_column].apply(parse_currency)
    df[value_col] = exploded.apply(lambda x: x[0])
    df[currency_col] = exploded.apply(lambda x: x[1])
    return df
