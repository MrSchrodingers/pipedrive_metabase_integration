import unicodedata
import re
import pandas as pd
import json

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

def safe_explode_address_field(
    df: pd.DataFrame,
    source_column: str,
    prefix: str
) -> pd.DataFrame:
    """
    Explode os campos de endereço a partir do JSON na coluna source_column,
    mas somente preenche/atualiza as colunas de destino se estas não existirem
    ou se estiverem nulas. Caso contrário, mantém o valor que já estava lá.
    """

    if source_column not in df.columns:
        return df

    def parse_json_safe(val):
        try:
            return json.loads(val) if isinstance(val, str) else (val if isinstance(val, dict) else {})
        except Exception:
            return {}

    parsed = df[source_column].apply(parse_json_safe)

    field_map = {
        "formatted_address": f"endereco_completo_combinado_de_{prefix}",
        "admin_area_level_2": f"cidade_municipio_vila_localidade_de_{prefix}",
        "admin_area_level_1": f"estado_de_{prefix}",
        "country": f"pais_de_{prefix}",
        "postal_code": f"cep_codigo_postal_de_{prefix}",
        "route": f"nome_da_rua_de_{prefix}",
        "sublocality": f"distrito_sub_localidade_de_{prefix}",
        "street_number": f"numero_da_casa_de_{prefix}",
        "latitude": f"latitude_de_{prefix}",
        "longitude": f"longitude_de_{prefix}",
    }

    for json_key, target_col in field_map.items():
        if target_col not in df.columns:
            df[target_col] = None

        def fill_fallback(row):
            if pd.isna(row[target_col]) or row[target_col] == "":
                return row["json_parsed"].get(json_key, None)
            else:
                return row[target_col]

        df = df.assign(json_parsed=parsed) 
        df[target_col] = df.apply(fill_fallback, axis=1)

    if "json_parsed" in df.columns:
        df.drop(columns=["json_parsed"], inplace=True)

    return df

def safe_explode_currency_field(
    df: pd.DataFrame,
    source_column: str,
    value_col: str,
    currency_col: str
) -> pd.DataFrame:
    """
    Explode campos de moeda a partir de um JSON contido em source_column,
    preenchendo somente se as colunas de destino estiverem ausentes ou nulas.
    """

    if source_column not in df.columns:
        return df

    def parse_currency(val):
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            return parsed.get("value"), parsed.get("currency")
        except Exception:
            return (None, None)

    exploded = df[source_column].apply(parse_currency)

    if value_col not in df.columns:
        df[value_col] = None
    if currency_col not in df.columns:
        df[currency_col] = None

    def fill_fallback(row):
        current_value = row[value_col]
        current_currency = row[currency_col]
        new_value, new_currency = row["temp_parsed"]

        final_value = current_value if not pd.isna(current_value) else new_value
        final_currency = current_currency if not pd.isna(current_currency) else new_currency
        return final_value, final_currency

    df = df.assign(temp_parsed=exploded)
    df[[value_col, currency_col]] = df.apply(
        lambda row: fill_fallback(row),
        axis=1,
        result_type="expand"
    )
    df.drop(columns=["temp_parsed"], inplace=True)

    return df

def force_explode_address_field(
    df: pd.DataFrame,
    source_column: str,
    prefix: str
) -> pd.DataFrame:
    """
    Explode os campos de endereço a partir do JSON na coluna `source_column`,
    sobrescrevendo sempre os valores das colunas de destino
    (i.e. não mantém valores preexistentes).
    """

    if source_column not in df.columns:
        return df

    def parse_json_safe(val):
        try:
            return json.loads(val) if isinstance(val, str) else (val if isinstance(val, dict) else {})
        except Exception:
            return {}

    parsed = df[source_column].apply(parse_json_safe)

    field_map = {
        "formatted_address": f"endereco_completo_combinado_de_{prefix}",
        "admin_area_level_2": f"cidade_municipio_vila_localidade_de_{prefix}",
        "admin_area_level_1": f"estado_de_{prefix}",
        "country": f"pais_de_{prefix}",
        "postal_code": f"cep_codigo_postal_de_{prefix}",
        "route": f"nome_da_rua_de_{prefix}",
        "sublocality": f"distrito_sub_localidade_de_{prefix}",
        "street_number": f"numero_da_casa_de_{prefix}",
        "latitude": f"latitude_de_{prefix}",
        "longitude": f"longitude_de_{prefix}",
    }

    for json_key, target_col in field_map.items():
        if target_col not in df.columns:
            df[target_col] = None

        df[target_col] = parsed.apply(lambda x: x.get(json_key))

    return df

def force_explode_currency_field(
    df: pd.DataFrame,
    source_column: str,
    value_col: str,
    currency_col: str
) -> pd.DataFrame:
    """
    Explode campos de moeda a partir de um JSON contido em `source_column`,
    sobrescrevendo sempre os valores das colunas de destino.
    """
    if source_column not in df.columns:
        return df

    def parse_currency(val):
        try:
            parsed = json.loads(val) if isinstance(val, str) else val
            return parsed.get("value"), parsed.get("currency")
        except Exception:
            return (None, None)

    exploded = df[source_column].apply(parse_currency)

    if value_col not in df.columns:
        df[value_col] = None
    if currency_col not in df.columns:
        df[currency_col] = None

    df[value_col] = exploded.apply(lambda x: x[0])
    df[currency_col] = exploded.apply(lambda x: x[1])

    return df
