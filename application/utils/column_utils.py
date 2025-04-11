import unicodedata
import re
import pandas as pd
import json
import numpy as np

import logging 

log = logging.getLogger(__name__) 

def parse_json_robust(val):
    """Tenta parsear um valor como JSON, retornando um dict vazio em caso de falha."""
    if pd.isna(val):
        return {}
    if isinstance(val, dict):
        return val 
    if isinstance(val, str):
        try:
            data = json.loads(val)
            return data if isinstance(data, dict) else {}
        except json.JSONDecodeError:
            return {}
    return {} 

def force_explode_address_field_optimized(df: pd.DataFrame, source_column: str, prefix: str) -> pd.DataFrame:
    """
    Explode campos de endereço de uma coluna JSON usando pd.json_normalize.
    Sobrescreve ou adiciona as colunas de destino no DataFrame original.
    """
    if source_column not in df.columns:
        log.warning(f"Coluna fonte '{source_column}' não encontrada para explosão de endereço.")
        return df

    # 1. Parsear JSON de forma robusta para uma Series de dicionários
    parsed_series = pd.Series([parse_json_robust(x) for x in df[source_column]], index=df.index)

    # 2. Usar json_normalize
    try:
        normalized_df = pd.json_normalize(parsed_series)
        normalized_df.index = df.index
    except Exception as e:
        log.error(f"Erro durante pd.json_normalize para '{source_column}': {e}", exc_info=True)
        return df

    # 3. Mapear nomes de chaves JSON para nomes de colunas finais
    field_map = {
        # Chave no JSON : Nome da coluna final
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

    # 4. Renomear e selecionar colunas relevantes do DataFrame normalizado
    rename_dict = {k: v for k, v in field_map.items() if k in normalized_df.columns}
    target_columns_present = list(rename_dict.values())
    exploded_data = normalized_df.rename(columns=rename_dict)[target_columns_present]

    # 5. Garantir que todas as colunas de destino esperadas existam
    all_target_cols = list(field_map.values())
    for target_col in all_target_cols:
        if target_col not in exploded_data.columns:
            exploded_data[target_col] = np.nan 

    # 6. Atualizar o DataFrame original com os dados explodidos
    for col in [f"latitude_de_{prefix}", f"longitude_de_{prefix}"]:
         if col in exploded_data.columns:
              exploded_data[col] = pd.to_numeric(exploded_data[col], errors='coerce')
    num_casa_col = f"numero_da_casa_de_{prefix}"
    if num_casa_col in exploded_data.columns:
        exploded_data[num_casa_col] = exploded_data[num_casa_col].astype(str).replace('nan', None)

    df.update(exploded_data)
    log.debug(f"Explosão de endereço concluída para '{source_column}'. Colunas atualizadas: {list(exploded_data.columns)}")

    return df


def force_explode_currency_field_optimized(
    df: pd.DataFrame,
    source_column: str,
    value_col: str,
    currency_col: str
) -> pd.DataFrame:
    """
    Explode campos de moeda ('value', 'currency') de uma coluna JSON.
    Sobrescreve ou adiciona as colunas de destino no DataFrame original.
    """
    if source_column not in df.columns:
        log.warning(f"Coluna fonte '{source_column}' não encontrada para explosão de moeda.")
        return df

    # 1. Parsear JSON robustamente
    parsed_series = pd.Series([parse_json_robust(x) for x in df[source_column]], index=df.index)

    # 2. Extrair 'value' e 'currency' diretamente
    values = parsed_series.apply(lambda d: d.get('value', np.nan))
    currencies = parsed_series.apply(lambda d: d.get('currency', None)) 

    # 3. Criar um DataFrame temporário com os dados extraídos
    exploded_data = pd.DataFrame({
        value_col: pd.to_numeric(values, errors='coerce'), 
        currency_col: currencies.astype(str).replace('nan', None).replace('None', None)
    }, index=df.index)

    # 4. Atualizar o DataFrame original
    df.update(exploded_data)
    log.debug(f"Explosão de moeda concluída para '{source_column}'. Colunas atualizadas: {[value_col, currency_col]}")

    return df

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
