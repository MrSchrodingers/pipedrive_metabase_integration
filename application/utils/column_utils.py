import time
from typing import Any, Dict
import unicodedata
import re
import pandas as pd

from application.utils.data_processing_utils import normalize_address

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
    ADDRESS_FIELDS = {
        'formatted_address': '',
        'street_number': 'numero_da_casa',
        'route': 'nome_da_rua',
        'sublocality': 'distrito_sub_localidade',
        'locality': 'cidade_municipio_vila_localidade',
        'admin_area_level_1': 'estado',
        'admin_area_level_2': 'regiao',
        'country': 'pais',
        'postal_code': 'cep_codigo_postal',
        'latitude': 'latitude',
        'longitude': 'longitude',
    }

    for api_key, normalized_name in repo_custom_mapping.items():
        field_data = custom_fields.get(api_key)

        if field_data is None:
            flat_dict[normalized_name] = None
            continue

        if isinstance(field_data, dict):
            if 'formatted_address' in field_data:
                flat_dict[normalized_name] = field_data.get('formatted_address')

                for address_key, address_suffix in ADDRESS_FIELDS.items():
                    column_name = f"{normalized_name}_{address_suffix}" if address_suffix else normalized_name
                    flat_dict[column_name] = field_data.get(address_key)

            else:
                flattened_subfields = pd.json_normalize(field_data, sep='_').to_dict(orient='records')[0]
                for sub_key, sub_val in flattened_subfields.items():
                    flat_dict[f"{normalized_name}_{sub_key}"] = sub_val
        else:
            flat_dict[normalized_name] = field_data

    return flat_dict

def robust_address_parsing(df: pd.DataFrame, address_col: str, prefix: str) -> pd.DataFrame:
    def throttled_normalize_address(addr):
        result = normalize_address(addr)
        time.sleep(1)  
        return result
    
    parsed_addresses = df[address_col].apply(throttled_normalize_address)

    df[f"{prefix}_latitude"] = parsed_addresses.apply(lambda x: x.get('latitude') if x else None)
    df[f"{prefix}_longitude"] = parsed_addresses.apply(lambda x: x.get('longitude') if x else None)
    df[f"{prefix}_completo"] = parsed_addresses.apply(lambda x: x.get('full_address') if x else None)
    
    def extract_component(components, comp_type):
        if not components or 'address' not in components:
            return None
        return components['address'].get(comp_type)
    
    df[f"{prefix}_cidade"] = parsed_addresses.apply(lambda x: extract_component(x['components'], 'city') or extract_component(x['components'], 'town'))
    df[f"{prefix}_bairro"] = parsed_addresses.apply(lambda x: extract_component(x['components'], 'suburb'))
    df[f"{prefix}_estado"] = parsed_addresses.apply(lambda x: extract_component(x['components'], 'state'))
    df[f"{prefix}_pais"] = parsed_addresses.apply(lambda x: extract_component(x['components'], 'country'))
    df[f"{prefix}_cep"] = parsed_addresses.apply(lambda x: extract_component(x['components'], 'postcode'))
    df[f"{prefix}_rua"] = parsed_addresses.apply(lambda x: extract_component(x['components'], 'road'))
    df[f"{prefix}_numero"] = parsed_addresses.apply(lambda x: extract_component(x['components'], 'house_number'))

    return df

