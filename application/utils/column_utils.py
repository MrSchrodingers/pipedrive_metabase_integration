import time
from typing import Any, Dict, List, Optional
import unicodedata
import re
import pandas as pd

from application.utils.data_processing_utils import normalize_address

def normalize_column_name(name: str) -> str:
    if not isinstance(name, str):
        return ""
    name = name.lower()
    try:
        name = unicodedata.normalize('NFKD', name).encode('ASCII', 'ignore').decode('ASCII')
    except Exception:
        pass
    name = re.sub(r'[^\w_]+', '_', name)
    name = re.sub(r'_+', '_', name)
    name = name.strip('_')
    if name and name[0].isdigit():
        name = '_' + name
    return name or "_invalid_normalized_name"

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

def apply_address_normalization_to_columns(
    df: pd.DataFrame,
    address_columns: List[str],
    prefix_map: Optional[Dict[str, str]] = None,
    delay: float = 1.0
) -> pd.DataFrame:
    def throttled_normalize_address(addr: Any):
        result = normalize_address(str(addr)) if pd.notna(addr) else None
        time.sleep(delay)
        return result

    def extract_component_safe(x: Optional[Dict[str, Any]], comp_type: str) -> Optional[str]:
        if not x or 'components' not in x:
            return None
        return x['components']['address'].get(comp_type) if x['components'].get('address') else None

    for col in address_columns:
        prefix = prefix_map[col] if prefix_map and col in prefix_map else col
        parsed_addresses = df[col].apply(throttled_normalize_address)

        df[f"{prefix}_latitude"] = parsed_addresses.apply(lambda x: x.get('latitude') if x else None)
        df[f"{prefix}_longitude"] = parsed_addresses.apply(lambda x: x.get('longitude') if x else None)
        df[f"{prefix}_completo"] = parsed_addresses.apply(lambda x: x.get('full_address') if x else None)

        df[f"{prefix}_cidade"] = parsed_addresses.apply(
            lambda x: extract_component_safe(x, 'city') or extract_component_safe(x, 'town')
        )
        df[f"{prefix}_bairro"] = parsed_addresses.apply(lambda x: extract_component_safe(x, 'suburb'))
        df[f"{prefix}_estado"] = parsed_addresses.apply(lambda x: extract_component_safe(x, 'state'))
        df[f"{prefix}_pais"] = parsed_addresses.apply(lambda x: extract_component_safe(x, 'country'))
        df[f"{prefix}_cep"] = parsed_addresses.apply(lambda x: extract_component_safe(x, 'postcode'))
        df[f"{prefix}_rua"] = parsed_addresses.apply(lambda x: extract_component_safe(x, 'road'))
        df[f"{prefix}_numero"] = parsed_addresses.apply(lambda x: extract_component_safe(x, 'house_number'))

    return df
