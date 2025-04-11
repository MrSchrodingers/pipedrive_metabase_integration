import time
from typing import Any, Dict, List, Optional
import unicodedata
import re
import pandas as pd

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
            flat_dict[normalized_name] = field_data.get('formatted_address') or field_data.get('value')
            for address_key, address_suffix in ADDRESS_FIELDS.items():
                column_name = f"{normalized_name}_{address_suffix}"
                flat_dict[column_name] = field_data.get(address_key)

    return flat_dict