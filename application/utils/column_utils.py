from typing import Any, Dict
import unicodedata
import re
import logging

log = logging.getLogger(__name__)

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

ADDRESS_COMPONENT_SUFFIX_MAP = {
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
    # 'formatted_address' mapeado para a coluna principal
}

# Adicione um conjunto de chaves que indicam um campo de endereço
ADDRESS_INDICATOR_KEYS = {'formatted_address', 'locality', 'country', 'postal_code'}

def flatten_custom_fields(custom_fields: Dict[str, Any], repo_custom_mapping: Dict[str, str]) -> Dict[str, Any]:
    """
    Achata os campos personalizados, tratando campos de endereço de forma especial
    para extrair seus componentes em colunas separadas.
    """
    flat_dict = {}

    for api_key, normalized_name in repo_custom_mapping.items():
        field_data = custom_fields.get(api_key)

        if normalized_name not in flat_dict:
            flat_dict[normalized_name] = None

        if isinstance(field_data, dict):
            is_likely_address = any(key in field_data for key in ADDRESS_INDICATOR_KEYS)

            if is_likely_address:
                log.debug(f"Processing field '{normalized_name}' (API Key: {api_key}) as address.")

                # 1. Pega o valor principal 
                main_address_value = field_data.get('formatted_address') or field_data.get('value')
                flat_dict[normalized_name] = main_address_value

                # 2. Extrai os componentes
                for component_key, suffix in ADDRESS_COMPONENT_SUFFIX_MAP.items():
                    subcol_name = f"{normalized_name}_{suffix}"
                    component_value = field_data.get(component_key)
                    flat_dict[subcol_name] = component_value

                expected_address_cols = {f"{normalized_name}_{suffix}" for suffix in ADDRESS_COMPONENT_SUFFIX_MAP.values()}
                for key in flat_dict.keys():
                     if key.startswith(normalized_name + "_") and key not in expected_address_cols:
                          if key not in flat_dict:
                               flat_dict[key] = None


            else:
                log.debug(f"Processing field '{normalized_name}' (API Key: {api_key}) as generic dictionary.")
                flat_dict[normalized_name] = field_data.get('value')

        elif field_data is not None:
            log.debug(f"Processing field '{normalized_name}' (API Key: {api_key}) as simple value.")
            flat_dict[normalized_name] = field_data

    return flat_dict