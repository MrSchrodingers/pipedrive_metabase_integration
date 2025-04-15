import json
from decimal import Decimal, InvalidOperation
from datetime import datetime, date, time
from typing import Any, Dict, Optional, Tuple

import structlog

from application.schemas.deal_schema import DealSchema
from core_domain.entities.deal import Deal
from core_domain.value_objects.identifiers import (
    DealId, UserId, PersonId, OrgId, StageId, PipelineId
)
from core_domain.value_objects.money import Money
from core_domain.value_objects.timestamp import Timestamp
from core_domain.value_objects.deal_status import DealStatus, DealStatusOption
from core_domain.value_objects.custom_field import CustomFieldKey, CustomFieldValue

from application.utils.column_utils import ADDRESS_COMPONENT_SUFFIX_MAP


log = structlog.get_logger(__name__)


# --- Helper para processar valores de campos customizados ---
def _process_custom_field_value(
    raw_value: Any,
    field_type: Optional[str]
) -> Tuple[Any, Optional[str]]:
    """
    Processa o valor bruto de um campo customizado com base no tipo Pipedrive.
    Retorna o valor processado (tipo Python padrão) e o tipo Pipedrive.
    """
    processed_value = raw_value
    pipedrive_type_str = field_type if field_type else "unknown"

    if raw_value is None:
        return None, pipedrive_type_str

    try:
        if field_type in ('varchar', 'text', 'enum', 'phone', 'set', 'user', 'person', 'org', 'pipeline'):
             if isinstance(raw_value, dict) and 'id' in raw_value:
                 processed_value = int(raw_value['id'])
             elif isinstance(raw_value, dict) and 'value' in raw_value:
                 processed_value = raw_value['value']
                 if isinstance(processed_value, int):
                     processed_value = int(processed_value)
                 else:
                    processed_value = str(processed_value) if processed_value is not None else None
             elif isinstance(raw_value, (int, float)): 
                 processed_value = int(raw_value)
             else:
                 processed_value = str(raw_value)

        elif field_type in ('double', 'int'):
            processed_value = float(raw_value) if '.' in str(raw_value) else int(raw_value)
        elif field_type == 'monetary':
            if isinstance(raw_value, dict):
                amount = raw_value.get('value')
                processed_value = Decimal(amount) if amount is not None else Decimal(0)
            else:
                processed_value = Decimal(raw_value)
        elif field_type == 'date':
            processed_value = datetime.fromisoformat(str(raw_value)).date()
        elif field_type == 'time':
             processed_value = datetime.strptime(str(raw_value), '%H:%M:%S').time()
        elif field_type == 'datetime':
             dt_vo = Timestamp(datetime.fromisoformat(str(raw_value).replace('Z', '+00:00')))
             processed_value = dt_vo.value
        elif field_type == 'address':
            if isinstance(raw_value, str): 
                try:
                    processed_value = json.loads(raw_value)
                except json.JSONDecodeError:
                    log.warning("Could not decode address JSON string", raw_value=raw_value)
                    processed_value = {"formatted_address": str(raw_value)}
            elif not isinstance(raw_value, dict):
                 processed_value = {"formatted_address": str(raw_value)}
            else:
                 processed_value = raw_value 
        else:
            processed_value = str(raw_value)

    except (ValueError, TypeError, InvalidOperation) as e:
        log.warning(
            "Failed to process custom field value based on type",
            raw_value=raw_value, field_type=field_type, error=str(e)
        )
        processed_value = str(raw_value) if raw_value is not None else None

    return processed_value, pipedrive_type_str


# --- Mapeamento API Dict -> Schema ---
def map_api_dict_to_schema(api_data: dict) -> DealSchema:
    """
    Valida e converte um dicionário da API Pipedrive para um DealSchema Pydantic.
    Levanta ValidationError em caso de falha.
    """
    try:
        return DealSchema.model_validate(api_data)
    except Exception as e:
        log.error("Pydantic validation failed for DealSchema", data_preview=str(api_data)[:200], error=str(e))
        raise


# --- Mapeamento Schema -> Domain ---
def map_schema_to_domain(
    schema: DealSchema,
    custom_field_map: Dict[str, str], # { api_hash: normalized_name }
    field_definitions: Dict[str, Dict] # { api_hash: { "key": "...", "field_type": "..." } }
) -> Deal:
    """
    Converte um DealSchema validado para a entidade Deal do domínio.
    """
    domain_custom_fields: Dict[CustomFieldKey, CustomFieldValue] = {}
    if schema.custom_fields:
        for api_hash_key, raw_value in schema.custom_fields.items():
            normalized_name = custom_field_map.get(api_hash_key)
            if not normalized_name:
                log.debug("Skipping custom field with unknown mapping", api_key=api_hash_key)
                continue

            field_def = field_definitions.get(api_hash_key, {})
            field_type = field_def.get("field_type")

            processed_value, pipedrive_type_str = _process_custom_field_value(raw_value, field_type)

            key_vo = CustomFieldKey(normalized_name)
            value_vo = CustomFieldValue(value=processed_value, pipedrive_type=pipedrive_type_str)
            domain_custom_fields[key_vo] = value_vo

    creator_user_id_val = schema.creator_user_id
    owner_id_val = schema.owner_id.get('id') if isinstance(schema.owner_id, dict) else schema.owner_id
    person_id_val = schema.person_id
    org_id_val = schema.org_id.get('value') if isinstance(schema.org_id, dict) else schema.org_id

    try:
        deal_entity = Deal(
            deal_id=DealId(schema.id),
            title=str(schema.title or ""),
            status=DealStatus(schema.status),
            value=Money(amount=Decimal(schema.value) if schema.value is not None else Decimal('0.0'),
                        currency=str(schema.currency or "BRL")),
            creator_user_id=UserId(int(creator_user_id_val)) if creator_user_id_val else None,
            owner_id=UserId(int(owner_id_val)) if owner_id_val else None, 
            pipeline_id=PipelineId(int(schema.pipeline_id)) if schema.pipeline_id else None, 
            stage_id=StageId(int(schema.stage_id)) if schema.stage_id else None, 
            # Datas/Horas - Schema já deve ter validado para datetime
            add_time=Timestamp(schema.add_time) if schema.add_time else None, 
            update_time=Timestamp(schema.update_time) if schema.update_time else None, 
            # Opcionais
            person_id=PersonId(int(person_id_val)) if person_id_val else None,
            org_id=OrgId(int(org_id_val)) if org_id_val else None,
            probability=Decimal(schema.probability) if schema.probability is not None else None,
            lost_reason=schema.lost_reason,
            visible_to=str(schema.visible_to) if schema.visible_to is not None else None,
            close_time=Timestamp(schema.close_time) if schema.close_time else None,
            won_time=Timestamp(schema.won_time) if schema.won_time else None,
            lost_time=Timestamp(schema.lost_time) if schema.lost_time else None,
            expected_close_date=schema.expected_close_date.date() if isinstance(schema.expected_close_date, datetime) \
                                else (datetime.strptime(schema.expected_close_date, '%Y-%m-%d').date() if isinstance(schema.expected_close_date, str) else None), # Ajuste para pegar só date
            custom_fields=domain_custom_fields
        )

        # Validações pós-construção
        if not deal_entity.creator_user_id or not deal_entity.owner_id or \
           not deal_entity.pipeline_id or not deal_entity.stage_id or \
           not deal_entity.add_time or not deal_entity.update_time:
            log.warning("Deal entity created with missing core relationship IDs or timestamps", deal_id=deal_entity.id)

        return deal_entity

    except Exception as domain_build_err:
        log.error("Failed to build Domain Deal entity from Schema",
                  schema_id=schema.id, error=str(domain_build_err), exc_info=True)
        raise ValueError(f"Could not create domain entity for deal {schema.id}") from domain_build_err


# --- Mapeamento Domain -> Persistence Dict ---
def map_domain_to_persistence_dict(deal: Deal) -> Dict[str, Any]:
    """
    Converte a entidade Deal do domínio para um dicionário achatado
    adequado para persistência no banco de dados.
    """
    persistence_dict = {
        "id": str(deal.id.value),
        "titulo": deal.title,
        "status": deal.status.value.value,
        "value": deal.value.amount,
        "currency": deal.value.currency,
        "add_time": deal.add_time.value if deal.add_time else None,
        "update_time": deal.update_time.value if deal.update_time else None,
        "close_time": deal.close_time.value if deal.close_time else None,
        "won_time": deal.won_time.value if deal.won_time else None,
        "lost_time": deal.lost_time.value if deal.lost_time else None,
        "expected_close_date": deal.expected_close_date,
        "creator_user_id": deal.creator_user_id.value if deal.creator_user_id else None,
        "owner_id": deal.owner_id.value if deal.owner_id else None,
        "person_id": deal.person_id.value if deal.person_id else None,
        "org_id": deal.org_id.value if deal.org_id else None,
        "stage_id": deal.stage_id.value if deal.stage_id else None,
        "pipeline_id": deal.pipeline_id.value if deal.pipeline_id else None,
        "probability": deal.probability, 
        "lost_reason": deal.lost_reason,
        "visible_to": deal.visible_to,
        # Campos de nomes (serão preenchidos no Repository/ETLService antes de salvar, não estão no domain)
        "creator_user_name": None,
        "person_name": None,
        "stage_name": None,
        "pipeline_name": None,
        "owner_name": None,
        "org_name": None,
         # Label (campo padrão não mapeado no Domain Entity inicial)
         "label": None 
    }

    # Processar campos customizados
    for key_vo, value_vo in deal.custom_fields.items():
        normalized_key = key_vo.value
        value_to_persist = value_vo.value

        # Lógica especial para endereços (achatamento)
        if value_vo.pipedrive_type == 'address' and isinstance(value_to_persist, dict):
            addr_dict = value_to_persist
            # Campo principal recebe o formatted_address
            persistence_dict[normalized_key] = addr_dict.get('formatted_address')

            # Campos de componentes (ex: local_do_acidente_numero_da_casa)
            for component_key, suffix in ADDRESS_COMPONENT_SUFFIX_MAP.items():
                subcol_name = f"{normalized_key}_{suffix}"
                persistence_dict[subcol_name] = addr_dict.get(component_key)
        else:
             if isinstance(value_to_persist, (set, tuple)):
                 persistence_dict[normalized_key] = list(value_to_persist)
             elif isinstance(value_to_persist, (time, date, datetime, Decimal)):
                 persistence_dict[normalized_key] = value_to_persist
             elif isinstance(value_to_persist, dict):
                  persistence_dict[normalized_key] = json.dumps(value_to_persist)
             else:
                 persistence_dict[normalized_key] = value_to_persist 

    # Remover chaves com valor None explicitamente se o DB não gostar?
    # persistence_dict = {k: v for k, v in persistence_dict.items() if v is not None}
    # Decisão: Manter None, psycopg2/COPY lida bem com isso ('\N').

    return persistence_dict