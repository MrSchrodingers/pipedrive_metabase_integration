# application/schemas/deal_schema.py
from typing import List, Optional, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator, confloat
from datetime import date, datetime, timezone
import structlog # Importa\u00e7\u00e3o adicionada para o logger no validador

log = structlog.get_logger(__name__) # Logger para mensagens de aviso

class DealSchema(BaseModel, extra='allow'):
 id: int
 title: Optional[str] = None
 creator_user_id: Optional[int] = None
 person_id: Optional[int] = None
 stage_id: Optional[int] = None
 stage_name: Optional[str] = None 
 pipeline_id: Optional[int] = None
 pipeline_name: Optional[str] = None 
 status: Optional[str] = None
 value: Optional[float] = 0.0
 currency: Optional[str] = 'BRL'
 add_time: Optional[datetime] = None
 update_time: Optional[datetime] = None
 close_time: Optional[datetime] = None 
 won_time: Optional[datetime] = None 
 lost_time: Optional[datetime] = None 
 expected_close_date: Optional[datetime | str] = None

 owner_id: Optional[Union[int, Dict[str, Any]]] = None
 org_id: Optional[Union[int, Dict[str, Any]]] = None 
 probability: Optional[float] = None
 lost_reason: Optional[str] = None
 visible_to: Optional[int] = None
 label_ids: List[int] = Field(default_factory=list)
 is_deleted: bool = False

 # Campos customizados
 custom_fields: Dict[str, Any] = Field({}, alias='custom_fields')

 class Config:
  populate_by_name = True 
  alias_generator = lambda x: x

 @field_validator('creator_user_id', 'person_id', 'stage_id', 'pipeline_id', 'owner_id', 'org_id', mode='before')
 def extract_id_or_value_from_dict(cls, v):
  if isinstance(v, dict):
   if 'id' in v:
    return v.get('id')
   if 'value' in v:
    return v.get('value')
  return v 

 @field_validator('add_time', 'update_time', 'close_time', 'won_time', 'lost_time', mode='before')
 def parse_datetime_optional(cls, value):
  if value is None or value == '':
   return None
  if isinstance(value, datetime):
   return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value.astimezone(timezone.utc)
  try:
   dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
   return dt.astimezone(timezone.utc)
  except (ValueError, TypeError):
   try:
    dt = datetime.strptime(str(value), '%Y-%m-%d %H:%M:%S')
    return dt.replace(tzinfo=timezone.utc) 
   except (ValueError, TypeError):
    return None

 @field_validator('expected_close_date', mode='before')
 def parse_date_optional(cls, value):
  if value is None or value == '':
   return None
  if isinstance(value, datetime):
   return value.date()
  if isinstance(value, date): 
   return value
  try:
   dt = datetime.strptime(str(value), '%Y-%m-%d')
   return dt.date()
  except (ValueError, TypeError):
   return None

 @field_validator('value')
 def value_to_float(cls, value):
  if value is None:
   return 0.0
  try:
   return float(value)
  except (ValueError, TypeError):
   return 0.0
