from typing import Optional, Dict, Any, Union
from pydantic import BaseModel, Field, field_validator, confloat
from datetime import datetime, timezone

class DealSchema(BaseModel, extra='allow'):
    id: int
    title: Optional[str] = None
    creator_user_id: Optional[int] = None 
    user_id: Optional[int] = None
    person_id: Optional[int] = None
    stage_id: Optional[int] = None
    stage_name: Optional[str] = None
    pipeline_id: Optional[int] = None
    pipeline_name: Optional[str] = None 
    status: Optional[str] = None
    value: Optional[float] = 0.0
    currency: Optional[str] = 'USD'
    add_time: Optional[datetime] = None
    update_time: Optional[datetime] = None
    custom_fields: Dict[str, Any] = Field({}, alias='custom_fields')

    class Config:
        populate_by_name  = True
        alias_generator = lambda x: x 

    @field_validator('creator_user_id', 'user_id', 'person_id', mode='before')
    def extract_id_from_dict(cls, value):
        if isinstance(value, dict):
            return value.get("id")
        return value
    
    @field_validator('add_time', 'update_time')
    def parse_datetime_optional(cls, value):
        if value is None:
            return None
        if isinstance(value, datetime):
            return value.replace(tzinfo=timezone.utc) if value.tzinfo is None else value
        try:
            dt = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
            return dt.astimezone(timezone.utc)
        except (ValueError, TypeError):
            print(f"Warning: Could not parse date '{value}'. Setting to None.")
            return None

    @field_validator('value')
    def value_to_float(cls, value):
        if value is None:
            return 0.0
        try:
            return float(value)
        except (ValueError, TypeError):
            print(f"Warning: Could not convert value '{value}' to float. Setting to 0.0.")
            return 0.0