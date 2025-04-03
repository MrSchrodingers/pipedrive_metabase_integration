import json
from datetime import datetime
from decimal import Decimal
from decimal import Decimal
import re

def normalize_currency(value_str: str) -> float:
    if not value_str:
        return 0.0
    try:
        cleaned = value_str.replace("R$", "").replace(",", "").strip()
        return float(cleaned)
    except Exception:
        return 0.0

def normalize_date(date_str: str) -> str:
    if not date_str:
        return None
    try:
        dt = datetime.fromisoformat(date_str.replace(" ", "T"))
        return dt.isoformat()
    except Exception:
        return date_str

class CustomJSONEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj) 
        return super().default(obj)
