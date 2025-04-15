from dataclasses import dataclass
from typing import Any

@dataclass(frozen=True)
class CustomFieldKey:
    """Represents the *normalized* key of a custom field."""
    value: str

    def __post_init__(self):
        if not self.value or not isinstance(self.value, str):
            raise ValueError("CustomFieldKey value must be a non-empty string.")

    def __str__(self) -> str:
        return self.value

@dataclass(frozen=True)
class CustomFieldValue:
    """Represents the value of a custom field, optionally storing its original type."""
    value: Any
    pipedrive_type: str | None = None

    def __str__(self) -> str:
         if isinstance(self.value, dict):
             return str(self.value.get('formatted_address', self.value))
         return str(self.value)