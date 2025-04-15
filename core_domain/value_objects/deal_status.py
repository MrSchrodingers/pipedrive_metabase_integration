from dataclasses import dataclass
from enum import Enum

class DealStatusOption(Enum):
    OPEN = "open"
    WON = "won"
    LOST = "lost"
    DELETED = "deleted"
    UNKNOWN = "unknown" 

    @classmethod
    def from_string(cls, status_str: str | None) -> 'DealStatusOption':
        if status_str is None:
            return cls.UNKNOWN
        for status in cls:
            if status.value == status_str.lower():
                return status
        return cls.UNKNOWN

@dataclass(frozen=True)
class DealStatus:
    """Represents the status of a Deal using an Enum."""
    value: DealStatusOption

    def __init__(self, status_str: str | None):
        object.__setattr__(self, 'value', DealStatusOption.from_string(status_str))

    def __str__(self) -> str:
        return self.value.value

    def is_open(self) -> bool:
        return self.value == DealStatusOption.OPEN

    def is_closed(self) -> bool:
        return self.value in (DealStatusOption.WON, DealStatusOption.LOST)