from dataclasses import dataclass
from datetime import datetime, timezone, date

@dataclass(frozen=True)
class Timestamp:
    """Represents a specific point in time, ensuring UTC timezone."""
    value: datetime

    def __post_init__(self):
        if not isinstance(self.value, datetime):
            raise ValueError("Timestamp value must be a datetime object.")
        if self.value.tzinfo is None:
            object.__setattr__(self, 'value', self.value.replace(tzinfo=timezone.utc))
        elif self.value.tzinfo != timezone.utc:
            object.__setattr__(self, 'value', self.value.astimezone(timezone.utc))

    @property
    def date(self) -> date:
        return self.value.date()

    def isoformat(self) -> str:
        return self.value.isoformat()

    def __str__(self) -> str:
        return self.isoformat()