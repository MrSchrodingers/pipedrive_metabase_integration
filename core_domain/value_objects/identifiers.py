from dataclasses import dataclass

@dataclass(frozen=True)
class BaseId:
    """Base class for numeric identifiers. Ensures value is positive."""
    value: int

    def __post_init__(self):
        if self.value <= 0:
            raise ValueError(f"{self.__class__.__name__} value must be positive.")

    def __int__(self) -> int:
        return self.value

    def __str__(self) -> str:
        return str(self.value)

class DealId(BaseId): pass
class UserId(BaseId): pass
class PersonId(BaseId): pass
class OrgId(BaseId): pass 
class StageId(BaseId): pass
class PipelineId(BaseId): pass
