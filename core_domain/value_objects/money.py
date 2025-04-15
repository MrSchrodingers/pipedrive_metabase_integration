from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

@dataclass(frozen=True)
class Money:
    """Represents a monetary value with amount and currency."""
    amount: Decimal
    currency: str 

    def __post_init__(self):
        if not isinstance(self.amount, Decimal):
            try:
                object.__setattr__(self, 'amount', Decimal(self.amount))
            except (InvalidOperation, TypeError, ValueError) as e:
                raise ValueError(f"Invalid amount for Money: {self.amount}. Must be Decimal compatible.") from e
        if not self.currency or not isinstance(self.currency, str):
            raise ValueError("Currency must be a non-empty string.")

    def __str__(self) -> str:
        return f"{self.amount:.2f} {self.currency}"