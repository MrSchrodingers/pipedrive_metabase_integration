from decimal import Decimal, InvalidOperation
from time import timezone
from typing import Optional, Dict, List 
from datetime import date, datetime, timedelta

from core_domain.value_objects.identifiers import (
    DealId, UserId, PersonId, OrgId, StageId, PipelineId
)
from core_domain.value_objects.money import Money
from core_domain.value_objects.timestamp import Timestamp
from core_domain.value_objects.deal_status import DealStatus
from core_domain.value_objects.custom_field import CustomFieldKey, CustomFieldValue

class Deal:
    """
    Represents a Deal in the Pipedrive domain.
    Holds the core attributes and custom fields.
    Entity identity is defined by `id`.
    """
    def __init__(
        self,
        deal_id: DealId,
        title: str,
        status: DealStatus,
        value: Money,
        creator_user_id: UserId,
        owner_id: UserId,
        pipeline_id: PipelineId,
        stage_id: StageId,
        add_time: Timestamp,
        update_time: Timestamp,
        person_id: Optional[PersonId] = None,
        org_id: Optional[OrgId] = None,
        probability: Optional[Decimal] = None, 
        lost_reason: Optional[str] = None,
        visible_to: Optional[str] = None, 
        close_time: Optional[Timestamp] = None,
        won_time: Optional[Timestamp] = None,
        lost_time: Optional[Timestamp] = None,
        expected_close_date: Optional[date] = None, 
        custom_fields: Optional[Dict[CustomFieldKey, CustomFieldValue]] = None,
    ):
        # --- Core Attributes & Identity ---
        self.id: DealId = deal_id
        self.title: str = title if title is not None else ""
        self.status: DealStatus = status
        self.value: Money = value

        # --- Timestamps ---
        self.add_time: Timestamp = add_time
        self.update_time: Timestamp = update_time
        self.close_time: Optional[Timestamp] = close_time
        self.won_time: Optional[Timestamp] = won_time
        self.lost_time: Optional[Timestamp] = lost_time
        self.expected_close_date: Optional[date] = expected_close_date

        # --- Relationships (IDs) ---
        self.creator_user_id: UserId = creator_user_id
        self.owner_id: UserId = owner_id
        self.person_id: Optional[PersonId] = person_id
        self.org_id: Optional[OrgId] = org_id
        self.pipeline_id: PipelineId = pipeline_id
        self.stage_id: StageId = stage_id

        # --- Other Attributes ---
        self.probability: Optional[Decimal] = self._validate_probability(probability)
        self.lost_reason: Optional[str] = lost_reason
        self.visible_to: Optional[str] = visible_to

        # --- Custom Fields ---
        self.custom_fields: Dict[CustomFieldKey, CustomFieldValue] = custom_fields if custom_fields is not None else {}

    def _validate_probability(self, probability: Optional[float | Decimal]) -> Optional[Decimal]:
        """ Ensures probability is a Decimal between 0 and 100, or None. """
        if probability is None:
            return None
        try:
            prob_decimal = Decimal(probability)
            if not (Decimal(0) <= prob_decimal <= Decimal(100)):
                 print(f"Warning: Probability {prob_decimal} out of range [0, 100] for Deal {self.id}")
                 pass 
            return prob_decimal
        except (InvalidOperation, TypeError, ValueError):
             print(f"Warning: Could not convert probability '{probability}' to Decimal for Deal {self.id}")
             return None 

    # --- Potential Business Logic Methods ---
    def is_likely_to_close(self) -> bool:
        """ Example business rule based on probability """
        return self.probability is not None and self.probability > 75

    def time_in_pipeline(self, current_time: Optional[datetime] = None) -> timedelta:
        """ Calculates how long the deal has been open. """
        if self.status.is_closed():
            effective_end_time = self.close_time or self.update_time
            return effective_end_time.value - self.add_time.value
        else:
            now = Timestamp(current_time or datetime.now(timezone.utc))
            return now.value - self.add_time.value

    # --- Equality and Representation ---
    def __eq__(self, other):
        """Entities are equal if their IDs are equal."""
        if isinstance(other, Deal):
            return self.id == other.id
        return False

    def __hash__(self):
        """Entities are hashable based on their ID."""
        return hash(self.id)

    def __repr__(self):
        return f"<Deal(id={self.id}, title='{self.title[:30]}...', status={self.status})>"