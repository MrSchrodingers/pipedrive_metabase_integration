from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Set

class DataRepositoryPort(ABC):
    """
    Port for interacting with the main Pipedrive deal data storage.
    """

    @abstractmethod
    def initialize_schema(self) -> None:
        """Ensures the main data table schema exists and is up-to-date."""
        pass

    @abstractmethod
    def save_data_upsert(self, data: List[Dict[str, Any]]) -> None:
        """
        Saves a batch of processed data using an upsert strategy.
        Expects data as a list of dictionaries ready for persistence.
        Handles dynamic schema updates for new columns present in the data.
        """
        pass

    # --- Methods specific to pipedrive_data table ---
    @abstractmethod
    def get_record_by_id(self, record_id: str) -> Optional[Dict]:
        """Fetches a single complete record by its ID."""
        pass

    @abstractmethod
    def get_all_ids(self) -> Set[str]:
        """Returns a set of all existing deal IDs."""
        pass

    @abstractmethod
    def count_records(self) -> int:
        """Counts the total number of records in the main data table."""
        pass

    # --- Methods specific to Stage History Backfill ---
    @abstractmethod
    def get_deals_needing_history_backfill(self, limit: int) -> List[str]:
        """Finds deal IDs potentially needing stage history backfill."""
        pass

    @abstractmethod
    def update_stage_history(self, updates: List[Dict[str, Any]]) -> None:
        """Applies stage history timestamp updates."""
        pass

    @abstractmethod
    def count_deals_needing_backfill(self) -> int:
        """Counts how many deals potentially need backfill."""
        pass