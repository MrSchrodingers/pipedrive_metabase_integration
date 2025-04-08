from abc import ABC, abstractmethod
from typing import Dict, List, Generator

class PipedriveClientPort(ABC):
    @abstractmethod
    def fetch_deal_fields_mapping(self) -> Dict[str, str]:
        """Fetches the mapping of custom field API keys to normalized names."""
        pass

    @abstractmethod
    def fetch_all_deals_stream(self, updated_since: str = None, items_limit: int = None) -> Generator[List[Dict], None, None]:
        """
        Fetches deals from Pipedrive in batches as a generator.
        Yields lists (batches) of deal dictionaries.
        """
        pass

    @abstractmethod
    def update_last_timestamp(self, new_timestamp: str) -> None:
        """Updates the timestamp for the next incremental fetch."""
        pass

    @abstractmethod
    def get_last_timestamp(self) -> str | None:
        """Gets the last stored timestamp."""
        pass