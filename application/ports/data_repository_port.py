from abc import ABC, abstractmethod
from typing import List, Dict

class DataRepositoryPort(ABC):
    @abstractmethod
    def save_data(self, data: List[Dict]) -> None:
        """
        Implements the logic to save transformed data to the repository.
        (Potentially delegates to save_data_upsert or another method).
        """
        pass

    @abstractmethod
    def save_data_upsert(self, data: List[Dict]) -> None:
        """
        Implements the logic to save transformed data using an upsert strategy.
        This should merge new records with existing ones based on a primary key.
        """
        pass

    @abstractmethod
    def filter_data_by_ids(self, data: List[Dict], id_key: str = "id") -> List[Dict]:
        """
        Filters a list of dictionaries, returning only those whose IDs
        do *not* exist in the target table.
        """
        pass

    @property
    @abstractmethod
    def custom_field_mapping(self) -> Dict[str, str]:
        """Returns the custom field mapping used by the repository."""
        pass

    @abstractmethod
    def ensure_schema_exists(self) -> None:
        """Ensures the target table and necessary indexes exist."""
        pass