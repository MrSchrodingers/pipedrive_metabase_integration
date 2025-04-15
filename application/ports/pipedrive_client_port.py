from abc import ABC, abstractmethod
from typing import Dict, List, Generator, Optional

class PipedriveClientPort(ABC):

    @abstractmethod
    def fetch_deal_fields_mapping(self) -> Dict[str, str]:
        pass

    @abstractmethod
    def fetch_deal_fields(self) -> List[Dict]: 
        pass

    @abstractmethod
    def fetch_all_deals_stream(self, updated_since: Optional[str] = None, items_limit: Optional[int] = None) -> Generator[Dict, None, None]: # Generator de Dict, não List[Dict]
        pass

    @abstractmethod
    def update_last_timestamp(self, new_timestamp: str) -> None:
        pass

    @abstractmethod
    def get_last_timestamp(self) -> Optional[str]:
        pass

    @abstractmethod
    def fetch_deal_changelog(self, deal_id: int) -> List[Dict]:
        pass

    @abstractmethod
    def fetch_all_users(self) -> List[Dict]:
        pass

    @abstractmethod
    def fetch_all_persons(self) -> List[Dict]:
        pass

    @abstractmethod
    def fetch_all_stages_details(self) -> List[Dict]:
        pass

    @abstractmethod
    def fetch_all_pipelines(self) -> List[Dict]:
        pass

    @abstractmethod
    def fetch_all_organizations(self) -> List[Dict]:
        pass