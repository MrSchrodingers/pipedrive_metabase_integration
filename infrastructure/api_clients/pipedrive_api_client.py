import requests
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from infrastructure.config.settings import settings
from infrastructure.cache import RedisCache

class PipedriveAPIClient:
    def __init__(self):
        self.api_key = settings.PIPEDRIVE_API_KEY
        self.base_url_v2 = "https://api.pipedrive.com/api/v2"
        self.base_url_v1 = "https://api.pipedrive.com/v1"
        self.session = requests.Session()
        self.cache = RedisCache()  # Instância do cache

    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10),
        retry=retry_if_exception_type(requests.exceptions.RequestException)
    )
    def _get(self, url, params=None):
        response = self.session.get(url, params=params, timeout=10)
        response.raise_for_status()
        return response

    def fetch_deal_fields_mapping(self):
        cache_key = "deal_fields_mapping"
        cached = self.cache.get(cache_key)
        if cached:
            return cached

        url = f"{self.base_url_v1}/dealFields?api_token={self.api_key}"
        response = self._get(url)
        data = response.json().get("data", [])
        base_columns = {
            "id", "title", "creator_user_id", "user_id", "person_id",
            "stage_id", "pipeline_id", "status", "value", "currency",
            "add_time", "update_time", "raw_data"
        }
        mapping = {
            field["key"]: field["name"].lower().replace(" ", "_")
            for field in data
            if field.get("key") and field.get("name") and field["key"] not in base_columns
        }
        self.cache.set(cache_key, mapping, ex_seconds=86400)  # Cache por 24h
        return mapping

    def fetch_all_deals(self, updated_since: str = None):
        url = f"{self.base_url_v2}/deals"
        params = {
            "api_token": self.api_key,
            "limit": 500
        }
        # Se o parâmetro não for informado, tenta buscar o último timestamp armazenado no cache
        if not updated_since:
            updated_since = self.cache.get("last_update")
            if updated_since:
                params["updated_since"] = updated_since

        all_data = []
        while True:
            response = self._get(url, params=params)
            json_response = response.json()
            if not json_response.get("success"):
                break

            data = json_response.get("data", [])
            if data:
                all_data.extend(data)

            next_cursor = json_response.get("additional_data", {}).get("next_cursor")
            if not next_cursor:
                break

            params = {
                "api_token": self.api_key,
                "limit": 500,
                "cursor": next_cursor
            }
        return all_data

    def update_last_timestamp(self, new_timestamp: str):
        self.cache.set("last_update", new_timestamp, ex_seconds=86400)
