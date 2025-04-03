import time
import requests
import structlog
import re
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type
from pybreaker import CircuitBreaker
from typing import Dict, List, Optional, Generator, Any, Set

from infrastructure.config.settings import settings
from infrastructure.cache import RedisCache
from application.ports.pipedrive_client_port import PipedriveClientPort
from application.utils.column_utils import normalize_column_name 
from infrastructure.monitoring.metrics import (
    api_request_duration_hist,
    api_errors_counter,
    pipedrive_api_token_cost_total
)


log = structlog.get_logger(__name__)

# Circuit Breaker
api_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)

class PipedriveAPIClient(PipedriveClientPort):
    BASE_URL_V1 = "https://api.pipedrive.com/v1"
    BASE_URL_V2 = "https://api.pipedrive.com/api/v2"

    DEFAULT_TIMEOUT = 30
    DEFAULT_V2_LIMIT = 500
    MAX_V1_PAGINATION_LIMIT = 500
    # TTL para mapas e lookups individuais
    DEFAULT_MAP_CACHE_TTL_SECONDS = 3600 * 12
    # TTL menor para lookups individuais de person
    PERSON_LOOKUP_CACHE_TTL_SECONDS = 3600 * 1
    
    ENDPOINT_COSTS = {
        '/deals': 10,              # GET /api/v2/deals
        '/dealFields': 20,         # GET /v1/dealFields
        '/persons': 10,            # GET /api/v2/persons
        '/persons/detail': 1,      # GET /api/v2/persons/{id}
        '/stages': 5,              # GET /api/v2/stages
        '/stages/detail': 1,       # GET /api/v2/stages/{id}
        '/pipelines': 5,           # GET /api/v2/pipelines
        '/users': 20,              # GET /v1/users
        '/users/detail': 2,        # GET /v1/users/{id}
    }
    DEFAULT_ENDPOINT_COST = 1 

    def __init__(self, cache: RedisCache):
        self.api_key = settings.PIPEDRIVE_API_KEY
        if not self.api_key:
            log.error("PIPEDRIVE_API_KEY is not set!")
            raise ValueError("Pipedrive API Key is required.")

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})
        self.cache = cache
        self.log = log.bind(client="PipedriveAPIClient")
        
    def _normalize_endpoint_for_metrics(self, url_path: str) -> str:
        """Normaliza o path da URL para usar como label na métrica, tratando IDs."""
        path = url_path.replace(self.BASE_URL_V1, '').replace(self.BASE_URL_V2, '')
        path = path.split('?')[0].strip('/')

        normalized_path = re.sub(r'/(\d+)$', '/detail', path)

        if not normalized_path.startswith('/'):
            normalized_path = '/' + normalized_path

        return normalized_path

    @api_breaker
    @retry(
        stop=stop_after_attempt(4),
        wait=wait_exponential(multiplier=1, min=4, max=30),
        retry=(
            retry_if_exception_type(requests.exceptions.Timeout) |
            retry_if_exception_type(requests.exceptions.ConnectionError) |
            retry_if_exception_type(requests.exceptions.ChunkedEncodingError) |
            retry_if_exception_type(requests.exceptions.HTTPError)
        ),
        reraise=True
    )
    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """Faz uma requisição GET com retry, timeout, circuit breaker e métricas."""
        effective_params = params or {}
        if "api_token" not in effective_params:
             effective_params["api_token"] = self.api_key

        start_time = time.monotonic()
        error_type = "unknown"; status_code = None; response = None
        log_params = {k:v for k,v in effective_params.items() if k != 'api_token'}
        
        raw_endpoint_path = url.split('?')[0]
        normalized_endpoint_label = self._normalize_endpoint_for_metrics(raw_endpoint_path)
        
        endpoint_name = url.split('?')[0]
        if self.BASE_URL_V1 in endpoint_name:
             endpoint_name = endpoint_name.split(self.BASE_URL_V1)[-1]
        elif self.BASE_URL_V2 in endpoint_name:
            endpoint_name = endpoint_name.split(self.BASE_URL_V2)[-1]
        endpoint_name = endpoint_name.strip('/')

        request_log = self.log.bind(endpoint=normalized_endpoint_label, method='GET', params=log_params)

        try:
            request_log.debug("Making API GET request") 

            response = self.session.get(url, params=effective_params, timeout=self.DEFAULT_TIMEOUT)
            status_code = response.status_code

            if 400 <= status_code < 500 and status_code != 429:
                # Não tentar novamente erros 4xx (exceto 429)
                response.raise_for_status() 
            elif status_code >= 500 or status_code == 429:
                # Tentar novamente erros 5xx ou 429
                 response.raise_for_status()

            if 200 <= status_code < 300:
                cost = self.ENDPOINT_COSTS.get(normalized_endpoint_label, self.DEFAULT_ENDPOINT_COST)
                if cost > 0:
                    pipedrive_api_token_cost_total.labels(endpoint=normalized_endpoint_label).inc(cost)
                    request_log.debug("API Token cost incremented", cost=cost)
                elif normalized_endpoint_label not in self.ENDPOINT_COSTS:
                     request_log.warning("API endpoint cost not defined, using default.", endpoint_called=raw_endpoint_path, normalized_endpoint=normalized_endpoint_label, default_cost=self.DEFAULT_ENDPOINT_COST)
                     if self.DEFAULT_ENDPOINT_COST > 0:
                           pipedrive_api_token_cost_total.labels(endpoint=normalized_endpoint_label).inc(self.DEFAULT_ENDPOINT_COST)
    
            duration = time.monotonic() - start_time
            api_request_duration_hist.labels(endpoint=normalized_endpoint_label, method='GET', status_code=status_code).observe(duration)
            request_log.debug("API GET request successful", status_code=status_code, duration_sec=f"{duration:.3f}s")
            return response

        except requests.exceptions.Timeout as e:
             error_type = "timeout"; request_log.warning("API request timed out", error=str(e)); raise
        except requests.exceptions.ConnectionError as e:
             error_type = "connection_error"; request_log.warning("API connection error", error=str(e)); raise
        except requests.exceptions.HTTPError as e:
             error_type = "http_error"
             response_text_snippet = e.response.text[:200] if e.response else "N/A"
             log_method = request_log.error if status_code and status_code >= 500 else request_log.warning
             log_method("API request failed with HTTP error", status_code=status_code, response_text=response_text_snippet, error=str(e))
             raise
        except requests.exceptions.RequestException as e:
             error_type = "request_exception"; request_log.error("API request failed (RequestException)", error=str(e), exc_info=True); raise
        finally:
             if error_type != "unknown" or (status_code and not 200 <= status_code < 300):
                 final_status_code = status_code if status_code else 'N/A'
                 api_errors_counter.labels(endpoint=normalized_endpoint_label, error_type=error_type, status_code=final_status_code).inc()


    def _fetch_paginated_v1(self, url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """Helper para buscar todos os itens de um endpoint V1 paginado (start/limit)."""
        all_data = []; start = 0
        base_params = params or {}; base_params["limit"] = self.MAX_V1_PAGINATION_LIMIT
        endpoint_name = url.split(self.BASE_URL_V1)[-1] if self.BASE_URL_V1 in url else url
        page_num = 0

        while True:
            page_num += 1
            current_params = base_params.copy(); current_params["start"] = start
            page_log = self.log.bind(endpoint=endpoint_name, page=page_num, start=start, limit=current_params["limit"])
            page_log.debug("Fetching V1 page")
            try:
                response = self._get(url, params=current_params); json_response = response.json()
                if not json_response or not json_response.get("success"): page_log.warning("API response indicates failure or empty data", response_preview=str(json_response)[:200]); break
                current_data = json_response.get("data", [])
                if not current_data: page_log.debug("No more V1 data found on this page."); break
                all_data.extend(current_data); page_log.debug(f"Fetched {len(current_data)} V1 items for this page.")
                pagination_info = json_response.get("additional_data", {}).get("pagination", {}); more_items = pagination_info.get("more_items_in_collection", False)
                if more_items:
                    next_start = pagination_info.get("next_start")
                    if next_start is not None: start = next_start
                    else: page_log.warning("API indicates more items but no 'next_start'. Stopping.", pagination=pagination_info); break
                else: page_log.debug("API indicates no more items in collection."); break
            except Exception as e: page_log.error("Error during V1 fetching page", error=str(e), exc_info=True); break 
        self.log.info(f"V1 Paginated fetch complete.", endpoint=endpoint_name, total_items=len(all_data), total_pages=page_num)
        return all_data

    def _fetch_paginated_v2(self, url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """Helper para buscar todos os itens de um endpoint V2 paginado (cursor)."""
        all_data = []; next_cursor: Optional[str] = None
        base_params = params or {}; base_params["limit"] = self.DEFAULT_V2_LIMIT
        endpoint_name = url.split(self.BASE_URL_V2)[-1] if self.BASE_URL_V2 in url else url
        page_num = 0

        while True:
            page_num += 1
            current_params = base_params.copy()
            if next_cursor: current_params["cursor"] = next_cursor
            elif "cursor" in current_params: del current_params["cursor"]
            page_log = self.log.bind(endpoint=endpoint_name, page=page_num, limit=current_params["limit"], cursor=next_cursor)
            page_log.debug("Fetching V2 page")
            try:
                response = self._get(url, params=current_params); json_response = response.json()
                if not json_response or not json_response.get("success"): page_log.warning("API response indicates failure or empty data", response_preview=str(json_response)[:200]); break
                current_data = json_response.get("data", [])
                if not current_data: page_log.debug("No more V2 data found on this page."); break
                all_data.extend(current_data); page_log.debug(f"Fetched {len(current_data)} V2 items for this page.")
                additional_data = json_response.get("additional_data", {}); next_cursor = additional_data.get("next_cursor") 
                if not next_cursor: page_log.debug("No 'next_cursor' found. Ending pagination."); break
            except Exception as e: page_log.error("Error during V2 fetching page", error=str(e), exc_info=True); break 
        self.log.info(f"V2 Paginated fetch complete.", endpoint=endpoint_name, total_items=len(all_data), total_pages=page_num)
        return all_data

    # --- Métodos de busca de mapas ---
    def fetch_all_users_map(self) -> Dict[int, str]:
        cache_key = "pipedrive:all_users_map"
        cached = self.cache.get(cache_key)
        if cached and isinstance(cached, dict): self.log.info("Users map retrieved from cache.", cache_hit=True, map_size=len(cached)); return cached
        self.log.info("Fetching users map from API (V1).", cache_hit=False)
        url = f"{self.BASE_URL_V1}/users"
        try:
            all_users = self._fetch_paginated_v1(url)
            user_map = {user['id']: user['name'] for user in all_users if user and 'id' in user and 'name' in user}
            if user_map: self.cache.set(cache_key, user_map, ex_seconds=self.DEFAULT_MAP_CACHE_TTL_SECONDS); self.log.info("Users map fetched and cached.", map_size=len(user_map))
            else: self.log.warning("Fetched user list was empty or malformed.")
            return user_map
        except Exception as e: self.log.error("Failed to fetch/process users map", error=str(e), exc_info=True); return {}

    def fetch_all_stages_map(self) -> Dict[int, str]:
        cache_key = "pipedrive:all_stages_map"
        cached = self.cache.get(cache_key)
        if cached and isinstance(cached, dict): self.log.info("Stages map retrieved from cache.", cache_hit=True, map_size=len(cached)); return cached
        self.log.info("Fetching stages map from API (V2).", cache_hit=False)
        url = f"{self.BASE_URL_V2}/stages"
        try:
            all_stages = self._fetch_paginated_v2(url)
            stage_map = {stage['id']: stage['name'] for stage in all_stages if stage and 'id' in stage and 'name' in stage}
            if stage_map: self.cache.set(cache_key, stage_map, ex_seconds=self.DEFAULT_MAP_CACHE_TTL_SECONDS); self.log.info("Stages map fetched and cached.", map_size=len(stage_map))
            else: self.log.warning("Fetched stage list was empty or malformed.")
            return stage_map
        except Exception as e: self.log.error("Failed to fetch/process stages map", error=str(e), exc_info=True); return {}

    def fetch_all_pipelines_map(self) -> Dict[int, str]:
        cache_key = "pipedrive:all_pipelines_map"
        cached = self.cache.get(cache_key)
        if cached and isinstance(cached, dict): self.log.info("Pipelines map retrieved from cache.", cache_hit=True, map_size=len(cached)); return cached
        self.log.info("Fetching pipelines map from API (V2).", cache_hit=False)
        url = f"{self.BASE_URL_V2}/pipelines"
        try:
            all_pipelines = self._fetch_paginated_v2(url)
            pipeline_map = {p['id']: p['name'] for p in all_pipelines if p and 'id' in p and 'name' in p}
            if pipeline_map: self.cache.set(cache_key, pipeline_map, ex_seconds=self.DEFAULT_MAP_CACHE_TTL_SECONDS); self.log.info("Pipelines map fetched and cached.", map_size=len(pipeline_map))
            else: self.log.warning("Fetched pipeline list was empty or malformed.")
            return pipeline_map
        except Exception as e: self.log.error("Failed to fetch/process pipelines map", error=str(e), exc_info=True); return {}
        
    def fetch_person_name(self, person_id: int) -> Optional[str]:
        """Busca o nome de uma person específica por ID, usando cache."""
        if not person_id or not isinstance(person_id, int) or person_id <= 0:
            self.log.warning("Invalid person_id received for lookup", person_id=person_id)
            return None

        cache_key = f"pipedrive:person_name:{person_id}"
        person_log = self.log.bind(person_id=person_id)

        try:
            cached_name = self.cache.get(cache_key)
            if cached_name is not None: 
                 person_log.debug("Person name retrieved from cache.", cache_hit=True, name=cached_name if cached_name else "''")
                 return str(cached_name) if cached_name is not None else None
        except Exception as cache_get_err:
             person_log.error("Failed to get person name from cache", error=str(cache_get_err))

        person_log.info("Person name cache miss, fetching from API.", cache_hit=False)
        url = f"{self.BASE_URL_V1}/persons/{person_id}"
        try:
            response = self._get(url)
            if response.status_code == 200:
                data = response.json()
                if data and data.get("success"):
                    person_data = data.get("data")
                    if person_data and 'name' in person_data:
                        name = person_data['name']
                        person_log.debug("Person name fetched from API successfully.", name=name if name else "''")
                        try:
                            self.cache.set(cache_key, name if name is not None else '', ex_seconds=self.PERSON_LOOKUP_CACHE_TTL_SECONDS)
                        except Exception as cache_set_err:
                            person_log.error("Failed to set person name in cache", error=str(cache_set_err))
                        return name
                    else:
                         person_log.warning("Person data received from API but 'name' field missing or invalid.", api_response_data=person_data)
                         self.cache.set(cache_key, '', ex_seconds=self.PERSON_LOOKUP_CACHE_TTL_SECONDS)
                         return None 
                else:
                    person_log.warning("API request for person successful (200) but response indicates failure.", api_response=response.text[:200])
                    return None 
            elif response.status_code == 404:
                 person_log.warning("Person ID not found in Pipedrive API (404).")
                 self.cache.set(cache_key, '', ex_seconds=self.PERSON_LOOKUP_CACHE_TTL_SECONDS)
                 return None
            else:
                 person_log.warning(f"Failed to fetch person name from API, status code: {response.status_code}.")
                 return None

        except Exception as e:
            person_log.error("Unexpected error fetching person name from API", error=str(e), exc_info=True)
            return None


    def fetch_person_names_for_ids(self, person_ids: Set[int]) -> Dict[int, str]:
        """
        Busca nomes para um conjunto de IDs de persons, priorizando cache.
        NOTA: Atualmente faz chamadas individuais à API para cache misses.
              O ideal seria usar um endpoint de busca em lote da API se disponível.
        """
        if not person_ids:
            return {}

        names_map: Dict[int, str] = {}
        ids_to_fetch_from_api: Set[int] = set()
        fetch_log = self.log.bind(total_ids_requested=len(person_ids))
        fetch_log.debug("Starting fetch for multiple person names.")

        # 1. Tentar buscar do cache
        cache_check_start = time.monotonic()
        cached_count = 0
        cache_fail_count = 0
        for p_id in person_ids:
             if not isinstance(p_id, int) or p_id <= 0: continue 
             cache_key = f"pipedrive:person_name:{p_id}"
             try:
                 cached_name = self.cache.get(cache_key)
                 if cached_name is not None:
                     names_map[p_id] = str(cached_name) 
                     cached_count += 1
                 else:
                     ids_to_fetch_from_api.add(p_id)
             except Exception as cache_err:
                 self.log.error("Cache GET error during batch person lookup", person_id=p_id, error=str(cache_err))
                 ids_to_fetch_from_api.add(p_id) 
                 cache_fail_count += 1

        cache_check_duration = time.monotonic() - cache_check_start
        fetch_log.info(
            "Person names cache check completed.",
            cache_hits=cached_count,
            cache_misses=len(ids_to_fetch_from_api),
            cache_errors=cache_fail_count,
            duration_sec=f"{cache_check_duration:.3f}s"
        )

        # 2. Buscar IDs restantes da API
        if ids_to_fetch_from_api:
            api_fetch_start = time.monotonic()
            api_fetched_count = 0
            api_fail_count = 0
            fetch_log.info(f"Fetching {len(ids_to_fetch_from_api)} person names from API.")
            for p_id in ids_to_fetch_from_api:
                # Reutiliza a lógica de fetch_person_name que já inclui retry e cache set
                name = self.fetch_person_name(p_id)
                if name is not None: # Mesmo se for '', considera sucesso
                     names_map[p_id] = name
                     api_fetched_count += 1
                else:
                     api_fail_count += 1

            api_fetch_duration = time.monotonic() - api_fetch_start
            fetch_log.info(
                "Person names API fetch completed.",
                api_fetched_ok=api_fetched_count,
                api_fetch_failed=api_fail_count, 
                duration_sec=f"{api_fetch_duration:.3f}s"
             )

        fetch_log.debug("Finished fetching multiple person names.", final_map_size=len(names_map))
        return names_map

    def fetch_deal_fields_mapping(self) -> Dict[str, str]:
        cache_key = "pipedrive:deal_fields_mapping"; cache_ttl_seconds = 86400 # 24h
        cached = self.cache.get(cache_key)
        if cached and isinstance(cached, dict): self.log.info("Deal fields mapping retrieved from cache.", cache_hit=True, map_size=len(cached)); return cached
        self.log.info("Fetching deal fields mapping from API (V1).", cache_hit=False)
        url = f"{self.BASE_URL_V1}/dealFields"
        try:
            all_fields_data = self._fetch_paginated_v1(url)
            if not all_fields_data: self.log.warning("Received no data for deal fields from API."); return {}
            non_custom_keys = {
                "id", "creator_user_id", "user_id", "person_id", "org_id",
                "stage_id", "pipeline_id", "title", "value", "currency", "add_time",
                "update_time", "status", "lost_reason", "visible_to", "close_time",
                "won_time", "lost_time", "first_won_time", "products_count",
                "files_count", "notes_count", "followers_count", "email_messages_count",
                "activities_count", "done_activities_count", "undone_activities_count",
                "participants_count", "expected_close_date", "probability",
                "next_activity_date", "next_activity_time", "next_activity_id",
                "last_activity_id", "last_activity_date", "stage_change_time",
                "last_incoming_mail_time", "last_outgoing_mail_time",
                "label", "stage_order_nr", "person_name", "org_name", "next_activity_subject",
                "next_activity_type", "next_activity_duration", "next_activity_note",
                "formatted_value", "weighted_value", "formatted_weighted_value",
                "weighted_value_currency", "rotten_time", "owner_name", "cc_email"
            }

            mapping = {}
            for field in all_fields_data:
                 api_key = field.get("key")
                 name = field.get("name")
                 if api_key and name and api_key not in non_custom_keys:
                     normalized = normalize_column_name(name) 
                     if normalized:
                         # Adiciona log se houver colisão de nome normalizado
                         if normalized in [m for m in mapping.values()]:
                             self.log.warning("Normalized custom field name collision detected.",
                                               conflicting_api_key=api_key,
                                               conflicting_name=name,
                                               normalized_name=normalized)
                         mapping[api_key] = normalized
                     else:
                         self.log.warning("Failed to normalize custom field name.", api_key=api_key, original_name=name)

            self.cache.set(cache_key, mapping, ex_seconds=cache_ttl_seconds)
            self.log.info("Deal fields mapping fetched and cached.", total_fields_api=len(all_fields_data), custom_mapping_count=len(mapping))
            return mapping
        except Exception as e: self.log.error("Failed to fetch and process deal fields mapping", error=str(e), exc_info=True); return {}

    def get_last_timestamp(self) -> str | None:
        cache_key = "pipedrive:last_update_timestamp"
        timestamp = self.cache.get(cache_key)
        if timestamp and isinstance(timestamp, str):
             self.log.debug("Last timestamp retrieved from cache", timestamp=timestamp)
             return timestamp
        self.log.info("No last update timestamp found in cache."); return None 

    def update_last_timestamp(self, new_timestamp: str):
        cache_key = "pipedrive:last_update_timestamp"; cache_ttl_seconds = 2592000 # 30 dias
        try: self.cache.set(cache_key, new_timestamp, ex_seconds=cache_ttl_seconds); self.log.info("Updated last update timestamp in cache", timestamp=new_timestamp)
        except Exception as e: self.log.error("Failed to store last update timestamp in cache", timestamp=new_timestamp, error=str(e), exc_info=True)

    def _fetch_paginated_v2_stream(self, url: str, params: Optional[Dict[str, Any]] = None) -> Generator[Dict, None, None]:
        """Helper generator para buscar itens V2 um por um via cursor."""
        next_cursor: Optional[str] = None
        base_params = params or {}; base_params["limit"] = self.DEFAULT_V2_LIMIT
        endpoint_name = url.split(self.BASE_URL_V2)[-1] if self.BASE_URL_V2 in url else url
        endpoint_name = endpoint_name.split('?')[0].strip('/') 
        items_yielded = 0; page_num = 0
        log_every_n_pages = 50

        while True:
            page_num += 1
            current_params = base_params.copy()
            if next_cursor: current_params["cursor"] = next_cursor
            elif "cursor" in current_params: del current_params["cursor"]

            page_log = self.log.bind(endpoint=endpoint_name, page=page_num, limit=current_params["limit"], cursor=next_cursor)

            if page_num == 1 or page_num % log_every_n_pages == 0:
                 page_log.info("Fetching V2 page for stream", items_yielded_so_far=items_yielded)
            else:
                 page_log.debug("Fetching V2 page for stream", items_yielded_so_far=items_yielded)


            try:
                response = self._get(url, params=current_params); json_response = response.json()
                if not json_response or not json_response.get("success"): page_log.warning("V2 API stream response indicates failure or empty data", response_preview=str(json_response)[:200]); break
                current_data = json_response.get("data", [])
                if not current_data: page_log.info("No more V2 stream data found on this page."); break

                for item in current_data: items_yielded += 1; yield item

                additional_data = json_response.get("additional_data", {}); next_cursor = additional_data.get("next_cursor")
                if not next_cursor: page_log.info("No 'next_cursor' found. Ending pagination stream."); break
            except Exception as e: page_log.error("Error during V2 stream fetching page, stopping stream.", error=str(e), exc_info=True); break

        self.log.info(f"V2 Stream fetch complete.", endpoint=endpoint_name, total_items_yielded=items_yielded, total_pages=page_num)


    def fetch_all_deals_stream(self, updated_since: str = None) -> Generator[Dict, None, None]:
        """Busca deals (V2) usando paginação por cursor e entrega registros um a um."""
        url = f"{self.BASE_URL_V2}/deals"
        params = {"sort_by": "update_time", "sort_direction": "asc"}
        if updated_since:
            params["updated_since"] = updated_since
            self.log.info("Fetching deals stream with filter", updated_since=updated_since)
        else:
            self.log.info("Fetching deals stream without update filter (full load).")
        yield from self._fetch_paginated_v2_stream(url, params)


    def update_last_timestamp(self, new_timestamp: str):
        """Armazena o último timestamp processado no cache."""
        cache_key = "pipedrive:last_update_timestamp"; cache_ttl_seconds = 2592000 # 30 dias
        try: self.cache.set(cache_key, new_timestamp, ex_seconds=cache_ttl_seconds); self.log.info("Updated last update timestamp in cache", timestamp=new_timestamp)
        except Exception as e: self.log.error("Failed to store last update timestamp in cache", timestamp=new_timestamp, error=str(e), exc_info=True)