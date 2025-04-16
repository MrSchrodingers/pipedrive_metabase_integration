import time
import requests
import structlog
from tenacity import RetryError, retry, retry_if_exception, stop_after_attempt, wait_exponential, retry_if_exception_type
from pybreaker import CircuitBreaker
from typing import Dict, List, Optional, Generator, Any, Set, Tuple

from infrastructure.config.settings import settings
from infrastructure.cache import RedisCache
from application.ports.pipedrive_client_port import PipedriveClientPort
from application.utils.column_utils import normalize_column_name 
from infrastructure.monitoring.metrics import (
    api_request_duration_hist,
    api_errors_counter,
    pipedrive_api_token_cost_total,
    pipedrive_api_call_total,
    pipedrive_api_cache_hit_total,
    pipedrive_api_cache_miss_total,
    pipedrive_api_rate_limit_remaining,
    pipedrive_api_rate_limit_reset_seconds,
    pipedrive_api_retries_total,
    pipedrive_api_retry_failures_total
)


log = structlog.get_logger(__name__)

# Circuit Breaker
api_breaker = CircuitBreaker(fail_max=3, reset_timeout=60)

class PipedriveAPIClient(PipedriveClientPort):
    BASE_URL_V1 = "https://api.pipedrive.com/v1"
    BASE_URL_V2 = "https://api.pipedrive.com/api/v2"

    DEFAULT_TIMEOUT = 45
    DEFAULT_V2_LIMIT = 500
    MAX_V1_PAGINATION_LIMIT = 500
    CHANGELOG_PAGE_LIMIT = 500

    # TTL para mapas e lookups individuais
    DEFAULT_MAP_CACHE_TTL_SECONDS = 300 * 12  # 12 hours
    STAGE_DETAILS_CACHE_TTL_SECONDS = 300 * 6  # 6 hours
    
    ENDPOINT_COSTS = {
        # V1 endpoints
        '/deals/detail/changelog': 20,  # GET /v1/deals/{id}/changelog
        '/dealFields': 20,              # GET /v1/dealFields
        '/users': 20,                   # GET /v1/users
        '/users/detail': 5,             # GET /v1/users/{id} se existir

        # V2 endpoints
        '/deals': 10,                   # GET /api/v2/deals
        '/stages': 5,                   # GET /api/v2/stages
        '/pipelines': 5,                # GET /api/v2/pipelines
        '/persons/detail': 1,           # GET /api/v2/persons/{id}
        '/persons': 10,                 # GET /api/v2/persons?ids=...
    }
    
    DEFAULT_ENDPOINT_COST = 10
    UNKNOWN_NAME = "Desconhecido"
    
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
        """Normaliza o path da URL para usar como label na métrica, tratando IDs etc."""
        base_url_v1_len = len(self.BASE_URL_V1)
        base_url_v2_len = len(self.BASE_URL_V2)
        path = url_path

        if url_path.startswith(self.BASE_URL_V1):
            path = url_path[base_url_v1_len:]
        elif url_path.startswith(self.BASE_URL_V2):
            path = url_path[base_url_v2_len:]

        path = path.split('?')[0].strip('/')
        parts = path.split('/')

        if not parts or not parts[0]:
            return "/"

        resource = parts[0]
        normalized_path = f"/{resource}"

        # /resource/{id} -> /resource/detail
        if len(parts) > 1 and parts[1].isdigit():
            normalized_path += "/detail"
            # /resource/{id}/subresource -> /resource/detail/subresource
            if len(parts) > 2:
                # (exemplo: /deals/detail/changelog)
                known_subresources = [
                    'changelog', 'followers', 'activities', 'files',
                    'mailMessages', 'participants', 'products'
                ]
                if parts[2] in known_subresources:
                    normalized_path += f"/{parts[2]}"
                # /resource/{id}/products/{product_id} => /resource/detail/products/detail
                elif len(parts) > 3 and parts[3].isdigit():
                    normalized_path += f"/{parts[2]}/detail"

        # /resource/subresource
        elif len(parts) > 1:
            known_actions = ['search', 'summary', 'timeline', 'collection', 'products', 'installments']
            if parts[1] in known_actions:
                normalized_path += f"/{parts[1]}"

        self.log.debug("Normalized endpoint", original=url_path, normalized=normalized_path)
        return normalized_path
    
    @api_breaker
    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=4, max=60),
        retry=(
            retry_if_exception_type(requests.exceptions.Timeout) |
            retry_if_exception_type(requests.exceptions.ConnectionError) |
            retry_if_exception_type(requests.exceptions.ChunkedEncodingError) |
            retry_if_exception(lambda e: isinstance(e, requests.exceptions.HTTPError) 
                               and getattr(e.response, 'status_code', None) >= 500) |
            retry_if_exception(lambda e: isinstance(e, requests.exceptions.HTTPError) 
                               and getattr(e.response, 'status_code', None) == 429)
        ),
        reraise=True
    )
    def _get(self, url: str, params: Optional[Dict[str, Any]] = None) -> requests.Response:
        """
        Único método que realmente faz uma requisição GET ao Pipedrive.
        - Só aqui incrementamos o custo de tokens.
        - Se algo vier do cache, não passará por aqui.
        """
        effective_params = params.copy() if params else {}
        if "api_token" not in effective_params:
            effective_params["api_token"] = self.api_key

        start_time = time.monotonic()
        error_type = "success"
        status_code = None
        response = None
        log_params = {k: v for k, v in effective_params.items() if k != 'api_token'}

        raw_endpoint_path = url
        normalized_endpoint_label = self._normalize_endpoint_for_metrics(raw_endpoint_path)
        request_log = self.log.bind(endpoint=normalized_endpoint_label, method='GET', params=log_params)

        try:
            request_log.debug("Making API GET request", url=url)
            response = self.session.get(url, params=effective_params, timeout=self.DEFAULT_TIMEOUT)
            status_code = response.status_code
            pipedrive_api_call_total.labels(
                endpoint=normalized_endpoint_label,
                method='GET',
                status_code=str(status_code)
            ).inc()

            # Rate limit handling
            if status_code == 429:
                retry_after = response.headers.get("Retry-After")
                request_log.warning("Rate limit hit (429)", retry_after=retry_after)

            if not response.ok:
                error_type = f"http_{status_code}"
                try:
                    response.raise_for_status()
                except requests.exceptions.HTTPError as e:
                    snippet = e.response.text[:200] if e.response else "N/A"
                    log_method = request_log.error if status_code >= 500 else request_log.warning
                    log_method("API request failed with HTTP error", 
                               status_code=status_code, 
                               response_text=snippet, 
                               error=str(e))
                    raise e

            # --- Se chegou aqui, é 2xx (ok) ou 3xx sem raise_for_status ---
            cost = self.ENDPOINT_COSTS.get(normalized_endpoint_label, self.DEFAULT_ENDPOINT_COST)
            request_log.debug("Incrementing API token cost", cost=cost, endpoint=normalized_endpoint_label)
            pipedrive_api_token_cost_total.labels(endpoint=normalized_endpoint_label).inc(cost)
            remaining = response.headers.get('X-RateLimit-Remaining')
            if remaining and remaining.isdigit():
                pipedrive_api_rate_limit_remaining.labels(endpoint=normalized_endpoint_label).set(int(remaining))
            reset = response.headers.get('X-RateLimit-Reset')
            if reset and reset.isdigit():
                pipedrive_api_rate_limit_reset_seconds.labels(endpoint=normalized_endpoint_label).set(int(reset))

            duration = time.monotonic() - start_time
            api_request_duration_hist.labels(
                endpoint=normalized_endpoint_label, 
                method='GET', 
                status_code=status_code
            ).observe(duration)

            request_log.debug("API GET request successful", status_code=status_code, duration_sec=f"{duration:.3f}s")
            return response

        except requests.exceptions.Timeout as e:
            pipedrive_api_retries_total.labels(endpoint=normalized_endpoint_label).inc()
            error_type = "timeout"
            request_log.warning("API request timed out", error=str(e))
            raise
        except requests.exceptions.ConnectionError as e:
            error_type = "connection_error"
            request_log.warning("API connection error", error=str(e))
            raise
        except requests.exceptions.RequestException as e:
            pipedrive_api_retry_failures_total.labels(endpoint=normalized_endpoint_label).inc()
            error_type = "request_exception"
            request_log.error("API request failed (RequestException)", exc_info=True)

        # Caso de exceção
        current_status_code = status_code
        if hasattr(e, 'response') and e.response is not None:
            current_status_code = e.response.status_code
        elif isinstance(e, RetryError) and hasattr(e.cause, 'response') and e.cause.response is not None:
            current_status_code = e.cause.response.status_code

        final_status_code_label = str(current_status_code) if current_status_code else 'N/A'
        api_errors_counter.labels(endpoint=normalized_endpoint_label, 
                                  error_type=error_type, 
                                  status_code=final_status_code_label).inc()
        request_log.debug("API Error counter incremented", 
                          error_type=error_type, 
                          status_code=final_status_code_label)
        raise

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
            except Exception as e: page_log.error("Error during V1 fetching page", exc_info=True); break 
        self.log.info(f"V1 Paginated fetch complete.", endpoint=endpoint_name, total_items=len(all_data), total_pages=page_num)
        return all_data

    def _fetch_paginated_v2(self, url: str, params: Optional[Dict[str, Any]] = None) -> List[Dict]:
        """Helper para buscar todos os itens de um endpoint V2 paginado (cursor)."""
        all_data = []; next_cursor: Optional[str] = None
        base_params = params or {}; base_params["limit"] = self.DEFAULT_V2_LIMIT
        normalized_endpoint_label = self._normalize_endpoint_for_metrics(url)
        page_num = 0

        while True:
            page_num += 1
            current_params = base_params.copy()
            if next_cursor: current_params["cursor"] = next_cursor
            elif "cursor" in current_params: del current_params["cursor"]
            page_log = self.log.bind(endpoint=normalized_endpoint_label, page=page_num, limit=current_params["limit"], cursor=next_cursor)
            page_log.debug("Fetching V2 page")
            try:
                response = self._get(url, params=current_params); json_response = response.json()
                if not json_response or not json_response.get("success"): page_log.warning("API response indicates failure or empty data", response_preview=str(json_response)[:200]); break
                current_data = json_response.get("data", [])
                if not current_data: page_log.debug("No more V2 data found on this page."); break
                all_data.extend(current_data); page_log.debug(f"Fetched {len(current_data)} V2 items for this page.")
                additional_data = json_response.get("additional_data", {}); next_cursor = additional_data.get("next_cursor") 
                if not next_cursor: page_log.debug("No 'next_cursor' found. Ending pagination."); break
            except Exception as e: page_log.error("Error during V2 fetching page", exc_info=True); break 
        self.log.info(f"V2 Paginated fetch complete.", endpoint=normalized_endpoint_label, total_items=len(all_data), total_pages=page_num)
        return all_data

    # --- Métodos de busca de mapas ---
    def fetch_all_users_map(self) -> Dict[int, str]:
        cache_key = "pipedrive:all_users_map"
        cached = self.cache.get(cache_key)
        if cached:
            pipedrive_api_cache_hit_total.labels(entity="users", source="redis").inc()
            return cached
        else:
            pipedrive_api_cache_miss_total.labels(entity="users", source="redis").inc()
            
        if cached and isinstance(cached, dict): self.log.info("Users map retrieved from cache.", cache_hit=True, map_size=len(cached)); return cached
        self.log.info("Fetching users map from API (V1).", cache_hit=False)
        url = f"{self.BASE_URL_V1}/users"
        try:
            all_users = self._fetch_paginated_v1(url)
            user_map = {user['id']: user.get('name', self.UNKNOWN_NAME)
                        for user in all_users if user and 'id' in user}
            if user_map: self.cache.set(cache_key, user_map, ex_seconds=self.DEFAULT_MAP_CACHE_TTL_SECONDS); self.log.info("Users map fetched and cached.", map_size=len(user_map))
            else: self.log.warning("Fetched user list was empty or malformed.")
            return user_map
        except Exception as e: self.log.error("Failed to fetch/process users map", exc_info=True); return {}

    def fetch_all_pipelines_map(self) -> Dict[int, str]:
        cache_key = "pipedrive:all_pipelines_map"
        cached = self.cache.get(cache_key)
        if cached:
            pipedrive_api_cache_hit_total.labels(entity="pipelines", source="redis").inc()
            return cached
        else:
            pipedrive_api_cache_miss_total.labels(entity="pipelines", source="redis").inc()
        if cached and isinstance(cached, dict): self.log.info("Pipelines map retrieved from cache.", cache_hit=True, map_size=len(cached)); return cached
        self.log.info("Fetching pipelines map from API (V2).", cache_hit=False)
        url = f"{self.BASE_URL_V2}/pipelines"
        try:
            all_pipelines = self._fetch_paginated_v2(url)
            pipeline_map = {p['id']: p.get('name', self.UNKNOWN_NAME)
                            for p in all_pipelines if p and 'id' in p}
            if pipeline_map: self.cache.set(cache_key, pipeline_map, ex_seconds=self.DEFAULT_MAP_CACHE_TTL_SECONDS); self.log.info("Pipelines map fetched and cached.", map_size=len(pipeline_map))
            else: self.log.warning("Fetched pipeline list was empty or malformed.")
            return pipeline_map
        except Exception as e: self.log.error("Failed to fetch/process pipelines map", exc_info=True); return {}

    def fetch_all_persons_map(self) -> Dict[int, str]:
        """
        Busca todos os persons usando paginação V2 via stream
        e armazena em cache de forma eficiente em memória.
        """
        cache_key = "pipedrive:all_persons_map"
        cached = self.cache.get(cache_key)
        if cached:
            pipedrive_api_cache_hit_total.labels(entity="persons", source="redis").inc()
            return cached
        else:
            pipedrive_api_cache_miss_total.labels(entity="persons", source="redis").inc()
        if cached and isinstance(cached, dict):
            self.log.info("Persons map retrieved from cache.", cache_hit=True, map_size=len(cached))
            return cached

        self.log.info("Fetching persons map from API (V2 - streamed).", cache_hit=False)
        url = f"{self.BASE_URL_V2}/persons"
        person_map: Dict[int, str] = {}
        total_persons_processed = 0

        try:
            person_stream = self._fetch_paginated_v2_stream(url)

            for person in person_stream:
                total_persons_processed += 1
                person_id = person.get('id')
                person_name = person.get('name', '').strip() 
                if person_id and person_name:
                    person_map[person_id] = person_name
                elif person_id:
                    self.log.debug("Person found with empty name, skipping map entry.", person_id=person_id)

            if person_map:
                self.cache.set(cache_key, person_map, ex_seconds=self.DEFAULT_MAP_CACHE_TTL_SECONDS)
                self.log.info("Persons map fetched via stream and cached.", map_size=len(person_map), total_persons_processed=total_persons_processed)
            else:
                self.log.warning("Fetched person stream resulted in an empty map (no valid names found?).", total_persons_processed=total_persons_processed)

            return person_map

        except Exception as e:
            self.log.error("Failed to fetch/process persons map via stream", exc_info=True)
            return {}
        
    def fetch_all_stages_details(self) -> List[Dict]:
        """Busca detalhes de todos os stages (necessário para nomes normalizados)."""
        cache_key = "pipedrive:all_stages_details"
        cached = self.cache.get(cache_key)
        if cached:
            pipedrive_api_cache_hit_total.labels(entity="stages", source="redis").inc()
            return cached
        else:
            pipedrive_api_cache_miss_total.labels(entity="stages", source="redis").inc()
        if cached and isinstance(cached, list):
            self.log.info("Stage details retrieved from cache.", cache_hit=True, count=len(cached))
            return cached

        self.log.info("Fetching stage details from API (V2).", cache_hit=False)
        url = f"{self.BASE_URL_V2}/stages"
        try:
            all_stages = self._fetch_paginated_v2(url)
            if all_stages:
                self.cache.set(cache_key, all_stages, ex_seconds=self.STAGE_DETAILS_CACHE_TTL_SECONDS)
                self.log.info("Stage details fetched and cached.", count=len(all_stages))
            else:
                self.log.warning("Fetched stage list was empty.")
            return all_stages
        except Exception as e:
            self.log.error("Failed to fetch/process stage details", exc_info=True)
            return []
        
    def fetch_deal_fields_mapping(self) -> Dict[str, str]:
        cache_key = "pipedrive:deal_fields_mapping"; cache_ttl_seconds = 86400 # 24h
        cached = self.cache.get(cache_key)
        if cached:
            pipedrive_api_cache_hit_total.labels(entity="deal_fields", source="redis").inc()
            return cached
        else:
            pipedrive_api_cache_miss_total.labels(entity="deal_fields", source="redis").inc()
        if cached and isinstance(cached, dict): self.log.info("Deal fields mapping retrieved from cache.", cache_hit=True, map_size=len(cached)); return cached
        self.log.info("Fetching deal fields mapping from API (V1).", cache_hit=False)
        url = f"{self.BASE_URL_V1}/dealFields"
        try:
            all_fields_data = self._fetch_paginated_v1(url)
            if not all_fields_data: self.log.warning("Received no data for deal fields from API."); return {}
            non_custom_keys = {
                "id", "creator_user_id", "person_id", "org_id",
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
        except Exception as e: self.log.error("Failed to fetch and process deal fields mapping", exc_info=True); return {}

    def get_last_timestamp(self) -> str | None:
        cache_key = "pipedrive:last_update_timestamp"
        timestamp = self.cache.get(cache_key)
        if timestamp and isinstance(timestamp, str):
             self.log.debug("Last timestamp retrieved from cache", timestamp=timestamp)
             return timestamp
        self.log.info("No last update timestamp found in cache."); return None 

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
            except Exception as e: page_log.error("Error during V2 stream fetching page, stopping stream.", exc_info=True); break

        self.log.info(f"V2 Stream fetch complete.", endpoint=endpoint_name, total_items_yielded=items_yielded, total_pages=page_num)
        
    def _fetch_paginated_v1_stream_adapter(self, url: str, params: Optional[Dict[str, Any]] = None) -> Generator[Dict, None, None]:
        """
        Gera itens de endpoints V1 paginados usando start/limit.
        Útil para consumir dados sem carregar tudo na memória.
        """
        start = 0
        base_params = params.copy() if params else {}
        base_params["limit"] = self.MAX_V1_PAGINATION_LIMIT
        endpoint_name = url.split(self.BASE_URL_V1)[-1] if self.BASE_URL_V1 in url else url
        page_num = 0
        items_yielded = 0

        while True:
            page_num += 1
            current_params = base_params.copy()
            current_params["start"] = start

            page_log = self.log.bind(endpoint=endpoint_name, page=page_num, start=start, limit=current_params["limit"])

            try:
                response = self._get(url, params=current_params)
                json_response = response.json()

                if not json_response or not json_response.get("success"):
                    page_log.warning("V1 stream response indicates failure or empty data", response_preview=str(json_response)[:200])
                    break

                current_data = json_response.get("data", [])
                if not current_data:
                    page_log.info("No more V1 stream data found.")
                    break

                for item in current_data:
                    items_yielded += 1
                    yield item

                pagination_info = json_response.get("additional_data", {}).get("pagination", {})
                more_items = pagination_info.get("more_items_in_collection", False)
                if more_items:
                    next_start = pagination_info.get("next_start")
                    if next_start is not None:
                        start = next_start
                    else:
                        page_log.warning("Missing 'next_start' despite 'more_items_in_collection' being true.")
                        break
                else:
                    page_log.info("Pagination completed via stream.")
                    break

            except Exception as e:
                page_log.error("Error during V1 stream pagination", exc_info=True)
                break

        self.log.info("V1 stream fetch completed.", endpoint=endpoint_name, items_yielded=items_yielded, total_pages=page_num)

    def fetch_all_deals_stream(self, updated_since: str = None, items_limit: int = None) -> Generator[Dict, None, None]:
        """Busca deals (V2) com limite opcional usando paginação por cursor."""
        url = f"{self.BASE_URL_V2}/deals"
        params = {"sort_by": "update_time", "sort_direction": "asc"}
        if updated_since:
            params["updated_since"] = updated_since
        if items_limit:
            params["limit"] = min(items_limit, self.DEFAULT_V2_LIMIT)
        
        count = 0
        for deal in self._fetch_paginated_v2_stream(url, params):
            if items_limit and count >= items_limit:
                break
            count += 1
            yield deal


    def update_last_timestamp(self, new_timestamp: str):
        """Armazena o último timestamp processado no cache."""
        cache_key = "pipedrive:last_update_timestamp"; cache_ttl_seconds = 2592000 # 30 dias
        try: self.cache.set(cache_key, new_timestamp, ex_seconds=cache_ttl_seconds); self.log.info("Updated last update timestamp in cache", timestamp=new_timestamp)
        except Exception as e: self.log.error("Failed to store last update timestamp in cache", timestamp=new_timestamp, exc_info=True)
        
    def fetch_deal_changelog(self, deal_id: int) -> List[Dict]:
            """Busca o changelog para um deal específico (V1)."""
            if not deal_id or deal_id <= 0:
                self.log.warning("Invalid deal_id for changelog fetch", deal_id=deal_id)
                return []

            url = f"{self.BASE_URL_V1}/deals/{deal_id}/changelog"
            self.log.debug("Fetching deal changelog from API (V1)", deal_id=deal_id)
            try:
                changelog_data = self._fetch_paginated_v1(url)
                return changelog_data
            except Exception as e:
                self.log.error("Failed to fetch deal changelog", deal_id=deal_id, error=str(e))
                raise e