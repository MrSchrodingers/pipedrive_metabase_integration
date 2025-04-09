import time
import requests
import structlog
import re
import math
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
    pipedrive_api_token_cost_total
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
    PERSON_BATCH_SIZE = 100
    CHANGELOG_PAGE_LIMIT = 500
    
    # TTL para mapas e lookups individuais
    DEFAULT_MAP_CACHE_TTL_SECONDS = 300 * 12  # 12 hours
    PERSON_LOOKUP_CACHE_TTL_SECONDS = 300 * 1  # 1 hour
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
    
    DEFAULT_ENDPOINT_COST = 10  # Custo padrão para rotas não mapeadas
    
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

            duration = time.monotonic() - start_time
            api_request_duration_hist.labels(
                endpoint=normalized_endpoint_label, 
                method='GET', 
                status_code=status_code
            ).observe(duration)

            request_log.debug("API GET request successful", status_code=status_code, duration_sec=f"{duration:.3f}s")
            return response

        except requests.exceptions.Timeout as e:
            error_type = "timeout"
            request_log.warning("API request timed out", error=str(e))
            raise
        except requests.exceptions.ConnectionError as e:
            error_type = "connection_error"
            request_log.warning("API connection error", error=str(e))
            raise
        except requests.exceptions.RequestException as e:
            error_type = "request_exception"
            request_log.error("API request failed (RequestException)", error=str(e), exc_info=True)

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
            except Exception as e: page_log.error("Error during V1 fetching page", error=str(e), exc_info=True); break 
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
            except Exception as e: page_log.error("Error during V2 fetching page", error=str(e), exc_info=True); break 
        self.log.info(f"V2 Paginated fetch complete.", endpoint=normalized_endpoint_label, total_items=len(all_data), total_pages=page_num)
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
        
    def fetch_all_stages_details(self) -> List[Dict]:
        """Busca detalhes de todos os stages (necessário para nomes normalizados)."""
        cache_key = "pipedrive:all_stages_details"
        cached = self.cache.get(cache_key)
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
            self.log.error("Failed to fetch/process stage details", error=str(e), exc_info=True)
            return []
        
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
        url = f"{self.BASE_URL_V2}/persons/{person_id}"
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
            elif response.status_code == 429:
                 person_log.warning("Rate limit reached in Pipedrive API (429).")
                 raise Exception("Rate limit")
            else:
                 person_log.warning(f"Failed to fetch person name from API, status code: {response.status_code}.")
                 return None

        except Exception as e:
            person_log.error("Unexpected error fetching person name from API", error=str(e), exc_info=True)
            return None


    def fetch_person_names_for_ids(self, person_ids: Set[int]) -> Dict[int, str]:
        """
        Busca nomes para um conjunto de IDs de persons, usando cache e API em lote V2.
        Retorna um dicionário apenas com os IDs encontrados (no cache ou API).
        IDs não encontrados (seja por cache miss seguido de 404 ou falha na API)
        serão cacheados como '' mas não incluídos no dict retornado.
        """
        if not person_ids:
            return {}

        # Filtrar IDs inválidos
        valid_person_ids = {p_id for p_id in person_ids if p_id > 0}
        if not valid_person_ids:
            self.log.warning("No valid person IDs provided after filtering.", original_count=len(person_ids))
            return {}

        names_map: Dict[int, str] = {}
        ids_to_fetch_from_api: Set[int] = set()
        fetch_log = self.log.bind(total_ids_requested=len(valid_person_ids))
        fetch_log.debug("Starting fetch for multiple person names (batch V2 strategy).")

        # 1. Tentar buscar do cache
        cache_check_start_time = time.monotonic()
        cached_count = 0
        cache_error_count = 0
        for p_id in valid_person_ids:
            cache_key = f"pipedrive:person_name:{p_id}"
            try:
                cached_value = self.cache.get(cache_key)
                if cached_value is not None: 
                    if cached_value: 
                        names_map[p_id] = str(cached_value)
                    cached_count += 1
                    self.log.debug("Person name cache hit.", person_id=p_id, cached_value=cached_value)
                else:
                    ids_to_fetch_from_api.add(p_id)
            except Exception as cache_err:
                self.log.error("Cache GET error during batch person lookup", person_id=p_id, error=str(cache_err))
                ids_to_fetch_from_api.add(p_id)
                cache_error_count += 1

        cache_check_duration = time.monotonic() - cache_check_start_time
        fetch_log.info(
            "Person names cache check completed.",
            cache_hits=cached_count,
            cache_misses=len(ids_to_fetch_from_api),
            cache_errors=cache_error_count,
            duration_sec=f"{cache_check_duration:.3f}s"
        )

        # 2. Buscar IDs restantes da API em lotes com paginação
        if ids_to_fetch_from_api:
            api_fetch_start_time = time.monotonic()
            api_found_count = 0
            api_not_found_count = 0
            api_batch_error_count = 0
            processed_in_api_count = 0

            list_ids_to_fetch = sorted(list(ids_to_fetch_from_api)) 
            total_batches = math.ceil(len(list_ids_to_fetch) / self.PERSON_BATCH_SIZE)
            fetch_log.info(f"Fetching {len(list_ids_to_fetch)} person names from API in {total_batches} batches.")

            for i in range(0, len(list_ids_to_fetch), self.PERSON_BATCH_SIZE):
                current_batch_ids_list = list_ids_to_fetch[i:i + self.PERSON_BATCH_SIZE]
                current_batch_ids_set = set(current_batch_ids_list)
                processed_in_api_count += len(current_batch_ids_list)
                batch_num = (i // self.PERSON_BATCH_SIZE) + 1
                batch_log = fetch_log.bind(batch_num=batch_num, batch_size=len(current_batch_ids_list), total_batches=total_batches)

                id_string = ",".join(map(str, current_batch_ids_list))
                url = f"{self.BASE_URL_V2}/persons"
                params = {"ids": id_string, "limit": len(current_batch_ids_list)}

                all_persons_in_batch = []
                next_cursor = None

                # Novo: Loop de paginação preservando parâmetros originais
                while True:
                    current_params = params.copy()
                    if next_cursor:
                        current_params["cursor"] = next_cursor

                    try:
                        response = self._get(url, params=current_params)
                        json_response = response.json()
                        
                        if not json_response.get("success"):
                            batch_log.warning("API response indicates failure", response_preview=str(json_response)[:200])
                            break

                        current_data = json_response.get("data", [])
                        all_persons_in_batch.extend(current_data)

                        # Atualizar cursor para próxima página
                        additional_data = json_response.get("additional_data", {}); next_cursor = additional_data.get("next_cursor")
                        if not next_cursor:
                            break

                    except Exception as api_err:
                        batch_log.error("API request failed", error=str(api_err))
                        break

                # Processar todos os dados coletados (todas as páginas)
                returned_ids_set = set()
                if all_persons_in_batch:
                    for person_data in all_persons_in_batch:
                        p_id = person_data.get("id")
                        name = person_data.get("name")
                        if isinstance(p_id, int):
                            returned_ids_set.add(p_id)
                            person_name_str = str(name) if name else ''
                            if person_name_str:
                                names_map[p_id] = person_name_str
                                self.cache.set(f"pipedrive:person_name:{p_id}", person_name_str, ex_seconds=self.PERSON_LOOKUP_CACHE_TTL_SECONDS)
                                api_found_count += 1
                            else:
                                self.cache.set(f"pipedrive:person_name:{p_id}", '', ex_seconds=self.PERSON_LOOKUP_CACHE_TTL_SECONDS)
                                batch_log.debug("Person name empty in API response", person_id=p_id)

                # Verificar IDs ausentes
                missing_in_response_ids = current_batch_ids_set - returned_ids_set
                if missing_in_response_ids:
                    batch_log.warning("Missing IDs in API response", missing_count=len(missing_in_response_ids))
                    for missing_id in missing_in_response_ids:
                        self.cache.set(f"pipedrive:person_name:{missing_id}", '', ex_seconds=self.PERSON_LOOKUP_CACHE_TTL_SECONDS)
                        api_not_found_count += 1

            api_fetch_duration = time.monotonic() - api_fetch_start_time
            fetch_log.info(
                "Person names API batch fetch completed.",
                api_ids_processed=processed_in_api_count,
                api_found_successfully=api_found_count,
                api_not_found_or_empty=api_not_found_count,
                api_batch_errors=api_batch_error_count,
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
        except Exception as e: self.log.error("Failed to store last update timestamp in cache", timestamp=new_timestamp, error=str(e), exc_info=True)