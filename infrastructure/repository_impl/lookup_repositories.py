from typing import List, Dict, Any

from .base_lookup_repository import BaseLookupRepository, UNKNOWN_NAME
from application.utils.column_utils import normalize_column_name


# --- Constants for Table Names ---
LOOKUP_TABLE_USERS = "pipedrive_users"
LOOKUP_TABLE_PERSONS = "pipedrive_persons"
LOOKUP_TABLE_STAGES = "pipedrive_stages"
LOOKUP_TABLE_PIPELINES = "pipedrive_pipelines"
LOOKUP_TABLE_ORGANIZATIONS = "pipedrive_organizations"


# --- User Repository ---
class UserRepository(BaseLookupRepository):
    TABLE_NAME = LOOKUP_TABLE_USERS
    ID_COLUMN = "user_id"
    NAME_COLUMN = "user_name"
    COLUMNS = {
        "user_id": "INTEGER PRIMARY KEY",
        "user_name": "TEXT",
        "is_active": "BOOLEAN",
        "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
    }
    INDEXES = {
        f"idx_{TABLE_NAME}_name": "user_name text_pattern_ops" 
    }

    def upsert_users(self, raw_api_data: List[Dict[str, Any]]):
        """Maps raw Pipedrive User API data and upserts into the database."""
        mapped_data = []
        cols_to_insert = ["user_id", "user_name", "is_active"] 
        for r in raw_api_data:
            if 'id' in r:
                mapped_data.append({
                    'user_id': r['id'],
                    'user_name': r.get('name', UNKNOWN_NAME),
                    # Check API response for correct active flag key (e.g., 'active_flag')
                    'is_active': r.get('active_flag', True)
                })
            else:
                self.log.warning("User record missing 'id' in raw API data", record_preview=str(r)[:100])

        return self._upsert_lookup_data(mapped_data, cols_to_insert)


# --- Person Repository ---
class PersonRepository(BaseLookupRepository):
    TABLE_NAME = LOOKUP_TABLE_PERSONS
    ID_COLUMN = "person_id"
    NAME_COLUMN = "person_name"
    COLUMNS = {
        "person_id": "INTEGER PRIMARY KEY",
        "person_name": "TEXT",
        "org_id": "INTEGER", 
        "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
    }
    INDEXES = {
        f"idx_{TABLE_NAME}_name": "person_name text_pattern_ops",
        f"idx_{TABLE_NAME}_org_id": "org_id"
    }

    def upsert_persons(self, raw_api_data: List[Dict[str, Any]]):
        """Maps raw Pipedrive Person API data and upserts into the database."""
        mapped_data = []
        cols_to_insert = ["person_id", "person_name", "org_id"]
        for r in raw_api_data:
            if 'id' in r:
                org_id_value = None
                raw_org_id = r.get('org_id')
                if isinstance(raw_org_id, dict):
                    org_id_value = raw_org_id.get('value')
                elif isinstance(raw_org_id, int):
                     org_id_value = raw_org_id

                try:
                    org_id_int = int(org_id_value) if org_id_value is not None else None
                except (ValueError, TypeError):
                     self.log.warning("Could not parse org_id for person", person_id=r['id'], raw_org_id=raw_org_id)
                     org_id_int = None

                mapped_data.append({
                    'person_id': r['id'],
                    'person_name': r.get('name', UNKNOWN_NAME),
                    'org_id': org_id_int
                })
            else:
                self.log.warning("Person record missing 'id' in raw API data", record_preview=str(r)[:100])

        return self._upsert_lookup_data(mapped_data, cols_to_insert)


# --- Stage Repository ---
class StageRepository(BaseLookupRepository):
    TABLE_NAME = LOOKUP_TABLE_STAGES
    ID_COLUMN = "stage_id"
    NAME_COLUMN = "stage_name" 
    COLUMNS = {
        "stage_id": "INTEGER PRIMARY KEY",
        "stage_name": "TEXT",
        "normalized_name": "TEXT", 
        "pipeline_id": "INTEGER",
        "order_nr": "INTEGER",
        "is_active": "BOOLEAN",
        "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
    }
    INDEXES = {
        f"idx_{TABLE_NAME}_norm_name": "normalized_name",
        f"idx_{TABLE_NAME}_pipeline_id": "pipeline_id"
    }

    def upsert_stages(self, raw_api_data: List[Dict[str, Any]]):
        """Maps raw Pipedrive Stage API data and upserts into the database."""
        mapped_data = []
        cols_to_insert = ["stage_id", "stage_name", "normalized_name", "pipeline_id", "order_nr", "is_active"]
        for r in raw_api_data:
            if 'id' in r:
                stage_name = r.get('name', UNKNOWN_NAME)
                normalized = normalize_column_name(stage_name) if stage_name != UNKNOWN_NAME else None
                if not normalized or normalized == "_invalid_normalized_name":
                    self.log.warning("Could not normalize stage name", stage_id=r['id'], original_name=stage_name)
                    normalized = f"invalid_stage_{r['id']}"

                mapped_data.append({
                    'stage_id': r['id'],
                    'stage_name': stage_name,
                    'normalized_name': normalized,
                    'pipeline_id': r.get('pipeline_id'),
                    'order_nr': r.get('order_nr'),
                    'is_active': not r.get('is_deleted', False)
                })
            else:
                self.log.warning("Stage record missing 'id' in raw API data", record_preview=str(r)[:100])

        return self._upsert_lookup_data(mapped_data, cols_to_insert)


# --- Pipeline Repository ---
class PipelineRepository(BaseLookupRepository):
    TABLE_NAME = LOOKUP_TABLE_PIPELINES
    ID_COLUMN = "pipeline_id"
    NAME_COLUMN = "pipeline_name"
    COLUMNS = {
        "pipeline_id": "INTEGER PRIMARY KEY",
        "pipeline_name": "TEXT",
        "is_active": "BOOLEAN",
        "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
    }
    INDEXES = {
         f"idx_{TABLE_NAME}_name": "pipeline_name text_pattern_ops"
    }

    def upsert_pipelines(self, raw_api_data: List[Dict[str, Any]]):
        """Maps raw Pipedrive Pipeline API data and upserts into the database."""
        mapped_data = []
        cols_to_insert = ["pipeline_id", "pipeline_name", "is_active"]
        for r in raw_api_data:
            if 'id' in r:
                mapped_data.append({
                    'pipeline_id': r['id'],
                    'pipeline_name': r.get('name', UNKNOWN_NAME),
                    'is_active': not r.get('is_deleted', False)
                })
            else:
                 self.log.warning("Pipeline record missing 'id' in raw API data", record_preview=str(r)[:100])

        return self._upsert_lookup_data(mapped_data, cols_to_insert)


# --- Organization Repository ---
class OrganizationRepository(BaseLookupRepository):
    TABLE_NAME = LOOKUP_TABLE_ORGANIZATIONS
    ID_COLUMN = "org_id"
    NAME_COLUMN = "org_name"
    COLUMNS = {
        "org_id": "INTEGER PRIMARY KEY",
        "org_name": "TEXT",
        "last_synced_at": "TIMESTAMPTZ DEFAULT NOW()"
        # Add other relevant org fields here if needed later (e.g., address, owner_id)
        # "address": "TEXT",
        # "owner_user_id": "INTEGER",
    }
    INDEXES = {
         f"idx_{TABLE_NAME}_name": "org_name text_pattern_ops"
    }

    def upsert_organizations(self, raw_api_data: List[Dict[str, Any]]):
        """Maps raw Pipedrive Organization API data and upserts into the database."""
        mapped_data = []
        cols_to_insert = ["org_id", "org_name"]
        for r in raw_api_data:
             if 'id' in r:
                 mapped_data.append({
                     'org_id': r['id'],
                     'org_name': r.get('name', UNKNOWN_NAME)
                     # Map other fields here if added to COLUMNS
                     # 'address': r.get('address'),
                     # 'owner_user_id': r.get('owner_id') # API might use 'owner_id' for user ID
                 })
             else:
                  self.log.warning("Organization record missing 'id' in raw API data", record_preview=str(r)[:100])

        return self._upsert_lookup_data(mapped_data, cols_to_insert)