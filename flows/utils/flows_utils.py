from datetime import datetime, timezone
import random
from typing import Any, Dict, List
import pandas as pd
import structlog
from prefect.utilities.hashing import hash_objects

from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository

log = structlog.get_logger(__name__)

def validate_loaded_data(
    repository: PipedriveRepository,
    source_data: List[Dict],
    batch_size: int
) -> Dict[str, Any]:
    """Validação completa dos dados carregados."""
    validation_log = structlog.get_logger().bind(batch_size=batch_size)
    issues = 0
    validated = 0
    
    try:
        # 1. Verificar contagem básica
        expected_count = len(source_data)
        actual_count = repository.count_records()
        count_match = expected_count == actual_count
        
        # 2. Verificar IDs ausentes
        source_ids = {str(item['id']) for item in source_data if 'id' in item}
        db_ids = set(repository.get_all_ids())
        
        missing_ids = source_ids - db_ids
        extra_ids = db_ids - source_ids
        
        # 3. Amostragem de validação detalhada
        sample_size = min(100, len(source_data))
        sample_records = random.sample(source_data, sample_size)
        detailed_issues = []
        
        for record in sample_records:
            db_data = repository.get_record_by_id(record['id'])
            if not db_data:
                detailed_issues.append(f"Missing record {record['id']}")
                continue
                
            # Verificar campos críticos
            critical_fields = ['value', 'currency', 'status', 'stage_name']
            for field in critical_fields:
                source_val = record.get(field)
                db_val = db_data.get(field)
                
                if source_val != db_val:
                    detailed_issues.append(
                        f"Field mismatch {field}: {source_val} vs {db_val}"
                    )
                    issues += 1
        
        # 4. Verificar consistência de datas
        date_issues = repository.validate_date_consistency()
        
        return {
            "data_quality_issues": issues + len(missing_ids) + len(extra_ids) + date_issues,
            "count_match": count_match,
            "missing_ids_count": len(missing_ids),
            "extra_ids_count": len(extra_ids),
            "detailed_issues_sample": detailed_issues[:5],
            "date_issues": date_issues
        }
        
    except Exception as e:
        validation_log.error("Data validation failed", error=str(e))
        return {"data_quality_issues": -1, "error": str(e)}

def calculate_optimal_batch_size(results: List[Dict]) -> int:
    """Calcula o tamanho ideal de batch com base nas métricas coletadas, ignorando falhas."""
    default_batch_size = 1000 

    if not results:
        print(f"WARN: No results provided, returning default batch size {default_batch_size}")
        return default_batch_size

    df = pd.DataFrame(results)

    valid_df = df[
        (df['data_quality_issues'] != -1) &
        df['duration'].notna() & (df['duration'] > 0) &
        df['memory_peak'].notna() & (df['memory_peak'] >= 0)
    ].copy() 

    if valid_df.empty:
        print(f"WARN: No valid results after filtering, returning default batch size {default_batch_size}")
        print("Original results head:\n", df.head())
        return default_batch_size

    # --- Normalização e Cálculo do Score (apenas em dados válidos) ---
    max_duration = valid_df['duration'].max()
    max_memory = valid_df['memory_peak'].max()
    max_quality_issues = valid_df['data_quality_issues'].max() 

    valid_df['norm_duration'] = valid_df['duration'] / max_duration if max_duration > 0 else 0
    valid_df['norm_memory'] = valid_df['memory_peak'] / max_memory if max_memory > 0 else 0

    if max_quality_issues > 0:
         valid_df['norm_quality'] = 1 - (valid_df['data_quality_issues'] / max_quality_issues)
    else:
         valid_df['norm_quality'] = 1.0 

    valid_df.fillna(0, inplace=True)

    weights = {
        'duration': 0.4,  # Menor duração é melhor (1 - norm_duration)
        'memory': 0.4,    # Menor memória é melhor (1 - norm_memory)
        'quality': 0.2    # Maior qualidade é melhor (norm_quality)
    }

    valid_df['score'] = (
        weights['duration'] * (1 - valid_df['norm_duration']) +
        weights['memory'] * (1 - valid_df['norm_memory']) +
        weights['quality'] * valid_df['norm_quality']
    )

    best_idx = valid_df['score'].idxmax()
    best = valid_df.loc[best_idx]

    optimal_size = int(best['batch_size'])
    print(f"INFO: Optimal batch size calculated: {optimal_size} based on score {best['score']:.3f}")
    print("INFO: Scores per batch size (valid runs):\n", valid_df[['batch_size', 'score', 'duration', 'memory_peak', 'data_quality_issues']])

    return optimal_size

def update_optimal_batch_config(repository: PipedriveRepository, optimal_size: int):
    """Atualiza a configuração do tamanho ótimo de batch no banco de dados."""
    config_key = "optimal_batch_size"
    logger = log.bind(config_key=config_key, optimal_size=optimal_size)
    try:
        now_iso = datetime.now(timezone.utc).isoformat()
        config_value = {'value': optimal_size, 'updated_at': now_iso }
        repository.save_configuration(key=config_key, value=config_value)
        logger.info("Optimal batch size configuration updated in DB.")
    except Exception as e:
        logger.error("Failed to update optimal batch size config in DB", exc_info=True)
        
def get_optimal_batch_size(repository: PipedriveRepository, default_size: int = 1000) -> int:
    """Busca o tamanho ótimo de batch da configuração do banco de dados."""
    config_key = "optimal_batch_size"
    logger = log.bind(config_key=config_key)
    try:
        config = repository.get_configuration(config_key)
        if config and isinstance(config.get("value"), int) and config["value"] > 0:
             size = int(config["value"])
             logger.info(f"Retrieved optimal batch size from config: {size}")
             return size
        else:
             logger.warning(f"Optimal batch size not found or invalid in config. Using default: {default_size}", config_value=config)
             return default_size
    except Exception as e:
        logger.error("Failed to get optimal batch size from config. Using default.", error=str(e), default_size=default_size, exc_info=True)
        return default_size
    
def backfill_cache_key_from_deal_ids(_, arguments: Dict[str, Any]) -> str:
    deal_ids = arguments.get("deal_ids", [])
    return hash_objects(sorted(deal_ids))
