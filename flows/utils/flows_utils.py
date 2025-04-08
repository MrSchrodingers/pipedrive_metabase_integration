from datetime import datetime, timezone
import random
from typing import Any, Dict, List
import pandas as pd
import structlog

from infrastructure.repository_impl.pipedrive_repository import PipedriveRepository

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
    """Calcula o tamanho ideal de batch com base nas métricas coletadas."""
    if not results:
        return 1000  # Valor padrão
    
    df = pd.DataFrame(results)
    
    # Normalizar métricas
    df['norm_duration'] = df['duration'] / df['duration'].max()
    df['norm_memory'] = df['memory_peak'] / df['memory_peak'].max()
    df['norm_quality'] = 1 - (df['data_quality_issues'] / df['data_quality_issues'].max())
    
    # Calcular score composto
    weights = {
        'duration': 0.4,
        'memory': 0.4,
        'quality': 0.2
    }
    
    df['score'] = (
        weights['duration'] * (1 - df['norm_duration']) +
        weights['memory'] * (1 - df['norm_memory']) +
        weights['quality'] * df['norm_quality']
    )
    
    # Selecionar melhor score
    best = df.loc[df['score'].idxmax()]
    return int(best['batch_size'])

def update_dynamic_batch_config(repository: PipedriveRepository, optimal_size: int):
    """Atualiza a configuração dinâmica no banco de dados."""
    try:
        repository.save_configuration(
            key='optimal_batch_size',
            value={'value': optimal_size, 'updated_at': datetime.now(timezone.utc)}
        )
    except Exception as e:
        structlog.get_logger().error("Failed to update batch config", error=str(e))