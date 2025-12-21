from src.config.database_config import DatabaseConfig, PostgresConfig, BigQueryConfig
from src.config.results_config import ResultsStorageConfig
from src.connectors.results_writer import ResultsWriter


def get_writer(
    db_config: DatabaseConfig,
    results_config: ResultsStorageConfig,
) -> ResultsWriter:
    """
    Factory to instantiate the appropriate writer based on database config.
    
    Args:
        db_config: PostgresConfig or BigQueryConfig
        results_config: ResultsStorageConfig (correlation/sensitivity settings)
    
    Returns:
        ResultsWriter instance (PostgresResultsWriter or BigQueryResultsWriter)
    """
    
    if isinstance(db_config, PostgresConfig):
        from sqlalchemy import create_engine
        from src.storage.postgres_writer import PostgresResultsWriter
        
        engine = create_engine(
            db_config.url,
            pool_pre_ping=db_config.pool_pre_ping,
            pool_size=db_config.pool_size,
            max_overflow=db_config.max_overflow,
        )
        return PostgresResultsWriter(engine, config=results_config)
    
    elif isinstance(db_config, BigQueryConfig):
        from src.storage.bigquery_writer import BigQueryResultsWriter
        
        return BigQueryResultsWriter(
            project_id=db_config.project_id,
            dataset_id=db_config.dataset,
            credentials_path=db_config.credentials_path,
            config=results_config,
        )
    
    else:
        raise ValueError(f"Unknown database config type: {type(db_config)}")
