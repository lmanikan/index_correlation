from index_correlation.config.database_config import (
    BigQueryConfig,
    DatabaseConfig,
    PostgresConfig,
)
from index_correlation.config.results_config import ResultsStorageConfig
from index_correlation.storage.backends.bigquery_writer import BigQueryResultsWriter
from index_correlation.storage.backends.postgres_writer import PostgresResultsWriter
from index_correlation.storage.interface import ResultsWriter


def get_writer(
    db_config: DatabaseConfig,
    results_config: ResultsStorageConfig,
) -> ResultsWriter:
    """
    Factory for results writers.
    """
    if isinstance(db_config.current, PostgresConfig):
        from sqlalchemy import create_engine

        engine = create_engine(db_config.current.url)
        return PostgresResultsWriter(engine, config=results_config)
    elif isinstance(db_config.current, BigQueryConfig):
        from google.cloud import bigquery

        client = bigquery.Client(project=db_config.current.project)
        return BigQueryResultsWriter(client, config=results_config)
    else:
        raise ValueError(f"Unsupported database type: {type(db_config.current)}")
