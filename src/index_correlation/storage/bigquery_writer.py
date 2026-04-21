# BigQuery Results Writer Implementation

from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
from google.cloud.bigquery import SchemaField

from index_correlation.core.data_models import TrialResults
from index_correlation.storage.schemas import ALL_SCHEMAS
from index_correlation.connectors.results_writer import (
    ResultsWriter,
    WriterException,
    WriterConnectionError,
    WriterTableError,
    WriterWriteError,
    _should_write_daily_snapshot,
)
from index_correlation.config.results_config import ResultsStorageConfig, DEFAULT_RESULTS_STORAGE_CONFIG


class BigQueryResultsWriter(ResultsWriter):
    """BigQuery implementation of ResultsWriter using google-cloud-bigquery."""
    
    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        credentials_path: str | None,
        config: ResultsStorageConfig = DEFAULT_RESULTS_STORAGE_CONFIG,
    ):
        """
        Initialize BigQuery writer.
        
        Args:
            project_id: GCP project ID
            dataset_id: BigQuery dataset ID
            config: Storage configuration
        
        Note:
            Expects GOOGLE_APPLICATION_CREDENTIALS environment variable
            to point to service account JSON file.
        """
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.credentials_path = credentials_path
        self.config = config
        
        try:
            self.client = bigquery.Client(project=project_id)
        except Exception as e:
            raise WriterConnectionError(f"Failed to connect to BigQuery: {e}")
    
    def ensure_tables_exist(self) -> None:
        """Create tables if they don't exist."""
        try:
            # Ensure dataset exists
            dataset_id_full = f"{self.project_id}.{self.dataset_id}"
            dataset = bigquery.Dataset(dataset_id_full)
            dataset.location = "US"
            
            try:
                self.client.get_dataset(dataset_id_full)
            except Exception:
                dataset = self.client.create_dataset(dataset, timeout=30)
            
            # Create tables
            for schema in ALL_SCHEMAS:
                table_id = f"{self.project_id}.{self.dataset_id}.{schema.table_name}"
                schema_fields = [
                    SchemaField(name, dtype) for name, dtype in schema.schema_bigquery()
                ]
                table = bigquery.Table(table_id, schema=schema_fields)
                
                try:
                    self.client.get_table(table_id)
                except Exception:
                    self.client.create_table(table)
        
        except WriterException:
            raise
        except Exception as e:
            raise WriterTableError(f"Failed to create tables: {e}")
    
    def write_trial(self, trial: TrialResults, as_of: datetime) -> None:
        """
        Write trial results to BigQuery.
        
        Flow:
        1. Append correlations to intraday table
        2. Upsert latest sensitivities
        3. Check if snapshot time (index-specific), write daily if needed
        4. Clean up old intraday data
        """
        try:
            # Get DataFrames from trial
            corr_df = trial.to_dataframe()
            sens_df = trial.sensitivities_to_dataframe()
            
            # 1. Append intraday correlations
            if not corr_df.empty:
                self._write_intraday_correlations(corr_df, as_of)
            
            # 2. Upsert latest sensitivities
            if not sens_df.empty:
                self._upsert_latest_sensitivities(sens_df, as_of)
            
            # 3. Write daily snapshot if at snapshot time (index-specific)
            # Only writes ONCE per day per index
            if _should_write_daily_snapshot(
                trial.index.portfolio,
                as_of,
                self.config,
            ):
                if not corr_df.empty:
                    self._upsert_daily_correlations(corr_df, as_of)
            
            # 4. Clean up old intraday data
            self.cleanup_old_intraday(as_of)
        
        except WriterException:
            raise
        except Exception as e:
            raise WriterWriteError(f"Failed to write trial: {e}")
    
    def cleanup_old_intraday(self, as_of: datetime) -> int:
        """Delete intraday data older than retention period."""
        try:
            retention_days = self.config.correlation.five_min_retention_days
            cutoff_date = as_of - timedelta(days=retention_days)
            
            table_id = f"{self.project_id}.{self.dataset_id}.correlations_intraday"
            
            query = f"""
            DELETE FROM `{table_id}`
            WHERE as_of_datetime < TIMESTAMP('{cutoff_date.isoformat()}Z')
            """
            
            job: bigquery.QueryJob = self.client.query(query)
            result = job.result()  # type: ignore[assignment]
            
            # Handle None result safely
            if result is None:
                return 0
            
            rows_deleted = getattr(result, "total_rows", 0)
            return int(rows_deleted or 0)
        
        except Exception as e:
            raise WriterWriteError(f"Cleanup failed: {e}")
    
    def _write_intraday_correlations(
        self, df: pd.DataFrame, as_of: datetime
    ) -> None:
        """Append correlations to intraday table."""
        # Prepare data
        records = []
        for _, row in df.iterrows():
            records.append({
                "as_of_datetime": as_of,
                "index_name": row["index"],
                "term": row["term"],
                "strike": row["strike"],
                "implied_correlation": row["implied_correlation"],
                "index_volatility": row.get("index_volatility", 0),
                "num_components": row.get("num_components", 0),
            })
        
        if records:
            table_id = f"{self.project_id}.{self.dataset_id}.correlations_intraday"
            errors = self.client.insert_rows_json(table_id, records)
            
            if errors:
                raise WriterWriteError(f"Failed to insert intraday rows: {errors}")
    
    def _upsert_latest_sensitivities(
        self, df: pd.DataFrame, as_of: datetime
    ) -> None:
        """Upsert sensitivities to latest table."""
        # Prepare data
        records = []
        for _, row in df.iterrows():
            records.append({
                "index_name": row["index"],
                "term": row["term"],
                "strike": row["strike"],
                "symbol": row["symbol"],
                "delta": row.get("delta", 0),
                "elasticity": row.get("elasticity", 0),
                "sens_type": row.get("type", "component"),
                "as_of_datetime": as_of,
                "updated_at": datetime.utcnow(),
            })
        
        if records:
            # Use MERGE to upsert
            temp_table = f"{self.project_id}.{self.dataset_id}.temp_sensitivities_{int(as_of.timestamp())}"
            target_table = f"{self.project_id}.{self.dataset_id}.sensitivities_latest"
            
            # Load data to temp table
            temp_records_df = pd.DataFrame(records)
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
            )
            
            load_job = self.client.load_table_from_dataframe(
                temp_records_df, temp_table, job_config=job_config
            )
            load_job.result()
            
            # MERGE temp into target
            merge_query = f"""
            MERGE `{target_table}` T
            USING `{temp_table}` S
            ON T.index_name = S.index_name
              AND T.term = S.term
              AND T.strike = S.strike
              AND T.symbol = S.symbol
            WHEN MATCHED THEN
              UPDATE SET
                delta = S.delta,
                elasticity = S.elasticity,
                sens_type = S.sens_type,
                as_of_datetime = S.as_of_datetime,
                updated_at = S.updated_at
            WHEN NOT MATCHED THEN
              INSERT (
                index_name, term, strike, symbol,
                delta, elasticity, sens_type,
                as_of_datetime, updated_at
              )
              VALUES (
                S.index_name, S.term, S.strike, S.symbol,
                S.delta, S.elasticity, S.sens_type,
                S.as_of_datetime, S.updated_at
              )
            """
            
            merge_job = self.client.query(merge_query)
            merge_job.result()
            
            # Clean up temp table
            self.client.delete_table(temp_table, not_found_ok=True)
    
    def _upsert_daily_correlations(
        self, df: pd.DataFrame, as_of: datetime
    ) -> None:
        """Upsert correlations to daily snapshot table.
        
        IMPORTANT: Only writes ONCE per day per index.
        Checks if snapshot already exists for this (date, type, index) tuple,
        skips if already snapped today.
        """
        # Get snapshot config for this trial's index
        index_portfolio = df["index"].iloc[0]  # All rows have same index
        snapshot_config = self.config.correlation.get_snapshot_config(index_portfolio)
        snapshot_type = snapshot_config.snapshot_type
        snapshot_date = as_of.date()
        
        # Check if we already snapped for this index today
        check_query = f"""
        SELECT COUNT(*) as snap_count
        FROM `{self.project_id}.{self.dataset_id}.correlations_daily`
        WHERE snapshot_date = DATE('{snapshot_date.isoformat()}')
          AND snapshot_type = '{snapshot_type}'
          AND index_name = '{index_portfolio}'
        """
        
        result = self.client.query(check_query).result()
        snap_count = list(result)[0][0] if result else 0
        
        # If already snapped today, skip
        if snap_count > 0:
            return
        
        # Otherwise, write the snapshot
        records = []
        for _, row in df.iterrows():
            records.append({
                "snapshot_date": snapshot_date,
                "snapshot_type": snapshot_type,
                "index_name": row["index"],
                "term": row["term"],
                "strike": row["strike"],
                "implied_correlation": row["implied_correlation"],
                "index_volatility": row.get("index_volatility", 0),
                "num_components": row.get("num_components", 0),
                "as_of_datetime": as_of,
            })
        
        if records:
            # Use MERGE to upsert
            temp_table = f"{self.project_id}.{self.dataset_id}.temp_daily_{int(as_of.timestamp())}"
            target_table = f"{self.project_id}.{self.dataset_id}.correlations_daily"
            
            # Load data to temp table
            temp_records_df = pd.DataFrame(records)
            job_config = bigquery.LoadJobConfig(
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
            )
            
            load_job = self.client.load_table_from_dataframe(
                temp_records_df, temp_table, job_config=job_config
            )
            load_job.result()
            
            # MERGE temp into target
            merge_query = f"""
            MERGE `{target_table}` T
            USING `{temp_table}` S
            ON T.snapshot_date = S.snapshot_date
              AND T.snapshot_type = S.snapshot_type
              AND T.index_name = S.index_name
              AND T.term = S.term
              AND T.strike = S.strike
            WHEN MATCHED THEN
              UPDATE SET
                implied_correlation = S.implied_correlation,
                index_volatility = S.index_volatility,
                num_components = S.num_components,
                as_of_datetime = S.as_of_datetime
            WHEN NOT MATCHED THEN
              INSERT (
                snapshot_date, snapshot_type, index_name,
                term, strike, implied_correlation,
                index_volatility, num_components, as_of_datetime
              )
              VALUES (
                S.snapshot_date, S.snapshot_type, S.index_name,
                S.term, S.strike, S.implied_correlation,
                S.index_volatility, S.num_components, S.as_of_datetime
              )
            """
            
            merge_job = self.client.query(merge_query)
            merge_job.result()
            
            # Clean up temp table
            self.client.delete_table(temp_table, not_found_ok=True)
