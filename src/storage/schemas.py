# Table Schemas for Results Storage

from abc import ABC, abstractmethod
from typing import Dict, List, Tuple


class TableSchema(ABC):
    """Abstract base for table schema definitions."""
    
    @property
    @abstractmethod
    def table_name(self) -> str:
        """Table name in database."""
        pass
    
    @property
    @abstractmethod
    def primary_key_columns(self) -> List[str]:
        """Primary key column names."""
        pass
    
    @property
    @abstractmethod
    def columns(self) -> Dict[str, str]:
        """Column definitions: {name: sql_type}."""
        pass
    
    @property
    @abstractmethod
    def indexes(self) -> List[Tuple[str, List[str]]]:
        """Index definitions: [(index_name, [columns])]."""
        pass
    
    def create_table_sql_postgresql(self) -> str:
        """Generate PostgreSQL CREATE TABLE statement."""
        col_defs = []
        for col_name, col_type in self.columns.items():
            col_defs.append(f"  {col_name} {col_type}")
        
        pk_str = ", ".join(self.primary_key_columns)
        col_defs.append(f"  PRIMARY KEY ({pk_str})")
        
        return f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
{',\\n'.join(col_defs)}
)"""
    
    def create_table_sql_bigquery(self) -> str:
        """Generate BigQuery CREATE TABLE statement."""
        col_defs = []
        for col_name, col_type in self.columns.items():
            bq_type = self._postgres_to_bigquery_type(col_type)
            col_defs.append(f"  {col_name} {bq_type}")
        
        pk_str = ", ".join(self.primary_key_columns)
        col_defs.append(f"  -- PRIMARY KEY: {pk_str}")
        
        return f"""CREATE TABLE IF NOT EXISTS {self.table_name} (
{',\\n'.join(col_defs)}
)"""
    
    @staticmethod
    def _postgres_to_bigquery_type(pg_type: str) -> str:
        """Convert PostgreSQL type to BigQuery type."""
        mapping = {
            "TIMESTAMP": "TIMESTAMP",
            "DATE": "DATE",
            "VARCHAR(255)": "STRING",
            "VARCHAR": "STRING",
            "FLOAT8": "FLOAT64",
            "INT": "INT64",
            "BOOLEAN": "BOOL",
        }
        return mapping.get(pg_type, "STRING")


class CorrelationIntradaySchema(TableSchema):
    """Schema for 5-minute correlation intraday data."""
    
    @property
    def table_name(self) -> str:
        return "correlations_intraday"
    
    @property
    def primary_key_columns(self) -> List[str]:
        return ["as_of_datetime", "index_name", "term", "strike"]
    
    @property
    def columns(self) -> Dict[str, str]:
        return {
            "as_of_datetime": "TIMESTAMP NOT NULL",
            "index_name": "VARCHAR(255) NOT NULL",
            "term": "VARCHAR(10) NOT NULL",
            "strike": "FLOAT8 NOT NULL",
            "implied_correlation": "FLOAT8 NOT NULL",
            "index_volatility": "FLOAT8 NOT NULL",
            "num_components": "INT NOT NULL",
            "weight_type": "VARCHAR(50)",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }
    
    @property
    def indexes(self) -> List[Tuple[str, List[str]]]:
        return [
            ("idx_corr_intraday_datetime", ["as_of_datetime"]),
            ("idx_corr_intraday_index", ["index_name"]),
        ]


class CorrelationDailySchema(TableSchema):
    """Schema for daily correlation snapshots."""
    
    @property
    def table_name(self) -> str:
        return "correlations_daily"
    
    @property
    def primary_key_columns(self) -> List[str]:
        return ["snapshot_date", "snapshot_type", "index_name", "term", "strike"]
    
    @property
    def columns(self) -> Dict[str, str]:
        return {
            "snapshot_date": "DATE NOT NULL",
            "snapshot_type": "VARCHAR(50) NOT NULL DEFAULT 'close'",
            "index_name": "VARCHAR(255) NOT NULL",
            "term": "VARCHAR(10) NOT NULL",
            "strike": "FLOAT8 NOT NULL",
            "implied_correlation": "FLOAT8 NOT NULL",
            "index_volatility": "FLOAT8 NOT NULL",
            "num_components": "INT NOT NULL",
            "as_of_datetime": "TIMESTAMP NOT NULL",
            "weight_type": "VARCHAR(50)",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }
    
    @property
    def indexes(self) -> List[Tuple[str, List[str]]]:
        return [
            ("idx_corr_daily_date", ["snapshot_date"]),
            ("idx_corr_daily_index", ["index_name"]),
        ]


class SensitivityLatestSchema(TableSchema):
    """Schema for latest sensitivities (no history)."""
    
    @property
    def table_name(self) -> str:
        return "sensitivities_latest"
    
    @property
    def primary_key_columns(self) -> List[str]:
        return ["index_name", "term", "strike", "symbol"]
    
    @property
    def columns(self) -> Dict[str, str]:
        return {
            "index_name": "VARCHAR(255) NOT NULL",
            "term": "VARCHAR(10) NOT NULL",
            "strike": "FLOAT8 NOT NULL",
            "symbol": "VARCHAR(255) NOT NULL",
            "delta": "FLOAT8 NOT NULL",
            "elasticity": "FLOAT8 NOT NULL",
            "sens_type": "VARCHAR(50) NOT NULL",
            "as_of_datetime": "TIMESTAMP NOT NULL",
            "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
            "updated_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
        }
    
    @property
    def indexes(self) -> List[Tuple[str, List[str]]]:
        return [
            ("idx_sens_latest_index", ["index_name"]),
            ("idx_sens_latest_datetime", ["as_of_datetime"]),
        ]


# Registry of all schemas
ALL_SCHEMAS = [
    CorrelationIntradaySchema(),
    CorrelationDailySchema(),
    SensitivityLatestSchema(),
]
