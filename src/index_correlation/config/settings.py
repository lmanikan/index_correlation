"""Configuration management"""
import os
from dataclasses import dataclass

@dataclass
class DatabaseConfig:
    """Database connection settings"""
    host: str = os.getenv("DB_HOST", "localhost")
    port: int = int(os.getenv("DB_PORT", "5432"))
    database: str = os.getenv("DB_NAME", "market_data")
    user: str = os.getenv("DB_USER", "postgres")
    password: str = os.getenv("DB_PASSWORD", "")

@dataclass
class PipelineSettings:
    """Pipeline configuration"""
    source_db: DatabaseConfig | None = None
    dest_db: DatabaseConfig | None = None
    batch_size: int = int(os.getenv("BATCH_SIZE", "1000"))
    min_components: int = int(os.getenv("MIN_COMPONENTS", "20"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    def __post_init__(self):
        if self.source_db is None:
            self.source_db = DatabaseConfig()
        if self.dest_db is None:
            self.dest_db = DatabaseConfig()
