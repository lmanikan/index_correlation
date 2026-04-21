from dataclasses import dataclass
from typing import Literal
from pathlib import Path
import yaml
import logging

logger = logging.getLogger(__name__)


@dataclass
class PostgresConfig:
    type: Literal["postgres"]
    url: str
    pool_pre_ping: bool = True
    pool_size: int = 5
    max_overflow: int = 10


@dataclass
class BigQueryConfig:
    type: Literal["bigquery"]
    project_id: str
    dataset: str
    credentials_path: str | None = None


DatabaseConfig = PostgresConfig | BigQueryConfig


def load_database_config(config_file: str | Path = "config/database.yaml") -> dict[str, DatabaseConfig]:
    """Load all database configs from YAML."""
    config_file = Path(config_file)
    
    if not config_file.exists():
        raise FileNotFoundError(f"Database config not found: {config_file}")
    
    data = yaml.safe_load(config_file.read_text())
    
    configs = {}
    for name, cfg in data.items():
        if name == "default":
            continue
        
        db_type = cfg.get("type")
        
        if db_type == "postgres":
            configs[name] = PostgresConfig(**cfg)
        elif db_type == "bigquery":
            configs[name] = BigQueryConfig(**cfg)
        else:
            raise ValueError(f"Unknown database type: {db_type}")
    
    logger.info(f"Loaded {len(configs)} database configs from {config_file}")
    return configs


def get_database_config(
    name: str | None = None,
    config_file: str | Path = "config/database.yaml"
) -> DatabaseConfig:
    """Get a specific database config, or default."""
    configs = load_database_config(config_file)
    
    # Get default if not specified
    if name is None:
        data = yaml.safe_load(Path(config_file).read_text())
        name = data.get("default", "postgres")
    
    if name not in configs:
        raise ValueError(f"Database config not found: {name}")
    
    return configs[name]
