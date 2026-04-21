# Correlations Pipeline – Detailed Technical Documentation

## Table of Contents
1. [System Architecture](#system-architecture)
2. [Data Model](#data-model)
3. [Configuration](#configuration)
4. [Component Details](#component-details)
5. [Operational Modes](#operational-modes)
6. [Implementation Guide](#implementation-guide)
7. [Design Principles](#design-principles)

---

## System Architecture

### Three-Layer Pipeline

```
┌─────────────────────────────────────────────────────────────┐
│  LAYER 1: INPUT EXTRACTION                                  │
│  Market DB → MarketDataExtractor → DataPackage             │
│  [TO BUILD]                                                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 2: COMPUTE                                           │
│  DataPackage → AnalyticsEngine.compute_all_terms()         │
│              → TrialResults                                 │
│  [EXTERNAL]                                                 │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  LAYER 3: LOAD + RESULTS                                    │
│  TrialResults → ResultsWriter → Analytics DB               │
│  Analytics DB → CorrelationsReader → Consumers             │
│  [BUILT]                                                    │
└─────────────────────────────────────────────────────────────┘
```

### Why This Design?

**Separation of Concerns:**
- **Extraction**: Decouples analytics from raw data schema
- **Compute**: Decouples pipeline from business logic
- **Storage**: Decouples write mechanics from semantics
- **Retrieval**: Decouples consumer needs from table structure

**Benefits:**
- Each layer evolves independently
- Database can change without touching compute
- Compute can change without touching extraction
- Easy to test each layer in isolation
- Enables horizontal scaling per layer

---

## Data Model

### Index

**File:** `src/config/indices_config.py`

**Structure:**
```python
from dataclasses import dataclass
from datetime import time

@dataclass
class Index:
    portfolio: str              # e.g., "SPX"
    symbol: str                 # e.g., "SPX"
    name: str                   # Auto-generated: f"{portfolio}_{symbol}"
    close_time_utc: time        # Market close in UTC, e.g., 21:00
    close_time_tolerance_minutes: int  # Window around close, e.g., 30
    description: str            # e.g., "S&P 500 Index"
```

**Example Configuration:**
```python
INDICES_CONFIG = [
    {
        "portfolio": "SPX",
        "symbol": "SPX",
        "close_time_utc": "21:00",
        "close_time_tolerance_minutes": 30,
        "description": "S&P 500 Index"
    },
    {
        "portfolio": "DAX",
        "symbol": "DAX",
        "close_time_utc": "16:30",
        "close_time_tolerance_minutes": 30,
        "description": "DAX Index"
    },
]
```

**Auto-Generated Lookups:**
```python
INDICES_BY_PORTFOLIO = {
    "SPX": Index(...),
    "DAX": Index(...),
}

INDICES_BY_SYMBOL = {
    "SPX": Index(...),
    "DAX": Index(...),
}

INDICES_BY_NAME = {
    "SPX_SPX": Index(...),
    "DAX_DAX": Index(...),
}
```

**Usage in Code:**
```python
# Lookup by any key
idx = INDICES_BY_NAME["SPX_SPX"]
print(idx.close_time_utc)  # "21:00"
```

---

### DataPackage (TO BUILD)

**File:** `src/extraction/data_package.py`

**Definition:**
```python
from dataclasses import dataclass
from datetime import datetime
from typing import Optional
import pandas as pd
from src.config.indices_config import Index

@dataclass
class DataPackage:
    """
    Immutable container for all data needed by AnalyticsEngine.
    
    Contract with AnalyticsEngine:
    - index: Index object with portfolio, symbol, name
    - as_of: Timestamp marking data freshness
    - ivol_surface: Index implied vol surface (rows per term/strike)
    - svol_surface: Single-name vols for index constituents
    - weights: Index weights (e.g., SPX holding weights)
    - returns: (Optional) Historical returns for correlation computation
    """
    
    index: Index
    as_of: datetime
    ivol_surface: pd.DataFrame      # Columns: term, strike, vol, ...
    svol_surface: pd.DataFrame      # Columns: symbol, term, strike, vol, ...
    weights: pd.DataFrame           # Columns: symbol, weight, ...
    returns: Optional[pd.DataFrame] = None  # Columns: symbol, date, return, ...
    
    def __post_init__(self):
        """Validate structure matches AnalyticsEngine expectations."""
        required_ivol_cols = {"term", "strike", "vol"}
        assert required_ivol_cols.issubset(self.ivol_surface.columns), \
            f"ivol_surface missing columns: {required_ivol_cols}"
        
        required_svol_cols = {"symbol", "term", "strike", "vol"}
        assert required_svol_cols.issubset(self.svol_surface.columns), \
            f"svol_surface missing columns: {required_svol_cols}"
        
        required_weight_cols = {"symbol", "weight"}
        assert required_weight_cols.issubset(self.weights.columns), \
            f"weights missing columns: {required_weight_cols}"
```

**Key Invariants:**
- `as_of` should be close to current time (or historical snapshot time for backfill)
- `ivol_surface` must have enough term/strike grid for analytics engine
- `svol_surface` must cover all constituents in `weights`
- `weights` must sum to ~1.0 (checked by validation)

---

### TrialResults

**Contract:** Output from `AnalyticsEngine.compute_all_terms(DataPackage)`

**Expected Structure (align with your AnalyticsEngine):**
```python
@dataclass
class TrialResults:
    index: Index
    as_of: datetime
    
    # Correlations and volatilities
    correlations: pd.DataFrame  # Columns: term, strike, implied_correlation, index_volatility, num_components
    
    # Sensitivities (risk decomposition)
    sensitivities: pd.DataFrame  # Columns: term, strike, symbol, delta, elasticity, sens_type, ...
```

---

## Configuration

### 1. Index Configuration

**File:** `src/config/indices_config.py`

**Responsibility:** Define all indices the pipeline will process.

```python
INDICES_CONFIG = [
    {
        "portfolio": "SPX",
        "symbol": "SPX",
        "close_time_utc": "21:00",
        "close_time_tolerance_minutes": 30,
        "description": "S&P 500 Index"
    },
    {
        "portfolio": "NIFTY",
        "symbol": "NIFTY",
        "close_time_utc": "09:00",
        "close_time_tolerance_minutes": 15,
        "description": "NIFTY 50 Index (India)"
    },
]

# Generated automatically:
def _generate_name(portfolio: str, symbol: str) -> str:
    return f"{portfolio}_{symbol}"

# Populate lookups
INDICES_BY_NAME = {}
INDICES_BY_PORTFOLIO = {}
INDICES_BY_SYMBOL = {}

for config in INDICES_CONFIG:
    idx = Index(**config, name=_generate_name(config["portfolio"], config["symbol"]))
    INDICES_BY_NAME[idx.name] = idx
    INDICES_BY_PORTFOLIO.setdefault(idx.portfolio, []).append(idx)
    INDICES_BY_SYMBOL.setdefault(idx.symbol, []).append(idx)
```

**Key Fields:**
- `portfolio`: Portfolio identifier (e.g., "SPX", "DAX")
- `symbol`: Symbol identifier (e.g., "SPX", "DAX")
- `close_time_utc`: Market close time in UTC, e.g., "21:00" for SPX
- `close_time_tolerance_minutes`: Window around close for snapshot detection, e.g., 30
- `description`: Human-readable description

---

### 2. Storage Configuration

**File:** `src/config/results_config.py`

**Responsibility:** Define where and when to write snapshots, retention policy.

```python
from dataclasses import dataclass
from datetime import time
from typing import Dict

@dataclass
class DailySnapshotConfig:
    """Config for daily snapshot writing per index."""
    close_time_utc: time            # When to write daily snapshot
    close_time_tolerance_minutes: int  # Window before/after close
    intraday_retention_days: int    # How long to keep intraday data

@dataclass
class ResultsStorageConfig:
    """Overall storage policy."""
    intraday_retention_days: int = 31
    daily_retention_days: Optional[int] = None  # None = forever
    sensitivities_retention_days: Optional[int] = None  # None = latest only

# Per-index snapshot configuration
MULTI_REGION_CONFIG: Dict[str, DailySnapshotConfig] = {
    "SPX_SPX": DailySnapshotConfig(
        close_time_utc=time(21, 0),
        close_time_tolerance_minutes=30,
        intraday_retention_days=31
    ),
    "DAX_DAX": DailySnapshotConfig(
        close_time_utc=time(16, 30),
        close_time_tolerance_minutes=30,
        intraday_retention_days=31
    ),
    "NIFTY_NIFTY": DailySnapshotConfig(
        close_time_utc=time(9, 0),
        close_time_tolerance_minutes=15,
        intraday_retention_days=31
    ),
}

# Global retention policy
GLOBAL_STORAGE_CONFIG = ResultsStorageConfig(
    intraday_retention_days=31,
    daily_retention_days=None,  # Keep daily snapshots forever
    sensitivities_retention_days=None  # Keep only latest
)
```

**How Writers Use This:**
```python
# PostgresResultsWriter.write_trial()
def write_trial(self, trial: TrialResults, as_of: datetime):
    # Write intraday (always)
    self._write_intraday(trial, as_of)
    
    # Should we write daily snapshot?
    if self._should_write_daily_snapshot(trial.index.name, as_of):
        self._write_daily_snapshot(trial, as_of)
    
    # Update sensitivities
    self._write_sensitivities(trial, as_of)

def _should_write_daily_snapshot(self, index_name: str, as_of: datetime) -> bool:
    """Check if as_of falls within this index's snapshot window."""
    config = MULTI_REGION_CONFIG[index_name]
    close_time = config.close_time_utc
    tolerance = config.close_time_tolerance_minutes
    
    # as_of.time() should be within [close_time - tolerance, close_time + tolerance]
    as_of_time = as_of.time()
    window_start = time(close_time.hour - (tolerance // 60), close_time.minute - (tolerance % 60))
    window_end = time(close_time.hour + (tolerance // 60), close_time.minute + (tolerance % 60))
    
    return window_start <= as_of_time <= window_end
```

---

## Component Details

### Layer 1: Input Extraction (TO BUILD)

#### MarketDataExtractor

**File:** `src/extraction/market_data_extractor.py`

**Purpose:** Read market data from source tables, construct DataPackage for AnalyticsEngine.

**Implementation:**
```python
import pandas as pd
from sqlalchemy import Engine
from src.config.indices_config import Index
from src.extraction.data_package import DataPackage

class MarketDataExtractor:
    def __init__(self, engine: Engine):
        """Initialize with database engine (reads from market data schema)."""
        self.engine = engine
    
    def get_ivol_surface(self, index: Index, as_of: datetime) -> pd.DataFrame:
        """
        Query ivols_live for this index's portfolio/symbol.
        
        Args:
            index: Index object with portfolio, symbol
            as_of: Timestamp; fetch vols as of this time
        
        Returns:
            DataFrame with columns: term, strike, vol, timestamp
            
        Example:
            ivol = extractor.get_ivol_surface(Index(portfolio="SPX", ...), datetime.utcnow())
            # Returns:
            #    term  strike  vol  timestamp
            # 0   1M    100.0  0.15  2024-01-15 21:00:00
            # 1   1M    105.0  0.16  2024-01-15 21:00:00
        """
        query = f"""
        SELECT term, strike, vol, timestamp
        FROM ivols_live
        WHERE portfolio = '{index.portfolio}'
          AND symbol = '{index.symbol}'
          AND timestamp <= '{as_of}'
        ORDER BY timestamp DESC
        LIMIT 1
        """
        df = pd.read_sql(query, self.engine)
        return df
    
    def get_svol_surface(self, index: Index, as_of: datetime) -> pd.DataFrame:
        """
        Query svols_live for all constituents of this index.
        
        Args:
            index: Index object
            as_of: Timestamp
        
        Returns:
            DataFrame with columns: symbol, term, strike, vol, timestamp
            
        Example:
            svol = extractor.get_svol_surface(index, datetime.utcnow())
            # Returns:
            #    symbol  term  strike  vol  timestamp
            # 0  AAPL    1M    100.0  0.20  2024-01-15 21:00:00
            # 1  MSFT    1M    100.0  0.18  2024-01-15 21:00:00
        """
        # First get constituents for this index
        weights = self.get_index_weights(index, as_of)
        symbols = weights["symbol"].tolist()
        
        # Then query svols for those symbols
        query = f"""
        SELECT symbol, term, strike, vol, timestamp
        FROM svols_live
        WHERE symbol IN ({','.join(f"'{s}'" for s in symbols)})
          AND timestamp <= '{as_of}'
        ORDER BY timestamp DESC
        """
        df = pd.read_sql(query, self.engine)
        return df
    
    def get_index_weights(self, index: Index, as_of: datetime) -> pd.DataFrame:
        """
        Query index_weights for this index.
        
        Returns:
            DataFrame with columns: symbol, weight
            
        Example:
            weights = extractor.get_index_weights(index, datetime.utcnow())
            # Returns:
            #   symbol  weight
            # 0  AAPL    0.07
            # 1  MSFT    0.06
            #   ...
        """
        query = f"""
        SELECT symbol, weight
        FROM index_weights
        WHERE portfolio = '{index.portfolio}'
          AND last_updated <= '{as_of}'
        ORDER BY last_updated DESC
        LIMIT 1
        """
        df = pd.read_sql(query, self.engine)
        return df
    
    def get_stock_returns(self, index: Index, as_of: datetime, lookback_days: int = 252) -> pd.DataFrame:
        """
        (Optional) Build historical returns for all constituents.
        
        Returns:
            DataFrame with columns: symbol, date, return
        """
        weights = self.get_index_weights(index, as_of)
        symbols = weights["symbol"].tolist()
        
        query = f"""
        SELECT symbol, date, 
               (close - LAG(close) OVER (PARTITION BY symbol ORDER BY date)) / LAG(close) OVER (PARTITION BY symbol ORDER BY date) as return
        FROM stock_prices
        WHERE symbol IN ({','.join(f"'{s}'" for s in symbols)})
          AND date >= DATE('{as_of}' - INTERVAL '{lookback_days} days')
          AND date <= DATE('{as_of}')
        ORDER BY symbol, date
        """
        df = pd.read_sql(query, self.engine)
        return df
    
    def create_data_package(self, index: Index, as_of: datetime) -> DataPackage:
        """
        Orchestrate all extraction methods, construct DataPackage.
        
        Raises:
            ValueError if any required data is missing/empty
        """
        ivol_surface = self.get_ivol_surface(index, as_of)
        if ivol_surface.empty:
            raise ValueError(f"No ivol data for {index.name} as of {as_of}")
        
        svol_surface = self.get_svol_surface(index, as_of)
        if svol_surface.empty:
            raise ValueError(f"No svol data for {index.name} as of {as_of}")
        
        weights = self.get_index_weights(index, as_of)
        if weights.empty:
            raise ValueError(f"No weight data for {index.name} as of {as_of}")
        
        returns = self.get_stock_returns(index, as_of, lookback_days=252)
        # Returns are optional; don't raise if empty
        
        return DataPackage(
            index=index,
            as_of=as_of,
            ivol_surface=ivol_surface,
            svol_surface=svol_surface,
            weights=weights,
            returns=returns if not returns.empty else None
        )
```

---

#### Validation Helpers (TO BUILD)

**File:** `src/extraction/validation.py`

**Purpose:** Detect data quality issues before passing to AnalyticsEngine.

```python
import pandas as pd
import numpy as np
from typing import Dict, List

class ValidationError(Exception):
    """Raised when data validation fails."""
    pass

def validate_weights(weights_df: pd.DataFrame) -> Dict[str, any]:
    """
    Validate index weights.
    
    Checks:
    - Sum close to 1.0
    - No negative weights
    - No NaN values
    
    Returns:
        {"valid": bool, "errors": [list of error strings]}
    """
    errors = []
    
    if weights_df.empty:
        return {"valid": False, "errors": ["Weights dataframe is empty"]}
    
    total = weights_df["weight"].sum()
    if not (0.99 <= total <= 1.01):
        errors.append(f"Weights sum to {total:.4f}, expected ~1.0")
    
    if (weights_df["weight"] < 0).any():
        neg_count = (weights_df["weight"] < 0).sum()
        errors.append(f"{neg_count} negative weights found")
    
    if weights_df["weight"].isna().any():
        na_count = weights_df["weight"].isna().sum()
        errors.append(f"{na_count} NaN weights found")
    
    return {"valid": len(errors) == 0, "errors": errors}

def validate_surface(surface_df: pd.DataFrame, surface_name: str = "surface") -> Dict[str, any]:
    """
    Validate volatility surface.
    
    Checks:
    - No negative vols
    - Reasonable boundaries (0.01 < vol < 2.0)
    - Coverage of term/strike grid
    - No NaN values in vol column
    
    Returns:
        {"valid": bool, "errors": [list of error strings]}
    """
    errors = []
    
    if surface_df.empty:
        return {"valid": False, "errors": [f"{surface_name} dataframe is empty"]}
    
    if (surface_df["vol"] < 0).any():
        neg_count = (surface_df["vol"] < 0).sum()
        errors.append(f"{neg_count} negative vols in {surface_name}")
    
    if (surface_df["vol"] < 0.01).any() or (surface_df["vol"] > 2.0).any():
        out_of_range = ((surface_df["vol"] < 0.01) | (surface_df["vol"] > 2.0)).sum()
        errors.append(f"{out_of_range} vols outside [0.01, 2.0] in {surface_name}")
    
    if surface_df["vol"].isna().any():
        na_count = surface_df["vol"].isna().sum()
        errors.append(f"{na_count} NaN vols in {surface_name}")
    
    # Check coverage
    unique_terms = surface_df["term"].nunique()
    unique_strikes = surface_df["strike"].nunique()
    expected_rows = unique_terms * unique_strikes
    actual_rows = len(surface_df)
    if actual_rows < expected_rows * 0.8:  # Allow 20% missing
        errors.append(f"Low coverage: {actual_rows}/{expected_rows} expected rows in {surface_name}")
    
    return {"valid": len(errors) == 0, "errors": errors}

def validate_data_package(package: DataPackage) -> Dict[str, any]:
    """
    Comprehensive validation of DataPackage before compute.
    
    Returns:
        {
            "valid": bool,
            "weight_issues": {...},
            "ivol_issues": {...},
            "svol_issues": {...}
        }
    """
    results = {
        "valid": True,
        "weight_issues": validate_weights(package.weights),
        "ivol_issues": validate_surface(package.ivol_surface, "ivol_surface"),
        "svol_issues": validate_surface(package.svol_surface, "svol_surface"),
    }
    
    results["valid"] = all(v["valid"] for v in [
        results["weight_issues"],
        results["ivol_issues"],
        results["svol_issues"]
    ])
    
    return results
```

---

### Layer 3: Load + Results (BUILT)

#### ResultsWriter (Abstract Base Class)

**File:** `src/connectors/results_writer.py`

```python
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional
from src.extraction.data_package import TrialResults

class WriterException(Exception):
    """Base exception for writer errors."""
    pass

class WriterConnectionError(WriterException):
    """Database connection failed."""
    pass

class WriterTableError(WriterException):
    """Table creation/schema error."""
    pass

class WriterWriteError(WriterException):
    """Write operation failed."""
    pass

class ResultsWriter(ABC):
    """Abstract writer for trial results."""
    
    @abstractmethod
    def ensure_tables_exist(self) -> None:
        """Create tables if they don't exist. Idempotent."""
        pass
    
    @abstractmethod
    def write_trial(self, trial: TrialResults, as_of: datetime) -> None:
        """
        Write trial results to database.
        
        Args:
            trial: TrialResults object
            as_of: Timestamp for this data snapshot
        
        Writes to:
        - correlations_intraday: append all rows
        - sensitivities_latest: upsert latest per (index, term, strike, symbol)
        - correlations_daily: upsert if as_of falls in snapshot window
        """
        pass
    
    @abstractmethod
    def cleanup_old_intraday(self, as_of: datetime) -> int:
        """
        Delete intraday rows older than retention window.
        
        Returns:
            Number of rows deleted
        """
        pass
```

#### PostgreSQL Writer

**File:** `src/storage/postgres_writer.py`

Key behaviors:
- Idempotent writes using `INSERT ... ON CONFLICT DO UPDATE`
- Once-per-day snapshot enforcement via `_should_write_daily_snapshot()`
- Cleanup respects configured retention

---

#### CorrelationsReader (Abstract Base Class)

**File:** `src/storage/correlations_reader.py`

```python
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Optional, List, Iterator, Dict, Any
import pandas as pd

class ReaderException(Exception):
    """Base exception for reader errors."""
    pass

class ReaderConnectionError(ReaderException):
    """Database connection failed."""
    pass

class ReaderQueryError(ReaderException):
    """Query execution failed."""
    pass

class ReaderValidationError(ReaderException):
    """Data validation failed."""
    pass

class CorrelationsReader(ABC):
    """Abstract reader for correlation results."""
    
    @abstractmethod
    def get_latest_intraday(self, index: Optional[str] = None, limit: int = 100) -> pd.DataFrame:
        """Get latest intraday records."""
        pass
    
    @abstractmethod
    def get_intraday_as_of(self, index: str, as_of: datetime) -> pd.DataFrame:
        """Get all intraday records at a specific timestamp."""
        pass
    
    @abstractmethod
    def get_intraday_range(self, index: str, start_datetime: datetime, end_datetime: datetime,
                          term: Optional[str], strike: Optional[float]) -> pd.DataFrame:
        """Get intraday records in a time range, optionally filtered by term/strike."""
        pass
    
    # ... (many more methods for daily, sensitivities, aggregations, export, validation)
```

---

## Operational Modes

### Mode 1: Backfill (BUILT)

**File:** `jobs/backfill_correlations.py`

**Purpose:** Populate historical correlations from a date range.

**Usage:**
```bash
python jobs/backfill_correlations.py \
  --from_date 2024-01-01 \
  --to_date 2024-12-31 \
  --portfolio SPX \
  --db_url postgresql://user:pass@localhost/analytics \
  --skip_existing
```

**Algorithm:**
1. Parse CLI arguments
2. Load INDICES_CONFIG and filter by portfolio/symbol
3. Generate business days from from_date to to_date
4. For each (date, index):
   - Skip if snapshot exists and --skip_existing flag set
   - Create DataPackage via MarketDataExtractor
   - Compute via AnalyticsEngine
   - Write via ResultsWriter
5. Report statistics and exit code

---

### Mode 2: Real-Time (TO BUILD)

**File:** `jobs/compute_correlations_realtime.py`

**Purpose:** Run every 5 minutes, compute for all indices, write latest data.

**Pseudocode:**
```python
#!/usr/bin/env python
"""
Real-time correlation compute job.

Usage:
    python jobs/compute_correlations_realtime.py \
      --db_url postgresql://user:pass@localhost/analytics

Typically scheduled via cron:
    */5 * * * * /path/to/venv/bin/python /path/to/jobs/compute_correlations_realtime.py
"""

import argparse
import logging
from datetime import datetime
from sqlalchemy import create_engine

from src.config.indices_config import INDICES_CONFIG, Index
from src.extraction.market_data_extractor import MarketDataExtractor
from src.extraction.validation import validate_data_package
from src.storage.postgres_writer import PostgresResultsWriter
from src.config.results_config import MULTI_REGION_CONFIG
from src.analytics import AnalyticsEngine  # External dependency

logger = logging.getLogger(__name__)

def compute_realtime(db_url: str) -> None:
    """
    Run one iteration of real-time compute.
    
    For each index in INDICES_CONFIG:
    1. Extract market data
    2. Validate
    3. Compute correlations
    4. Write to database
    """
    now = datetime.utcnow()
    logger.info(f"Starting real-time compute at {now}")
    
    engine = create_engine(db_url)
    extractor = MarketDataExtractor(engine)
    writer = PostgresResultsWriter(engine, config=MULTI_REGION_CONFIG)
    analytics = AnalyticsEngine()
    
    # Ensure tables exist
    writer.ensure_tables_exist()
    
    success_count = 0
    failure_count = 0
    
    for config in INDICES_CONFIG:
        index = Index(**config, name=f"{config['portfolio']}_{config['symbol']}")
        
        try:
            logger.info(f"Processing {index.name}")
            
            # Extract
            package = extractor.create_data_package(index, as_of=now)
            logger.debug(f"Extracted package for {index.name}: {len(package.ivol_surface)} ivol rows")
            
            # Validate
            validation_result = validate_data_package(package)
            if not validation_result["valid"]:
                logger.warning(f"Validation failed for {index.name}: {validation_result}")
                failure_count += 1
                continue
            
            # Compute
            trial = analytics.compute_all_terms(package)
            logger.debug(f"Computed trial for {index.name}: {len(trial.correlations)} correlation rows")
            
            # Write
            writer.write_trial(trial, as_of=now)
            logger.info(f"Successfully wrote {index.name}")
            success_count += 1
            
        except Exception as e:
            logger.error(f"Failed to process {index.name}: {e}", exc_info=True)
            failure_count += 1
    
    logger.info(f"Real-time compute complete: {success_count} successes, {failure_count} failures")
    
    if failure_count > 0:
        raise RuntimeError(f"{failure_count} indices failed")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Real-time correlations compute job")
    parser.add_argument("--db_url", type=str, default="postgresql://localhost/analytics",
                       help="Database URL")
    parser.add_argument("--log_level", type=str, default="INFO",
                       choices=["DEBUG", "INFO", "WARNING", "ERROR"],
                       help="Logging level")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    try:
        compute_realtime(args.db_url)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        exit(1)
```

---

### Mode 3: Cleanup (TO BUILD)

**File:** `jobs/cleanup_old_data.py`

**Purpose:** Delete old intraday data (retention management).

**Pseudocode:**
```python
#!/usr/bin/env python
"""
Cleanup old intraday data.

Usage:
    python jobs/cleanup_old_data.py \
      --db_url postgresql://user:pass@localhost/analytics

Typically scheduled daily:
    0 2 * * * /path/to/venv/bin/python /path/to/jobs/cleanup_old_data.py
"""

import argparse
import logging
from datetime import datetime
from sqlalchemy import create_engine

from src.storage.postgres_writer import PostgresResultsWriter
from src.config.results_config import GLOBAL_STORAGE_CONFIG

logger = logging.getLogger(__name__)

def cleanup_old_data(db_url: str) -> None:
    """Delete intraday data older than retention window."""
    engine = create_engine(db_url)
    writer = PostgresResultsWriter(engine)
    
    now = datetime.utcnow()
    rows_deleted = writer.cleanup_old_intraday(as_of=now)
    
    logger.info(f"Cleanup complete: deleted {rows_deleted} old intraday rows")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Cleanup old intraday data")
    parser.add_argument("--db_url", type=str, default="postgresql://localhost/analytics",
                       help="Database URL")
    
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    
    try:
        cleanup_old_data(args.db_url)
    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        exit(1)
```

---

## Implementation Guide

### Phase 1: Minimal Viable (Week 1)
1. **Define DataPackage** – finalize structure
2. **Implement MarketDataExtractor.create_data_package()** – basic queries
3. **Test backfill job** – verify write semantics
4. **Verify once-per-day snapshots** – check daily table has correct rows

**Success criteria:**
- Backfill runs for 5 business days
- correlations_daily has exactly 5 rows (one per day per index)
- correlations_intraday has ~5 rows per day

### Phase 2: Production Hardening (Week 2)
5. **Add extraction validation** – validate_weights(), validate_surface()
6. **Implement real-time job** – 5-min loop, AnalyticsEngine integration
7. **Test snapshot timing edge cases** – multi-region snapshot windows
8. **Add cleanup job** – daily intraday pruning

**Success criteria:**
- Real-time job runs without errors
- Daily snapshot written at correct time for each index
- Old intraday rows cleaned up after retention window

### Phase 3: Operational Excellence (Week 3+)
9. **Setup observability** – logging, metrics, alerts
10. **Implement BigQueryCorrelationsReader** – optional, for analytics
11. **Document operational procedures** – runbooks, troubleshooting
12. **Performance tuning** – query optimization, batch sizes

---

## Design Principles

| Principle | Application | Benefit |
|-----------|-----------|---------|
| **Single Source of Truth** | INDICES_CONFIG used by all jobs | Consistency across pipeline |
| **Configuration over Code** | Snapshot windows, retention in config | Easy to modify without deploys |
| **Immutable Intraday** | Append-only, no updates | Audit trail, replay-safe |
| **Idempotent Writes** | INSERT ... ON CONFLICT | Retry-safe, horizontally scalable |
| **Interface Segregation** | ResultsWriter/Reader ABCs | Easy to swap backends |
| **Fail Fast** | Validation helpers | Catch bad data before compute |
| **Stateless Jobs** | No job state, reentrant | Can be scaled, retried, parallelized |
| **Consumer-Centric** | Rich Reader API | Easy for downstream users |
| **Declarative Scheduling** | Config-driven snapshot times | Different indices, one pipeline |

---

## Troubleshooting Guide

### Problem: Backfill runs but no data written

**Diagnosis:**
1. Check MarketDataExtractor queries return rows
2. Verify AnalyticsEngine output is non-null
3. Check database connectivity

**Solution:**
```python
# Debug extraction
from src.extraction.market_data_extractor import MarketDataExtractor
extractor = MarketDataExtractor(engine)
index = INDICES_BY_NAME["SPX_SPX"]
package = extractor.create_data_package(index, datetime.utcnow())
print(package.ivol_surface)  # Should not be empty
```

### Problem: Daily snapshot not written at expected time

**Diagnosis:**
1. Check `close_time_utc` in INDICES_CONFIG
2. Verify `as_of` timestamp vs. snapshot window
3. Ensure job runs during the window

**Solution:**
```python
# Check snapshot window
from src.config.results_config import MULTI_REGION_CONFIG
config = MULTI_REGION_CONFIG["SPX_SPX"]
print(f"Close: {config.close_time_utc}, Tolerance: {config.close_time_tolerance_minutes} min")

# Check if as_of falls in window
from datetime import datetime, timedelta, time
as_of = datetime.utcnow()
close_time = config.close_time_utc
tolerance = timedelta(minutes=config.close_time_tolerance_minutes)
window_start = datetime.combine(as_of.date(), close_time) - tolerance
window_end = datetime.combine(as_of.date(), close_time) + tolerance
print(f"Window: {window_start} to {window_end}")
print(f"as_of in window? {window_start <= as_of <= window_end}")
```

---

