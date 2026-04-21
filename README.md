# Correlations Pipeline

A production-grade three-layer pipeline for implied correlation analytics. Extracts market data, computes correlations and sensitivities, and stores results in PostgreSQL or BigQuery with rich querying and validation capabilities.

## Quick Start

### Installation
```bash
pip install -r requirements.txt
```

### Configuration
Edit `src/config/indices_config.py` to add/modify indices:
```python
INDICES_CONFIG = [
    {
        "portfolio": "SPX",
        "symbol": "SPX",
        "close_time_utc": "21:00",
        "close_time_tolerance_minutes": 30,
        "description": "S&P 500 Index"
    },
    # Add more indices...
]
```

### Backfill Historical Data
```bash
python jobs/backfill_correlations.py \
  --from_date 2024-01-01 \
  --to_date 2024-12-31 \
  --portfolio SPX \
  --db_url postgresql://user:pass@localhost/analytics
```

### Run Real-Time Pipeline (5-min)
```bash
python jobs/compute_correlations_realtime.py \
  --db_url postgresql://user:pass@localhost/analytics
```

Typically scheduled via cron or Airflow:
```bash
*/5 * * * * /path/to/venv/bin/python /path/to/jobs/compute_correlations_realtime.py
```

### Query Results
```python
from src.storage.postgres_reader import PostgresCorrelationsReader

reader = PostgresCorrelationsReader(engine)

# Get latest intraday data
df = reader.get_latest_intraday(index="SPX_SPX", limit=100)

# Get daily snapshots
df = reader.get_daily_snapshots(
    index="SPX_SPX",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 12, 31),
    term="1M"
)

# Validate data quality
issues = reader.validate_data(index="SPX_SPX")
```

---

## Architecture

### Three-Layer Design
1. **Input Extraction** (market data → DataPackage)
2. **Compute** (DataPackage → TrialResults) – external AnalyticsEngine
3. **Load + Results** (TrialResults → database)

### Data Flow
```
Market DB (ivols_live, svols_live, index_weights, stock_prices)
    ↓
MarketDataExtractor
    ↓
DataPackage
    ↓
AnalyticsEngine.compute_all_terms()
    ↓
TrialResults
    ↓
ResultsWriter (Postgres/BigQuery)
    ↓
Analytics DB
    ├─ correlations_intraday (append-only, 31-day retention)
    ├─ correlations_daily (upsert, forever)
    └─ sensitivities_latest (upsert, latest only)
    ↓
CorrelationsReader (query, aggregate, export, validate)
    ↓
Consumers (dashboards, reports, downstream systems)
```

---

## Components

### Built ✅
- **Index Config** (`src/config/indices_config.py`) – Central index registry
- **Storage Config** (`src/config/results_config.py`) – Snapshot windows + retention policies
- **Writers** – PostgreSQL + BigQuery result writers with idempotent semantics
- **Readers** – Rich query API for intraday, daily snapshots, sensitivities
- **Backfill Job** – Historical data population with CLI and detailed stats
- **Table Schemas** – Immutable intraday, durable daily snapshots, point-in-time sensitivities

### To Build 🔨
- **MarketDataExtractor** – Extract ivols, svols, weights, returns
- **DataPackage** – Finalized structure for AnalyticsEngine
- **Validation Helpers** – Weights, vols, coverage checks
- **Real-Time Job** – 5-min compute + write pipeline
- **Cleanup Job** – Daily intraday retention pruning

---

## File Structure
```
correlations-pipeline/
├── README.md
├── docs.md
├── requirements.txt
├── src/
│   ├── __init__.py
│   ├── config/
│   │   ├── indices_config.py       [BUILT]
│   │   └── results_config.py       [BUILT]
│   ├── connectors/
│   │   └── results_writer.py       [BUILT]
│   ├── storage/
│   │   ├── schemas.py              [BUILT]
│   │   ├── postgres_writer.py      [BUILT]
│   │   ├── bigquery_writer.py      [BUILT]
│   │   └── postgres_reader.py      [BUILT]
│   └── extraction/
│       ├── market_data_extractor.py [TO BUILD)
│       ├── data_package.py          [TO BUILD)
│       └── validation.py            [TO BUILD)
├── jobs/
│   ├── backfill_correlations.py    [BUILT]
│   ├── compute_correlations_realtime.py [TO BUILD)
│   └── cleanup_old_data.py          [TO BUILD)
└── tests/
    └── (test files)
```

---

## Key Design Decisions

| Decision | Rationale | Tradeoff |
|----------|-----------|----------|
| **Once-per-day snapshots (config-driven)** | Indices close at different UTC times | Requires careful timestamp coordination |
| **Sensitivities are point-in-time** | Latest decomposition sufficient for risk | Cannot query sensitivity evolution over time |
| **Intraday retention ~31 days** | Reduces storage cost | Use daily snapshots for longer backtests |
| **Append-only intraday** | Immutable audit trail | Cannot correct historical intraday data |
| **Idempotent writes (ON CONFLICT)** | Retry-safe, horizontally scalable | Slight write overhead |
| **Abstract Reader/Writer ABCs** | Swap backends without changing code | Some queries may not translate perfectly |

---

## Configuration

### Index Config (`src/config/indices_config.py`)
```python
INDICES_CONFIG = [
    {
        "portfolio": "SPX",
        "symbol": "SPX",
        "close_time_utc": "21:00",
        "close_time_tolerance_minutes": 30,
        "description": "S&P 500 Index"
    },
]

# Auto-generated from config:
# INDICES_BY_PORTFOLIO["SPX"] = Index(name="SPX_SPX", ...)
# INDICES_BY_SYMBOL["SPX"] = Index(name="SPX_SPX", ...)
# INDICES_BY_NAME["SPX_SPX"] = Index(...)
```

### Storage Config (`src/config/results_config.py`)
```python
MULTI_REGION_CONFIG = {
    "SPX_SPX": DailySnapshotConfig(
        close_time_utc="21:00",
        close_time_tolerance_minutes=30,
        intraday_retention_days=31
    ),
    "DAX_DAX": DailySnapshotConfig(
        close_time_utc="16:30",
        close_time_tolerance_minutes=30,
        intraday_retention_days=31
    ),
}
```

---

## Database Schema

### correlations_intraday
```sql
CREATE TABLE correlations_intraday (
    id SERIAL PRIMARY KEY,
    as_of_datetime TIMESTAMP NOT NULL,
    index_name VARCHAR(50) NOT NULL,
    term VARCHAR(20) NOT NULL,
    strike FLOAT NOT NULL,
    implied_correlation FLOAT,
    index_volatility FLOAT,
    num_components INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
CREATE INDEX idx_intraday_as_of_index ON correlations_intraday(as_of_datetime, index_name);
```

**Characteristics:**
- Append-only mode
- ~31 day retention
- Indexed by `as_of_datetime, index_name`

### correlations_daily
```sql
CREATE TABLE correlations_daily (
    id SERIAL PRIMARY KEY,
    snapshot_date DATE NOT NULL,
    snapshot_type VARCHAR(20) NOT NULL,
    index_name VARCHAR(50) NOT NULL,
    term VARCHAR(20) NOT NULL,
    strike FLOAT NOT NULL,
    implied_correlation FLOAT,
    index_volatility FLOAT,
    num_components INT,
    as_of_datetime TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(snapshot_date, index_name, term, strike)
);
CREATE INDEX idx_daily_snapshot_date_index ON correlations_daily(snapshot_date, index_name);
```

**Characteristics:**
- Upsert mode (unique per snapshot_date, index_name, term, strike)
- Forever retention
- One snapshot per day per index at configured close time

### sensitivities_latest
```sql
CREATE TABLE sensitivities_latest (
    id SERIAL PRIMARY KEY,
    index_name VARCHAR(50) NOT NULL,
    term VARCHAR(20) NOT NULL,
    strike FLOAT NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    delta FLOAT,
    elasticity FLOAT,
    sens_type VARCHAR(50),
    as_of_datetime TIMESTAMP NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(index_name, term, strike, symbol)
);
CREATE INDEX idx_sensitivities_index_name ON sensitivities_latest(index_name);
```

**Characteristics:**
- Upsert mode (latest only per index_name, term, strike, symbol)
- No history stored
- Point-in-time risk decomposition

---

## API Reference

### CorrelationsReader
```python
# Intraday queries
get_latest_intraday(index: Optional[str], limit: int) -> pd.DataFrame
get_intraday_as_of(index: str, as_of: datetime) -> pd.DataFrame
get_intraday_range(index: str, start_datetime, end_datetime, term, strike) -> pd.DataFrame
get_intraday_by_filters(index: str, start_datetime, end_datetime, term_min, term_max, strike_min, strike_max, vol_min, vol_max) -> pd.DataFrame
iter_intraday(index: str, start_datetime, end_datetime, batch_size) -> Iterator[pd.DataFrame]

# Daily queries
get_daily_snapshots(index: str, start_date, end_date, term, strike) -> pd.DataFrame
get_daily_as_of(index: str, snapshot_date: date) -> pd.DataFrame
get_daily_by_filters(index: str, start_date, end_date, term_min, term_max, strike_min, strike_max) -> pd.DataFrame
iter_daily_snapshots(index: str, start_date, end_date, batch_size) -> Iterator[pd.DataFrame]

# Sensitivities
get_latest_sensitivities(index, term, strike) -> pd.DataFrame
get_sensitivities_as_of(index: str, as_of: datetime) -> pd.DataFrame

# Aggregations
get_correlation_timeseries(index: str, metric, frequency, start_date, end_date, term, strike, aggregation) -> pd.DataFrame
compare_indices(indices: List[str], metric, frequency, start_date, end_date) -> pd.DataFrame
get_correlation_statistics(index: str, start_date, end_date, frequency) -> Dict[str, float]

# Export
export_to_csv(index: str, filepath: str, frequency, start_date, end_date) -> int
export_to_parquet(index: str, filepath: str, frequency, start_date, end_date) -> int

# Validation
validate_data(index: str, snapshot_date) -> Dict[str, List[str]]
check_snapshot_coverage(index: str, start_date, end_date) -> Dict[str, float]
check_consistency(index: str, snapshot_date) -> Dict[str, Any]
get_data_summary(index: str, start_date, end_date) -> Dict[str, Any]
```

---

## Testing

Run tests:
```bash
pytest tests/ -v
```

Example test:
```python
def test_backfill_single_index():
    # Setup
    engine = create_engine("sqlite:///:memory:")
    writer = PostgresResultsWriter(engine)
    
    # Backfill 5 business days
    backfill(
        from_date=date(2024, 1, 1),
        to_date=date(2024, 1, 5),
        portfolio="SPX",
        writer=writer
    )
    
    # Verify
    reader = PostgresCorrelationsReader(engine)
    snapshots = reader.get_daily_snapshots(
        index="SPX_SPX",
        start_date=date(2024, 1, 1),
        end_date=date(2024, 1, 5)
    )
    assert len(snapshots) == 5
```

---

## Monitoring & Observability

### Key Metrics
- **Backfill job:** successes, failures, skipped records, average run time per index
- **Real-time job:** compute latency, write latency, missing data packages
- **Data quality:** outlier correlations, null fields, coverage %, duplicate snapshots

### Logging
All components use Python `logging`. Enable DEBUG for verbose output:
```python
import logging
logging.basicConfig(level=logging.DEBUG)
```

### Data Quality Checks
Use `CorrelationsReader.validate_data()`:
```python
issues = reader.validate_data(index="SPX_SPX", snapshot_date=date(2024, 1, 15))
# Returns: {
#     "out_of_range_correlations": [3, 47],  # row ids
#     "null_fields": [1, 2, 5],
# }
```

---

## Troubleshooting

### No data written after backfill
1. Check `MarketDataExtractor` returns non-empty DataPackage
2. Verify `AnalyticsEngine.compute_all_terms()` returns non-null TrialResults
3. Check database connectivity and schema exists

### Snapshot not written at expected time
1. Verify `close_time_utc` in `INDICES_CONFIG` matches your intent
2. Check `as_of` timestamp in job vs. configured snapshot window
3. Ensure job runs during or shortly after the snapshot window

### Duplicate snapshots
1. Use `check_consistency()` to detect duplicates
2. Backfill was likely re-run for same date/index
3. Upsert logic will overwrite; safe to retry

---

## Contributing

1. Add new indices to `INDICES_CONFIG`
2. Update `MULTI_REGION_CONFIG` with snapshot times
3. Run backfill for historical data
4. Monitor via data quality checks

---

## License

Internal use only.
