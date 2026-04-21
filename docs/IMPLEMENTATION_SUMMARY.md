# Implementation Summary – Correlations Pipeline

## What Has Been Built

### Documentation
✅ **README.md** – Quick start guide, architecture overview, configuration, API reference, troubleshooting  
✅ **docs.md** – Detailed technical documentation, design principles, implementation guide  

### Code Files (Ready to Use)

**Input Extraction Layer (`src/extraction/`):**
✅ **`data_package.py`** – DataPackage definition with validation  
✅ **`market_data_extractor.py`** – Extract market data from tables, construct DataPackage  
✅ **`validation.py`** – Validation helpers for weights, surfaces, constituent coverage  

**Operational Jobs (`jobs/`):**
✅ **`compute_correlations_realtime.py`** – 5-minute scheduler, extract→compute→write loop  
✅ **`cleanup_old_data.py`** – Daily intraday data retention pruning  

---

## Next Steps

### Phase 1: Integration (This Week)
1. **Adapt MarketDataExtractor** to your actual table schemas
   - Update SQL queries in `get_ivol_surface()`, `get_svol_surface()`, `get_index_weights()`
   - Adjust column names, table names, filters as needed
   - Test with sample data

2. **Integrate with AnalyticsEngine**
   - Ensure `AnalyticsEngine.compute_all_terms(DataPackage)` returns TrialResults with:
     - `trial.correlations`: DataFrame with columns `term, strike, implied_correlation, index_volatility, num_components`
     - `trial.sensitivities`: DataFrame with columns `index_name, term, strike, symbol, delta, elasticity, sens_type, as_of_datetime`
   - Update import in `compute_correlations_realtime.py` line 21

3. **Test backfill job (already exists)**
   ```bash
   python jobs/backfill_correlations.py \
     --from_date 2024-01-01 \
     --to_date 2024-01-10 \
     --portfolio SPX \
     --db_url postgresql://localhost/analytics
   ```
   - Verify `correlations_daily` has 5 rows (one per business day)
   - Verify `correlations_intraday` has correct row counts

### Phase 2: Real-Time Pipeline (Week 2)
1. **Test real-time job**
   ```bash
   python jobs/compute_correlations_realtime.py \
     --db_url postgresql://localhost/analytics
   ```
   - Verify all indices process without errors
   - Check data written to correlations_intraday
   - Verify daily snapshot written at correct time for each index

2. **Setup cron or Airflow scheduler**
   ```bash
   # Cron: every 5 minutes
   */5 * * * * /path/to/venv/bin/python /path/to/jobs/compute_correlations_realtime.py --db_url postgresql://localhost/analytics
   
   # Cron: cleanup daily at 2 AM UTC
   0 2 * * * /path/to/venv/bin/python /path/to/jobs/cleanup_old_data.py --db_url postgresql://localhost/analytics
   ```

3. **Test cleanup job**
   ```bash
   # Dry run first (see what would be deleted)
   python jobs/cleanup_old_data.py --db_url postgresql://localhost/analytics --dry_run
   
   # Then run for real
   python jobs/cleanup_old_data.py --db_url postgresql://localhost/analytics
   ```

### Phase 3: Production Hardening (Week 3+)
1. **Add observability**
   - Setup logging to files (see commented lines in job files)
   - Configure metrics export (job duration, row counts, error rates)
   - Setup alerts for:
     - Job failures (exit code != 0)
     - Data quality issues (validate_data_package returns errors)
     - Missing data packages (ExtractionError)
     - Snapshot timing issues

2. **Performance tuning**
   - Batch size optimization in iterator methods
   - Query optimization for large tables
   - Connection pooling for database

3. **Documentation updates**
   - Runbooks for operational procedures
   - SLA definitions (job should complete in <X seconds)
   - Escalation procedures for failures

---

## File Organization

### Project Structure
```
correlations-pipeline/
├── README.md                          [NEW - Quick start guide]
├── docs.md                            [NEW - Technical details]
├── requirements.txt                   [Update with your deps]
├── src/
│   ├── config/
│   │   ├── indices_config.py          [EXISTING - REVIEWED]
│   │   └── results_config.py          [EXISTING - REVIEWED]
│   ├── extraction/
│   │   ├── __init__.py
│   │   ├── data_package.py            [NEW]
│   │   ├── market_data_extractor.py   [NEW]
│   │   └── validation.py              [NEW]
│   ├── storage/
│   │   ├── schemas.py                 [EXISTING]
│   │   ├── postgres_writer.py         [EXISTING]
│   │   ├── bigquery_writer.py         [EXISTING]
│   │   └── postgres_reader.py         [EXISTING]
│   ├── connectors/
│   │   └── results_writer.py          [EXISTING]
│   └── analytics.py                   [UPDATE with your AnalyticsEngine]
├── jobs/
│   ├── backfill_correlations.py       [EXISTING - BUILT]
│   ├── compute_correlations_realtime.py [NEW]
│   └── cleanup_old_data.py            [NEW]
└── tests/
    └── (test files)
```

---

## Key Implementation Details

### DataPackage
- **Purpose**: Immutable container for all data needed by AnalyticsEngine
- **Columns**:
  - `ivol_surface`: term, strike, vol
  - `svol_surface`: symbol, term, strike, vol
  - `weights`: symbol, weight (sums to ~1.0)
  - `returns`: (optional) symbol, date, return
- **Validation**: Automatically validates structure in `__post_init__()`

### MarketDataExtractor
- **Methods**: 
  - `get_ivol_surface()` – query ivols_live
  - `get_svol_surface()` – query svols_live for constituents
  - `get_index_weights()` – query index_weights
  - `get_stock_returns()` – build returns from stock_prices
  - `create_data_package()` – orchestrator (main entry point)
- **Error Handling**: Raises ExtractionError on failures
- **Logging**: DEBUG logs show row counts extracted

### Validation Helpers
- **validate_weights()**: weights sum, no negatives, no NaNs
- **validate_surface()**: no negative vols, reasonable range, coverage >80%
- **validate_constituent_coverage()**: all weights have svol data
- **validate_data_package()**: comprehensive validation with detailed report

### Real-Time Job
- **Frequency**: Every 5 minutes (via cron/scheduler)
- **Flow**: Extract → Validate → Compute → Write for each index
- **Error Handling**: Continues on per-index failure, reports summary
- **Exit Code**: 0 = all succeeded, 1 = any failure
- **Logging**: INFO for progress, ERROR for failures, DEBUG for details

### Cleanup Job
- **Frequency**: Once daily (e.g., 2 AM UTC)
- **Logic**: Delete correlations_intraday rows older than retention window
- **Dry Run**: `--dry_run` flag shows what would be deleted
- **Safety**: Uses retention config, never deletes daily snapshots

---

## Testing Checklist

- [ ] DataPackage validates correctly with valid data
- [ ] DataPackage raises ValueError with missing columns
- [ ] MarketDataExtractor queries return expected shapes
- [ ] validate_weights() detects weight sum issues
- [ ] validate_surface() detects vol range issues
- [ ] validate_data_package() comprehensive validation works
- [ ] Backfill job writes exactly one snapshot per business day
- [ ] Real-time job processes all indices without error
- [ ] Daily snapshot written at correct UTC time per index
- [ ] Cleanup job deletes old intraday rows correctly
- [ ] Cleanup --dry_run doesn't delete anything

---

## Common Customizations

### 1. Adjust SQL Queries
**File**: `src/extraction/market_data_extractor.py`

Update `get_ivol_surface()`, `get_svol_surface()`, `get_index_weights()` methods:
```python
# If your table names differ:
query = """
SELECT term, strike, vol, timestamp
FROM your_custom_ivol_table_name
WHERE ...
"""
```

### 2. Add Custom Validation
**File**: `src/extraction/validation.py`

Add new validation function:
```python
def validate_custom_metric(surface_df: pd.DataFrame) -> Dict[str, Any]:
    """Your custom validation logic."""
    ...
```

Then call from `validate_data_package()`.

### 3. Modify Snapshot Timing
**File**: `src/config/results_config.py`

Update MULTI_REGION_CONFIG to adjust close times per index:
```python
"SPX_SPX": DailySnapshotConfig(
    close_time_utc=time(21, 0),  # 9:00 PM UTC (4:00 PM ET)
    close_time_tolerance_minutes=30,
    intraday_retention_days=31
),
```

### 4. Adjust Logging Level
**Real-time job**:
```bash
python jobs/compute_correlations_realtime.py --log_level DEBUG
```

**Cleanup job**:
```bash
python jobs/cleanup_old_data.py --log_level DEBUG
```

---

## Troubleshooting Quick Fixes

### Problem: Real-time job exits with code 1
**Solution**: Check logs for specific index failures
```bash
python jobs/compute_correlations_realtime.py --log_level DEBUG 2>&1 | grep ERROR
```

### Problem: Daily snapshot not written
**Solution**: Verify close_time_utc in INDICES_CONFIG matches your intent
```python
# Check if as_of falls in window
from datetime import datetime, time
as_of = datetime.utcnow()
close = time(21, 0)  # 9 PM UTC
tolerance_min = 30
# as_of.time() should be between (20:30, 21:30) UTC
```

### Problem: Validation fails but job should continue
**Solution**: Use `validate_data_package(..., strict=False)` to treat warnings as non-fatal

### Problem: Extraction queries return empty
**Solution**: Verify table/column names and filter conditions
```python
# Debug SQL query directly
import pandas as pd
from sqlalchemy import create_engine
engine = create_engine("postgresql://localhost/analytics")
df = pd.read_sql("SELECT * FROM ivols_live LIMIT 1", engine)
print(df.columns)  # Check column names
```

---

## Questions?

Refer to:
- **Quick questions**: Check README.md sections
- **Architecture questions**: See docs.md "System Architecture"
- **Configuration questions**: See docs.md "Configuration"
- **Code questions**: Docstrings in each file explain intent

---

**Status**: Ready for Phase 1 integration. All code files generated and documented.
