# Quick Reference – Commands & Troubleshooting

## Installation & Setup

```bash
# Install dependencies
pip install -r requirements.txt

# Setup database (if using PostgreSQL)
psql -U postgres -d analytics -f schema_setup.sql

# Verify installation
python -c "from src.config.indices_config import INDICES_CONFIG; print(f'{len(INDICES_CONFIG)} indices configured')"
```

---

## Running Jobs

### Backfill Historical Data
```bash
# Single index, date range
python jobs/backfill_correlations.py \
  --from_date 2024-01-01 \
  --to_date 2024-01-31 \
  --portfolio SPX \
  --db_url postgresql://user:pass@localhost/analytics

# All indices, skip existing
python jobs/backfill_correlations.py \
  --from_date 2024-01-01 \
  --to_date 2024-01-31 \
  --skip_existing \
  --db_url postgresql://user:pass@localhost/analytics

# Dry run (shows stats without writing)
python jobs/backfill_correlations.py \
  --from_date 2024-01-01 \
  --to_date 2024-01-10 \
  --portfolio SPX \
  --db_url postgresql://user:pass@localhost/analytics \
  --log_level DEBUG 2>&1 | head -50
```

### Real-Time 5-Minute Compute
```bash
# Run once (for testing)
python jobs/compute_correlations_realtime.py \
  --db_url postgresql://user:pass@localhost/analytics \
  --log_level INFO

# With debug logging
python jobs/compute_correlations_realtime.py \
  --db_url postgresql://user:pass@localhost/analytics \
  --log_level DEBUG

# Custom AnalyticsEngine path
python jobs/compute_correlations_realtime.py \
  --db_url postgresql://user:pass@localhost/analytics \
  --analytics_engine_path /path/to/custom_analytics.py
```

### Setup Cron Schedule
```bash
# Real-time: every 5 minutes
crontab -e
# Add: */5 * * * * /path/to/venv/bin/python /path/to/jobs/compute_correlations_realtime.py --db_url postgresql://user:pass@localhost/analytics >> /var/log/correlations/realtime.log 2>&1

# Cleanup: daily at 2 AM UTC
# Add: 0 2 * * * /path/to/venv/bin/python /path/to/jobs/cleanup_old_data.py --db_url postgresql://user:pass@localhost/analytics >> /var/log/correlations/cleanup.log 2>&1
```

### Cleanup Old Data
```bash
# Dry run (see what would be deleted)
python jobs/cleanup_old_data.py \
  --db_url postgresql://user:pass@localhost/analytics \
  --dry_run

# Actually delete (after verifying dry run)
python jobs/cleanup_old_data.py \
  --db_url postgresql://user:pass@localhost/analytics

# With custom retention
# (Edit src/config/results_config.py GLOBAL_STORAGE_CONFIG first)
```

---

## Testing & Debugging

### Test MarketDataExtractor
```python
from sqlalchemy import create_engine
from src.config.indices_config import INDICES_BY_NAME
from src.extraction.market_data_extractor import MarketDataExtractor
from datetime import datetime

engine = create_engine("postgresql://user:pass@localhost/analytics")
extractor = MarketDataExtractor(engine)

# Test extraction
index = INDICES_BY_NAME["SPX_SPX"]
try:
    package = extractor.create_data_package(index, datetime.utcnow())
    print(f"✓ Extracted {len(package.ivol_surface)} ivol rows")
    print(f"✓ Extracted {len(package.svol_surface)} svol rows")
    print(f"✓ Extracted {len(package.constituents)} constituents")
except Exception as e:
    print(f"✗ Extraction failed: {e}")
```

### Test Validation
```python
from src.extraction.validation import validate_data_package

result = validate_data_package(package)
print(f"Valid: {result['valid']}")
if result['errors']:
    print(f"Errors: {result['errors']}")
if result['warnings']:
    print(f"Warnings: {result['warnings']}")
```

### Test Writing
```python
from src.storage.postgres_writer import PostgresResultsWriter
from src.config.results_config import MULTI_REGION_CONFIG

writer = PostgresResultsWriter(engine, config=MULTI_REGION_CONFIG)
writer.ensure_tables_exist()
print("✓ Tables exist")

# After getting trial results from AnalyticsEngine:
writer.write_trial(trial, as_of=datetime.utcnow())
print("✓ Data written")
```

### Test Reading
```python
from src.storage.postgres_reader import PostgresCorrelationsReader
from datetime import date

reader = PostgresCorrelationsReader(engine)

# Get latest intraday
df = reader.get_latest_intraday(index="SPX_SPX", limit=10)
print(f"Latest {len(df)} intraday rows")

# Get daily snapshots
df = reader.get_daily_snapshots(
    index="SPX_SPX",
    start_date=date(2024, 1, 1),
    end_date=date(2024, 1, 31)
)
print(f"Daily snapshots: {len(df)} rows")

# Validate data quality
issues = reader.validate_data(index="SPX_SPX")
if issues["out_of_range_correlations"]:
    print(f"⚠ Out of range correlations: {issues['out_of_range_correlations']}")
```

---

## Common Problems & Solutions

### Problem: "No ivol data for SPX_SPX"

**Cause**: MarketDataExtractor.get_ivol_surface() returns empty

**Debug**:
```python
from sqlalchemy import create_engine, text
engine = create_engine("postgresql://user:pass@localhost/analytics")

# Check if ivols_live table exists and has data
with engine.connect() as conn:
    result = conn.execute(text("SELECT COUNT(*) as cnt FROM ivols_live"))
    count = result.scalar()
    print(f"Total ivol rows: {count}")
    
    # Check columns
    result = conn.execute(text("SELECT * FROM ivols_live LIMIT 1"))
    print(f"Columns: {list(result.keys())}")
    
    # Check for SPX data
    result = conn.execute(text("SELECT COUNT(*) FROM ivols_live WHERE portfolio='SPX' AND symbol='SPX'"))
    count = result.scalar()
    print(f"SPX rows: {count}")
```

**Solution**:
1. Verify table/column names match your schema
2. Update SQL queries in `market_data_extractor.py`
3. Ensure data exists for your date range

---

### Problem: "Validation failed: Weights sum to X.XX, expected ~1.0"

**Cause**: Index weights don't sum to 1.0

**Debug**:
```python
weights = extractor.get_index_weights(index, datetime.utcnow())
total = weights["weight"].sum()
print(f"Weight sum: {total:.6f}")
print(f"Expected: 1.0 ± 0.01")
print(weights.sort_values("weight", ascending=False).head(10))
```

**Solution**:
1. Check if weights table has most recent update
2. Verify constituents list is complete
3. Adjust tolerance in `validate_weights(weights_df, tolerance=0.02)`

---

### Problem: "Daily snapshot not written at expected time"

**Cause**: `as_of` timestamp not in snapshot window

**Debug**:
```python
from datetime import datetime, time, timedelta
from src.config.results_config import MULTI_REGION_CONFIG

config = MULTI_REGION_CONFIG["SPX_SPX"]
close_time = config.close_time_utc
tolerance = config.close_time_tolerance_minutes

as_of = datetime.utcnow()
as_of_time = as_of.time()

tolerance_td = timedelta(minutes=tolerance)
window_start = datetime.combine(as_of.date(), close_time) - tolerance_td
window_end = datetime.combine(as_of.date(), close_time) + tolerance_td

print(f"Close time: {close_time} ± {tolerance} min")
print(f"Window: {window_start} to {window_end}")
print(f"as_of: {as_of}")
print(f"In window? {window_start <= as_of <= window_end}")
```

**Solution**:
1. Adjust `close_time_utc` in `src/config/indices_config.py` to match actual market close
2. Adjust `close_time_tolerance_minutes` if window is too narrow
3. Schedule real-time job to run during the window

---

### Problem: Backfill succeeds but correlations_daily is empty

**Cause**: Data written to correlations_intraday but not daily snapshot

**Debug**:
```python
# Check intraday
result = conn.execute(text("SELECT COUNT(*) FROM correlations_intraday"))
print(f"Intraday rows: {result.scalar()}")

# Check daily
result = conn.execute(text("SELECT COUNT(*) FROM correlations_daily"))
print(f"Daily rows: {result.scalar()}")

# Check which snapshots exist
result = conn.execute(text(
    "SELECT snapshot_date, index_name, COUNT(*) FROM correlations_daily "
    "GROUP BY snapshot_date, index_name ORDER BY snapshot_date DESC"
))
for row in result:
    print(f"{row.snapshot_date} {row.index_name}: {row.count} rows")
```

**Solution**:
1. Verify backfill runs at a time within snapshot window
2. Check that `_should_write_daily_snapshot()` logic is correct
3. Run backfill again with as_of closer to market close time

---

### Problem: Real-time job fails with "AnalyticsEngine not found"

**Cause**: AnalyticsEngine import path incorrect

**Solution**:
```bash
# Specify custom path
python jobs/compute_correlations_realtime.py \
  --db_url postgresql://user:pass@localhost/analytics \
  --analytics_engine_path /absolute/path/to/analytics.py

# Or fix import in compute_correlations_realtime.py line ~21
# Change: from src.analytics import AnalyticsEngine
# To:     from your_package.your_module import AnalyticsEngine
```

---

## Monitoring & Health Checks

### Check job status
```bash
# Real-time job last run
tail -20 /var/log/correlations/realtime.log | grep -E "SUCCESS|FAILED|complete"

# Cleanup job last run
tail -20 /var/log/correlations/cleanup.log

# Count intraday rows added today
psql -U postgres -d analytics -c "
SELECT COUNT(*) as rows_today
FROM correlations_intraday
WHERE as_of_datetime::date = CURRENT_DATE
"

# Check snapshot coverage
psql -U postgres -d analytics -c "
SELECT 
  snapshot_date,
  index_name,
  COUNT(DISTINCT snapshot_type) as snapshot_types,
  COUNT(*) as total_rows
FROM correlations_daily
WHERE snapshot_date >= CURRENT_DATE - INTERVAL '7 days'
GROUP BY snapshot_date, index_name
ORDER BY snapshot_date DESC, index_name
"
```

### Verify retention policy
```bash
psql -U postgres -d analytics -c "
SELECT 
  MIN(as_of_datetime) as oldest_intraday,
  MAX(as_of_datetime) as newest_intraday,
  CURRENT_TIMESTAMP - MIN(as_of_datetime) as age
FROM correlations_intraday
"
```

---

## Performance Tips

### Speed up backfill
```bash
# Run multiple date ranges in parallel
python jobs/backfill_correlations.py --from_date 2024-01-01 --to_date 2024-01-15 --portfolio SPX &
python jobs/backfill_correlations.py --from_date 2024-01-16 --to_date 2024-01-31 --portfolio SPX &
wait
```

### Monitor real-time job latency
```bash
# Add timing to logs
time python jobs/compute_correlations_realtime.py --db_url postgresql://... 2>&1 | tail -5
# Look for "real 0m2.345s" to see total execution time
```

### Optimize database queries
```python
# Add database indexes (in schema setup)
CREATE INDEX idx_ivols_live_lookup ON ivols_live(portfolio, symbol, timestamp DESC);
CREATE INDEX idx_svols_live_lookup ON svols_live(symbol, timestamp DESC);
CREATE INDEX idx_weights_lookup ON index_weights(portfolio, last_updated DESC);
```

---

## Getting Help

**Documentation**:
- README.md – Quick start and examples
- docs.md – Deep dive into design and implementation
- IMPLEMENTATION_SUMMARY.md – What's built, what's next

**Code Comments**:
- Every class has a docstring explaining purpose
- Every function has docstring with Args/Returns/Examples
- Complex logic has inline comments

**Debug Mode**:
```bash
# Enable debug logging for any job
python jobs/backfill_correlations.py --log_level DEBUG
python jobs/compute_correlations_realtime.py --log_level DEBUG
python jobs/cleanup_old_data.py --log_level DEBUG
```

