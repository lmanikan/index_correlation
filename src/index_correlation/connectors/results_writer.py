# Abstract Interface for Results Writer

from abc import ABC, abstractmethod
from datetime import datetime, time
from index_correlation.core.data_models import TrialResults
from index_correlation.config.results_config import ResultsStorageConfig


class WriterException(Exception):
    """Base exception for writer errors."""
    pass


class WriterConnectionError(WriterException):
    """Failed to connect to database."""
    pass


class WriterTableError(WriterException):
    """Failed to create or access table."""
    pass


class WriterWriteError(WriterException):
    """Failed to write data to database."""
    pass


def _should_write_daily_snapshot(
    index: str,
    as_of: datetime,
    config: ResultsStorageConfig,
) -> bool:
    """
    Check if current time is within index-specific snapshot window.
    
    Each index has its own snapshot time (e.g., SPX at 9 PM UTC, DAX at 4:30 PM UTC).
    This function checks if `as_of` falls within the tolerance window for that index.
    
    Args:
        index_portfolio: Index portfolio name (e.g., "SPX_CORR", "DAX_CORR")
        as_of: Current time (typically datetime.utcnow())
        config: Storage configuration with index-specific snapshot settings
    
    Returns:
        True if within snapshot window, False otherwise
    
    Example:
        >>> from datetime import datetime, time
        >>> from index_correlation.config.results_config import MULTI_REGION_CONFIG
        >>> 
        >>> # SPX snapshots at 9 PM UTC ±15 min (8:45-9:15 PM)
        >>> at_8_55_pm = datetime(2025, 12, 21, 20, 55)
        >>> result = _should_write_daily_snapshot("SPX_CORR", at_8_55_pm, MULTI_REGION_CONFIG)
        >>> print(result)  # True (within window)
        >>> 
        >>> at_4_00_pm = datetime(2025, 12, 21, 16, 0)
        >>> result = _should_write_daily_snapshot("SPX_CORR", at_4_00_pm, MULTI_REGION_CONFIG)
        >>> print(result)  # False (outside window)
    """
    # Get snapshot config for this index
    snapshot_config = config.correlation.get_snapshot_config(index)
    
    # Extract snapshot time and tolerance
    snapshot_time: time = snapshot_config.snapshot_time
    tolerance_minutes: int = snapshot_config.tolerance_minutes
    
    # Get current time of day
    current_time: time = as_of.time()
    
    # Convert to minutes since midnight for comparison
    snapshot_minutes = snapshot_time.hour * 60 + snapshot_time.minute
    current_minutes = current_time.hour * 60 + current_time.minute
    
    # Check if within tolerance window
    time_diff = abs(current_minutes - snapshot_minutes)
    
    # Handle day-wrap case (e.g., snapshot at 23:50, current time 00:10)
    if time_diff > 12 * 60:  # More than 12 hours diff = probably wrapped
        time_diff = 24 * 60 - time_diff
    
    return time_diff <= tolerance_minutes


class ResultsWriter(ABC):
    """
    Abstract interface for writing trial results to database.
    
    Subclasses must implement:
    - ensure_tables_exist(): Create tables if needed
    - write_trial(trial, as_of): Write correlations, sensitivities, snapshots
    - cleanup_old_intraday(as_of): Delete old data by retention period
    
    Data Flow:
    1. write_trial() appends 5-min intraday correlations (always)
    2. write_trial() upserts latest sensitivities (always)
    3. write_trial() upserts daily snapshot IF:
       a. Current time is within index-specific snapshot window
       b. Snapshot doesn't already exist for this (date, type, index)
    4. cleanup_old_intraday() runs daily to manage disk space
    
    Example:
        >>> from sqlalchemy import create_engine
        >>> from index_correlation.storage.postgres_writer import PostgresResultsWriter
        >>> from index_correlation.config.results_config import MULTI_REGION_CONFIG
        >>> from datetime import datetime
        >>> 
        >>> engine = create_engine("postgresql://localhost/analytics_db")
        >>> writer = PostgresResultsWriter(engine, config=MULTI_REGION_CONFIG)
        >>> 
        >>> # One-time setup
        >>> writer.ensure_tables_exist()
        >>> 
        >>> # Every 5 minutes
        >>> trial = engine.compute_all_terms(packages)
        >>> writer.write_trial(trial, as_of=datetime.utcnow())
        >>> 
        >>> # Daily cleanup
        >>> deleted = writer.cleanup_old_intraday(as_of=datetime.utcnow())
        >>> print(f"Deleted {deleted} old rows")
    """
    
    @abstractmethod
    def ensure_tables_exist(self) -> None:
        """
        Create database tables if they don't exist.
        
        Must create three tables:
        - correlations_intraday: 5-min append-only
        - correlations_daily: daily snapshots (upsert)
        - sensitivities_latest: latest only (upsert)
        
        Should be idempotent (safe to call multiple times).
        
        Raises:
            WriterTableError: If table creation fails
            WriterConnectionError: If database connection fails
        """
        pass
    
    @abstractmethod
    def write_trial(self, trial: TrialResults, as_of: datetime) -> None:
        """
        Write trial results to database.
        
        Flow:
        1. Extract correlations and sensitivities from trial
        2. Append correlations to intraday table
        3. Upsert sensitivities to latest table
        4. IF within snapshot window and not already snapped today:
           - Upsert to daily snapshot table (once per index per day)
        5. Clean up old intraday data
        
        Args:
            trial: TrialResults from AnalyticsEngine
            as_of: Timestamp for this compute (typically datetime.utcnow())
        
        Raises:
            WriterWriteError: If write fails
            WriterConnectionError: If connection fails
        
        Example:
            >>> trial = engine.compute_all_terms(packages)
            >>> writer.write_trial(trial, as_of=datetime.utcnow())
            >>> # Data is now in database
        """
        pass
    
    @abstractmethod
    def cleanup_old_intraday(self, as_of: datetime) -> int:
        """
        Delete intraday correlations older than retention period.
        
        Retention period is configured in ResultsStorageConfig.
        Default: 31 days.
        
        Daily snapshots and latest sensitivities are NOT cleaned up.
        
        Args:
            as_of: Current timestamp (typically datetime.utcnow())
        
        Returns:
            Number of rows deleted
        
        Raises:
            WriterWriteError: If cleanup fails
        
        Example:
            >>> deleted = writer.cleanup_old_intraday(as_of=datetime.utcnow())
            >>> print(f"Deleted {deleted} rows older than 31 days")
        """
        pass
