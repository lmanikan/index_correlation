# Configuration for Results Storage

from dataclasses import dataclass, field
from datetime import time
from typing import Dict


@dataclass
class DailySnapshotConfig:
    """Configuration for daily correlation snapshot behavior."""
    
    snapshot_time: time = field(default_factory=lambda: time(16, 0))
    """Time of day to write daily snapshot (default: 4 PM market close)"""
    
    tolerance_minutes: int = 30
    """Tolerance around snapshot_time in minutes (±30 min = 3:30-4:30 PM)"""
    
    snapshot_type: str = "close"
    """Type label for snapshot (e.g., 'close', 'open', 'intraday')"""


@dataclass
class CorrelationStorageConfig:
    """Configuration for correlation storage behavior."""
    
    five_min_retention_days: int = 31
    """Retention period for 5-minute intraday data"""
    
    daily_snapshot: DailySnapshotConfig = field(default_factory=DailySnapshotConfig)
    """Default daily snapshot configuration"""
    
    index_specific_snapshots: Dict[str, DailySnapshotConfig] = field(default_factory=dict)
    """Override daily snapshot config per index portfolio name.
    
    Example:
        index_specific_snapshots={
            "SPX_CORR": DailySnapshotConfig(snapshot_time=time(21, 0), tolerance_minutes=15),  # 9 PM UTC
            "DAX_CORR": DailySnapshotConfig(snapshot_time=time(16, 30), tolerance_minutes=10),  # 4:30 PM UTC
            "NIFTY_CORR": DailySnapshotConfig(snapshot_time=time(9, 0), tolerance_minutes=30),  # 9 AM UTC
        }
    """
    
    def get_snapshot_config(self, index_name: str) -> DailySnapshotConfig:
        """Get snapshot config for specific index, with fallback to default.
        
        Args:
            index_portfolio: Index portfolio name (e.g., "SPX_CORR")
        
        Returns:
            DailySnapshotConfig for this index or default
        """
        return self.index_specific_snapshots.get(index_name, self.daily_snapshot)


@dataclass
class SensitivityStorageConfig:
    """Configuration for sensitivity storage behavior."""
    
    keep_latest_only: bool = True
    """Whether to keep only latest sensitivity (vs. history)"""


@dataclass
class ResultsStorageConfig:
    """Master configuration for results storage layer."""
    
    correlation: CorrelationStorageConfig = field(default_factory=CorrelationStorageConfig)
    """Correlation storage configuration"""
    
    sensitivity: SensitivityStorageConfig = field(default_factory=SensitivityStorageConfig)
    """Sensitivity storage configuration"""


# Default production configuration (global defaults)
DEFAULT_RESULTS_STORAGE_CONFIG = ResultsStorageConfig(
    correlation=CorrelationStorageConfig(
        five_min_retention_days=31,
        daily_snapshot=DailySnapshotConfig(
            snapshot_time=time(16, 0),
            tolerance_minutes=30,
            snapshot_type="close",
        ),
        index_specific_snapshots={},  # No index-specific overrides by default
    ),
    sensitivity=SensitivityStorageConfig(
        keep_latest_only=True,
    ),
)


# Example: Multi-region configuration with index-specific snapshot times
MULTI_REGION_CONFIG = ResultsStorageConfig(
    correlation=CorrelationStorageConfig(
        five_min_retention_days=31,
        daily_snapshot=DailySnapshotConfig(
            snapshot_time=time(16, 0),  # Default: 4 PM UTC (US market close)
            tolerance_minutes=30,
            snapshot_type="close",
        ),
        index_specific_snapshots={
            # US indices: 4 PM UTC (16:00)
            "SPX_CORR": DailySnapshotConfig(
                snapshot_time=time(21, 0),  # 9 PM UTC (4 PM ET)
                tolerance_minutes=15,
                snapshot_type="close",
            ),
            # European indices: afternoon close
            "STOXX_CORR": DailySnapshotConfig(
                snapshot_time=time(16, 30),  # 4:30 PM UTC (CET market close)
                tolerance_minutes=10,
                snapshot_type="close",
            ),
            "DAX_CORR": DailySnapshotConfig(
                snapshot_time=time(16, 30),  # 4:30 PM UTC (CET market close)
                tolerance_minutes=10,
                snapshot_type="close",
            ),
            # Asian indices: morning/early afternoon UTC
            "NIFTY_CORR": DailySnapshotConfig(
                snapshot_time=time(9, 0),  # 9 AM UTC (3:30 PM IST close)
                tolerance_minutes=30,
                snapshot_type="close",
            ),
            "HSI_CORR": DailySnapshotConfig(
                snapshot_time=time(8, 0),  # 8 AM UTC (4 PM HKT close)
                tolerance_minutes=20,
                snapshot_type="close",
            ),
        },
    ),
    sensitivity=SensitivityStorageConfig(
        keep_latest_only=True,
    ),
)