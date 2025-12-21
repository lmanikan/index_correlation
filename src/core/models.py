"""Core data models for the implied correlation pipeline."""
import pandas as pd
from dataclasses import dataclass, field
from datetime import datetime,date

from typing import Optional, List, Tuple,Dict
from enum import Enum



class WeightType(str, Enum):
    MARKET_CAP = "MARKET_CAP"    # Use external weights
    EQUAL_WEIGHT = "EQUAL_WEIGHT"


class VolType(str, Enum):
    SVOL = "SVOL"   
    IVOL = "IVOL" 


@dataclass
class ImpliedCorrelationResult:
    """
    Standard output format for implied correlation calculations.

    text
    All teams use this exact format for consistency.
    """
    # Index identification
    index: str
    term: str
    strike: float

    # Core results
    implied_correlation: float
    index_volatility: float

    # Data provenance
    num_components: int
    calculation_date: datetime
    weight_type: WeightType = WeightType.MARKET_CAP

    # Quality metrics
    data_quality_flags: List[str] = field(default_factory=list)

    def __post_init__(self):
        """Validate after initialization."""
        if not (-1.0 <= self.implied_correlation <= 1.0):
            raise ValueError(
                f"Correlation {self.implied_correlation} outside [-1, 1]"
            )
        
        if self.num_components < 1:
            raise ValueError(f"num_components must be >= 1")
        
        if self.index_volatility < 0:
            raise ValueError("index_volatility cannot be negative")

    def is_quality_ok(self) -> bool:
        """Check if data quality is acceptable."""
        return len(self.data_quality_flags) == 0

    def summary(self) -> str:
        """Human-readable summary."""
        return (
            f"{self.index:8s} {self.term:4s}: "
            f"ρ={self.implied_correlation:7.4f} "
            f"(σ_idx={self.index_volatility:.4f}, "
            f"n={self.num_components:3d})"
        )

@dataclass
class CorrelationSensitivity:
    """
    Analytical sensitivity of implied correlation to component volatility changes.

    text
    For each symbol, stores dρ/dσ_i:
    How much does the correlation change if that component's vol moves by 1 unit?

    Elasticity: percentage change in correlation per 1% change in component vol.
    """
    symbol: str
    delta: float  # dρ/dσ_i (derivative)
    elasticity: float  # (dρ/ρ) / (dσ/σ) - percentage change in rho per 1% vol move

    def summary(self) -> str:
        return (
            f"{self.symbol:10s}: dρ/dσ={self.delta:8.6f}, "
            f"elasticity={self.elasticity:8.4f}%"
        )
 