# src/core/data_models.py
"""
Core data models for implied correlation analytics.

Architecture:
  - Index: Static definition of a basket/portfolio + strikes to analyze
  - IndexWeights: Multi-portfolio component composition [portfolio, symbol, weight]
  - VolSurfaceUniverse: All available (portfolio, symbol, strike) → vol for ONE term
  - ComponentVolatilities: Projected subset for index components, ONE term, filtered to index strikes
  - DataPackage: Complete input for analytics engine (index, term, date)
  - AnalyticsResult: Complete output (correlations, sensitivities per strike)
  - TrialResults: Multi-term aggregation (term → AnalyticsResult)
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum
from typing import Dict, List, Optional, Tuple
from index_correlation.core.models import (
    WeightType,
    VolType,
    CorrelationSensitivity,
    ImpliedCorrelationResult,
    TransformationInput,
)
import pandas as pd


# ============================================================================
# Enums
# ============================================================================

class DataSourceType(Enum):
    """Data source origin"""
    POSTGRES = "POSTGRES"
    BIGQUERY = "BIGQUERY"
    CSV_FILE = "CSV_FILE"

@dataclass
class Index:
    portfolio: str
    symbol: str
    weight_type: WeightType
    num_components: int
    vol_type: VolType
    strikes: List

    # auto-generated, not passed to __init__
    name: str = field(init=False)

    def __post_init__(self):
        if self.weight_type == WeightType.EQUAL_WEIGHT and self.num_components <= 0:
            raise ValueError("EQUAL_WEIGHT index must have num_components > 0")
        self.name = f"{self.portfolio}_{self.symbol}"

# ============================================================================
# Weights
# ============================================================================

@dataclass
class IndexWeights:
    """
    Index composition: [portfolio, symbol, weight].
    
    Multi-portfolio index example:
      portfolio    | symbol          | weight
      SPX_OPT      | SPY US Equity   | 0.25
      SPX_OPT      | IVV US Equity   | 0.15
      SPX_FUT      | ESZ5 Index      | 0.60
    """
    
    df: pd.DataFrame  # Columns: [portfolio, symbol, weight]
    source: DataSourceType
    as_of_date: date
    
    # Extracted metadata
    component_pairs: List[Tuple[str, str]] = field(default_factory=list)
    num_components: int = 0
    total_weight: float = 0.0
    
    def __post_init__(self):
        """Extract metadata"""
        if not self.df.empty:
            self.component_pairs = [
                (row['portfolio'], row['symbol'])
                for _, row in self.df.iterrows()
            ]
            self.num_components = len(self.component_pairs)
            self.total_weight = self.df['weight'].sum()
    
    def is_valid(self) -> bool:
        """Check: non-empty, positive weights, ~100% allocation"""
        if self.df.empty:
            return False
        
        if not all(self.df['weight'] > 0):
            return False
        
        # Allow 99-101% for floating point
        if not (0.99 <= self.total_weight <= 1.01):
            return False
        
        return True
    
    def get_component_pairs(self) -> List[Tuple[str, str]]:
        """Return list of (portfolio, symbol) tuples"""
        return self.component_pairs
    
    def summary(self) -> str:
        """Quick summary"""
        return (
            f"IndexWeights({self.num_components} components, "
            f"total_weight={self.total_weight:.4f})"
        )


# ============================================================================
# Volatility Surfaces
# ============================================================================

@dataclass
class VolSurfaceUniverse:
    """
    Global volatility surface for ONE TERM across all (portfolio, symbol, strike).
    
    Raw data: [portfolio, symbol, term, strike, volatility]
    
    Extracted once per term with:
      term: str = "1M", "3M", etc. (parameter to extraction, not in key)
      vols: Dict[(portfolio, symbol, strike)] → volatility
    
    Example:
      (SPX_OPT, SPY, 0.95): 0.325
      (SPX_OPT, SPY, 1.00): 0.330
      (SPX_FUT, ESZ5, 1.00): 0.300
      ...
    """
    
    term: str  # "1M", "3M", "6M", "1Y", etc.
    as_of_date: date
    source: DataSourceType
    
    # Key structure: (portfolio, symbol, strike) → volatility
    # All entries are for self.term (not in key)
    vols: Dict[Tuple[str, str, float], float]
    
    # Extracted metadata
    portfolios: List[str] = field(default_factory=list)
    symbols: Dict[str, List[str]] = field(default_factory=dict)  # portfolio → symbols
    strikes: List[float] = field(default_factory=list)
    
    def __post_init__(self):
        """Extract unique portfolios, symbols, strikes"""
        if self.vols:
            self.portfolios = sorted(set(p for p, s, k in self.vols.keys()))
            self.strikes = sorted(set(k for p, s, k in self.vols.keys()))
            
            self.symbols = {}
            for p in self.portfolios:
                syms = sorted(set(s for (pf, s, k) in self.vols.keys() if pf == p))
                self.symbols[p] = syms
    
    def get_surface_for_components(
        self,
        component_pairs: List[Tuple[str, str]],
        strikes: List[float],
    ) -> "ComponentVolatilities":
        """
        Project to specific index components and strikes.
        
        Args:
            component_pairs: List[(portfolio, symbol)] from weights
            strikes: List[float] of moneyness levels from Index
        
        Returns:
            ComponentVolatilities with only the required (portfolio, symbol, strike) tuples
            
        Raises:
            ValueError if any required strike is not in universe
        """
        # Validate strikes are available
        available_strikes = set(self.strikes)
        required_strikes = set(strikes)
        missing_strikes = required_strikes - available_strikes
        
        if missing_strikes:
            raise ValueError(
                f"Missing strikes in {self.term} universe: {sorted(missing_strikes)}"
            )
        
        # Filter to only the (portfolio, symbol, strike) tuples needed
        filtered_vols = {
            (p, s, k): v
            for (p, s, k), v in self.vols.items()
            if (p, s) in set(component_pairs) and k in required_strikes
        }
        
        return ComponentVolatilities(
            term=self.term,
            as_of_date=self.as_of_date,
            source=self.source,
            vols=filtered_vols,
        )
    
    def summary(self) -> str:
        """Quick summary"""
        return (
            f"VolSurfaceUniverse({self.term}: "
            f"{len(self.portfolios)} portfolios × "
            f"{sum(len(s) for s in self.symbols.values())} symbols × "
            f"{len(self.strikes)} strikes)"
        )


@dataclass
class ComponentVolatilities:
    """
    Volatilities for index components for ONE TERM, filtered to index strikes.
    
    Projection of VolSurfaceUniverse to the specific (portfolio, symbol)
    pairs that the index uses, AND the specific strikes the index specifies.
    Created by VolSurfaceUniverse.get_surface_for_components(component_pairs, index.strikes).
    
    Key structure: (portfolio, symbol, strike) → volatility
    All vols are for self.term (not in key).
    All strikes are from Index.strikes (filtered in projection).
    
    Example (for SPX_CORR with 3 components and strikes=[0.95, 1.00]):
      (SPX_OPT, SPY, 0.95): 0.325
      (SPX_OPT, SPY, 1.00): 0.330
      (SPX_OPT, IVV, 0.95): 0.320
      (SPX_OPT, IVV, 1.00): 0.325
      (SPX_FUT, ESZ5, 0.95): 0.295
      (SPX_FUT, ESZ5, 1.00): 0.300
    """
    
    term: str  # "1M", "3M", etc.
    as_of_date: date
    source: DataSourceType
    
    # (portfolio, symbol, strike) → vol
    # ONLY for index components AND index strikes
    vols: Dict[Tuple[str, str, float], float]
    
    # Extracted metadata
    component_pairs: List[Tuple[str, str]] = field(default_factory=list)
    strikes: List[float] = field(default_factory=list)
    num_components: int = 0
    
    def __post_init__(self):
        """Extract unique strikes and component pairs"""
        if self.vols:
            self.strikes = sorted(set(k for (p, s, k) in self.vols.keys()))
            self.component_pairs = sorted(set((p, s) for (p, s, k) in self.vols.keys()))
            self.num_components = len(self.component_pairs)
        else:
            self.strikes = []
            self.component_pairs = []
            self.num_components = 0
    
    def get_vols_for_strike(self, strike: float) -> pd.DataFrame:
        """
        Get all component vols at this strike for this term.
        
        Returns:
            DataFrame with columns: [portfolio, symbol, volatility]
        """
        rows = [
            {"portfolio": p, "symbol": s, "volatility": v}
            for (p, s, k), v in self.vols.items()
            if k == strike
        ]
        return pd.DataFrame(rows)
    
    def validate_for_components(
        self,
        required_pairs: List[Tuple[str, str]],
    ) -> Tuple[bool, List[Tuple[str, str]]]:
        """
        Check if we have vols for all required (portfolio, symbol) pairs
        across all strikes.
        
        Returns:
            (all_present, missing_pairs)
        """
        have = set(self.component_pairs)
        required = set(required_pairs)
        missing = list(required - have)
        
        return len(missing) == 0, missing
    
    def is_valid(self) -> bool:
        """Check: non-empty, positive values"""
        if not self.vols:
            return False
        return all(v > 0 for v in self.vols.values())
    
    def summary(self) -> str:
        """Quick summary"""
        return (
            f"ComponentVolatilities({self.term}: "
            f"{self.num_components} components × "
            f"{len(self.strikes)} strikes)"
        )


# ============================================================================
# Index Vol
# ============================================================================
@dataclass
class IndexVolatility:
    """
    Volatility of the index itself, indexed by strike.
    
    Raw data: [portfolio, term, strike, volatility]
    
    Key: (strike) → volatility
    All vols are for self.term (not in key).
    
    Example (for SPX_CORR, 1M):
      0.90: 0.175
      0.95: 0.177
      1.00: 0.180
      1.05: 0.182
      1.10: 0.184
    """
    
    portfolio: str
    term: str
    source: DataSourceType
    as_of_date: date
    
    # (strike) → volatility [indexed by strike, ONE term]
    vols: Dict[float, float]
    
    # Extracted metadata
    strikes: List[float] = field(default_factory=list)
    
    def __post_init__(self):
        """Extract unique strikes"""
        if self.vols:
            self.strikes = sorted(self.vols.keys())
    
    def get_vol_for_strike(self, strike: float) -> Optional[float]:
        """Get index vol at specific strike"""
        return self.vols.get(strike)
    
    def is_valid(self) -> bool:
        """Check: non-empty, positive values"""
        if not self.vols:
            return False
        return all(v > 0 for v in self.vols.values())
    
    def summary(self) -> str:
        """Quick summary"""
        return (
            f"IndexVol({self.portfolio} {self.term}: "
            f"{len(self.strikes)} strikes)"
        )


# ============================================================================
# Input Package
# ============================================================================

@dataclass
class DataPackage:
    """
    Complete input for analytics engine for ONE (index, term, date).
    
    Created by DataLoader.load_all_terms(), one per term.
    Consumed by AnalyticsEngine.compute_all_terms().
    """
    
    index: Index
    date: date
    term: str
    
    weights: IndexWeights
    component_vols: ComponentVolatilities
    index_vol: IndexVolatility
    
    missing_vol_symbols: List[Tuple[str, str]] = field(default_factory=list)
    
    def is_valid(self) -> bool:
        """Check all components valid and strikes match"""
        strikes_match = set(self.component_vols.strikes) == set(self.index.strikes)
        
        return (
            self.weights.is_valid() and
            self.component_vols.is_valid() and
            self.index_vol.is_valid() and
            len(self.missing_vol_symbols) == 0 and
            strikes_match
        )
    
    def to_dto_stream(self) -> List[TransformationInput]:
        """
        Slice DataPackage into atomic TransformationInput DTOs.
        """
        dtos = []
        calc_date = datetime.combine(self.date, datetime.min.time())
        for strike in self.component_vols.strikes:
            vols_df = self.component_vols.get_vols_for_strike(strike)
            # Merge with weights
            merged = self.weights.df.merge(
                vols_df, on=['portfolio', 'symbol'], how='inner'
            )
            # Create DTO
            dtos.append(TransformationInput(
                index_name=self.index.portfolio,
                term=self.term,
                strike=strike,
                index_volatility=self.index_vol.get_vol_for_strike(strike),
                weights=merged[['symbol', 'weight']],
                vols=merged[['symbol', 'volatility']],
                calculation_date=calc_date,
                weight_strategy=self.index.weight_type
            ))
        return dtos
    
    def summary(self) -> str:
        """Quick summary"""
        status = "✓" if self.is_valid() else "✗"
        return (
            f"{status} {self.index.portfolio} {self.term} {self.date}: "
            f"{self.component_vols.num_components} components, "
            f"{len(self.component_vols.strikes)} strikes"
        )
