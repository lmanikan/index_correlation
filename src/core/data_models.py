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
from src.core.models import WeightType,VolType,CorrelationSensitivity,ImpliedCorrelationResult
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
    
    def to_engine_args(self) -> dict:
        """
        Convert to AnalyticsEngine.compute() arguments.
        
        Merges weights_df with vols_df on (portfolio, symbol) per strike.
        """
        
        vols_df_by_strike = {}
        
        for strike in self.component_vols.strikes:
            # Get vols at this strike: [portfolio, symbol, volatility]
            vols_df = self.component_vols.get_vols_for_strike(strike)
            
            # Merge with weights on (portfolio, symbol)
            merged = self.weights.df.merge(
                vols_df,
                on=['portfolio', 'symbol'],
                how='left',
            )
            
            vols_df_by_strike[strike] = merged[['portfolio', 'symbol', 'weight', 'volatility']]
        
        return {
            'index': self.index.portfolio,
            'term': self.term,
            'index_volatility_by_strike': {
                strike: self.index_vol.get_vol_for_strike(strike)
                for strike in self.component_vols.strikes
            },
            'weights_df': self.weights.df[['portfolio', 'symbol', 'weight']],
            'vols_df_by_strike': vols_df_by_strike,
            'calculation_date': datetime.combine(self.date, datetime.min.time()),
        }
    
    def summary(self) -> str:
        """Quick summary"""
        status = "✓" if self.is_valid() else "✗"
        return (
            f"{status} {self.index.portfolio} {self.term} {self.date}: "
            f"{self.component_vols.num_components} components, "
            f"{len(self.component_vols.strikes)} strikes"
        )


# ============================================================================
# Output
# ============================================================================

@dataclass
class AnalyticsResult:
    """
    Aggregated analytics results for one term across all strikes.
    
    Contains correlation and sensitivity results for each strike.
    This is the primary output of the AnalyticsEngine for a single term.
    """
    
    index: Index                                    # Index object
    term: str                                       # Term label (e.g., "1M", "3M", "6M")
    as_of_date: date                                # Date of computation
    correlation_by_strike: Dict[float, ImpliedCorrelationResult]  # {strike → correlation result}
    sensitivities_by_strike: Dict[float, Tuple[List[CorrelationSensitivity], float, float]] = field(default_factory=dict)  # {strike → (component_sens, idx_delta, idx_elast)}
    
    # =========================================================================
    # PROPERTIES
    # =========================================================================
    
    @property
    def strikes(self) -> List[float]:
        """Sorted list of strikes with correlation data."""
        return sorted(self.correlation_by_strike.keys())
    
    @property
    def num_strikes(self) -> int:
        """Number of strikes computed."""
        return len(self.correlation_by_strike)
    
    @property
    def has_sensitivities(self) -> bool:
        """Whether sensitivities were computed for any strike."""
        return len(self.sensitivities_by_strike) > 0
    
    # =========================================================================
    # ACCESSORS
    # =========================================================================
    
    def get_correlation(self, strike: float) -> Optional[float]:
        """
        Get implied correlation at strike.
        
        Args:
            strike: Strike level (e.g., 1.00)
        
        Returns:
            Implied correlation (float) or None if not computed
        """
        result = self.correlation_by_strike.get(strike)
        return result.implied_correlation if result else None
    
    def get_index_vol(self, strike: float) -> Optional[float]:
        """
        Get index volatility at strike.
        
        Args:
            strike: Strike level
        
        Returns:
            Index volatility or None if not computed
        """
        result = self.correlation_by_strike.get(strike)
        return result.index_volatility if result else None
    
    def get_sensitivities(
        self, strike: float
    ) -> Optional[Tuple[List[CorrelationSensitivity], float, float]]:
        """
        Get sensitivities at strike.
        
        Args:
            strike: Strike level
        
        Returns:
            Tuple of (component_sensitivities, index_vol_delta, index_vol_elasticity)
            or None if not computed
        """
        return self.sensitivities_by_strike.get(strike)
    
    def get_component_sensitivity(self, strike: float, symbol: str) -> Optional[CorrelationSensitivity]:
        """
        Get sensitivity for specific component at strike.
        
        Args:
            strike: Strike level
            symbol: Component symbol
        
        Returns:
            CorrelationSensitivity or None
        """
        sens_tuple = self.sensitivities_by_strike.get(strike)
        if sens_tuple is None:
            return None
        
        component_sens, _, _ = sens_tuple
        for sens in component_sens:
            if sens.symbol == symbol:
                return sens
        
        return None
    
    # =========================================================================
    # EXPORT TO DATAFRAME
    # =========================================================================
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert correlations to DataFrame for export/analysis.
        
        Returns:
            DataFrame with columns:
              - index: Portfolio name
              - term: Term label
              - strike: Strike level
              - implied_correlation: ρ value
              - index_volatility: σ_idx
              - num_components: Number of components
        """
        rows = []
        for strike, corr_result in self.correlation_by_strike.items():
            rows.append({
                'index': self.index.portfolio,
                'term': self.term,
                'strike': strike,
                'implied_correlation': corr_result.implied_correlation,
                'index_volatility': corr_result.index_volatility,
                'num_components': corr_result.num_components,
            })
        return pd.DataFrame(rows)
    
    def sensitivities_to_dataframe(self) -> pd.DataFrame:
        """
        Convert sensitivities to DataFrame for export/analysis.
        
        One row per component per strike, plus one row for index vol sensitivity.
        
        Returns:
            DataFrame with columns:
              - index: Portfolio name
              - term: Term label
              - strike: Strike level
              - symbol: Component symbol or 'INDEX_VOL'
              - delta: dρ/dσ (sensitivity)
              - elasticity: (dρ/ρ)/(dσ/σ) in percent
              - type: 'component' or 'index_vol'
        """
        if not self.sensitivities_by_strike:
            return pd.DataFrame()
        
        rows = []
        for strike, (comp_sens, idx_delta, idx_elast) in self.sensitivities_by_strike.items():
            # Component sensitivities
            for sens in comp_sens:
                rows.append({
                    'index': self.index.portfolio,
                    'term': self.term,
                    'strike': strike,
                    'symbol': sens.symbol,
                    'delta': sens.delta,
                    'elasticity': sens.elasticity,
                    'type': 'component',
                })
            
            # Index vol sensitivity
            rows.append({
                'index': self.index.portfolio,
                'term': self.term,
                'strike': strike,
                'symbol': 'INDEX_VOL',
                'delta': idx_delta,
                'elasticity': idx_elast,
                'type': 'index_vol',
            })
        
        return pd.DataFrame(rows)
    
    # =========================================================================
    # SUMMARY & DISPLAY
    # =========================================================================
    
    def summary(self) -> str:
        """
        Human-readable summary of results.
        
        Returns:
            Summary string with correlation range and data availability
        """
        if not self.correlation_by_strike:
            return f"{self.index.portfolio} {self.term}: NO DATA"
        
        corrs = [r.implied_correlation for r in self.correlation_by_strike.values()]
        sens_count = len(self.sensitivities_by_strike)
        
        sens_str = f", {sens_count} with sensitivities" if sens_count > 0 else ", no sensitivities"
        
        return (
            f"{self.index.portfolio} {self.term}: {len(corrs)} strikes, "
            f"ρ ∈ [{min(corrs):.4f}, {max(corrs):.4f}]{sens_str}"
        )
    
    def __str__(self) -> str:
        """String representation (same as summary)."""
        return self.summary()
    
    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"AnalyticsResult(index={self.index.portfolio}, term={self.term}, "
            f"date={self.as_of_date}, strikes={len(self.correlation_by_strike)}, "
            f"sensitivities={len(self.sensitivities_by_strike)})"
        )
 
@dataclass
class TrialResults:
    """
    Aggregated results for all terms computed in a single trial.
    
    This is the primary output of AnalyticsEngine.compute_all_terms().
    Contains results for multiple terms (1M, 3M, 6M, etc.) for a single index.
    """
    
    index: Index                                    # Index object (same for all terms)
    date: date                                      # Date of computation
    terms: List[str]                                # List of terms (e.g., ["1M", "3M", "6M"])
    results_by_term: Dict[str, Optional[AnalyticsResult]]  # {term → AnalyticsResult or None}
    
    # =========================================================================
    # PROPERTIES
    # =========================================================================
    
    @property
    def num_terms(self) -> int:
        """Total number of terms requested."""
        return len(self.terms)
    
    @property
    def num_successful_terms(self) -> int:
        """Number of terms that computed successfully."""
        return sum(1 for result in self.results_by_term.values() if result is not None)
    
    @property
    def num_failed_terms(self) -> int:
        """Number of terms that failed to compute."""
        return self.num_terms - self.num_successful_terms
    
    @property
    def successful_terms(self) -> List[str]:
        """List of terms that computed successfully."""
        return [term for term in self.terms if self.results_by_term.get(term) is not None]
    
    @property
    def failed_terms(self) -> List[str]:
        """List of terms that failed to compute."""
        return [term for term in self.terms if self.results_by_term.get(term) is None]
    
    @property
    def all_terms_successful(self) -> bool:
        """Whether all terms computed successfully."""
        return self.num_failed_terms == 0
    
    @property
    def total_strikes_computed(self) -> int:
        """Total number of strikes across all successful terms."""
        total = 0
        for result in self.results_by_term.values():
            if result is not None:
                total += result.num_strikes
        return total
    
    # =========================================================================
    # ACCESSORS
    # =========================================================================
    
    def get_result(self, term: str) -> Optional[AnalyticsResult]:
        """
        Get AnalyticsResult for specific term.
        
        Args:
            term: Term label (e.g., "1M")
        
        Returns:
            AnalyticsResult or None if term failed or not found
        """
        return self.results_by_term.get(term)
    
    def get_correlation_at_strike(self, term: str, strike: float) -> Optional[float]:
        """
        Get implied correlation at specific (term, strike).
        
        Args:
            term: Term label
            strike: Strike level
        
        Returns:
            Implied correlation or None if not found
        """
        result = self.results_by_term.get(term)
        if result is None:
            return None
        return result.get_correlation(strike)
    
    def get_sensitivities_at_strike(self, term: str, strike: float) -> Optional[tuple]:
        """
        Get sensitivities at specific (term, strike).
        
        Args:
            term: Term label
            strike: Strike level
        
        Returns:
            Tuple of (component_sens, idx_delta, idx_elast) or None
        """
        result = self.results_by_term.get(term)
        if result is None:
            return None
        return result.get_sensitivities(strike)
    
    # =========================================================================
    # EXPORT TO DATAFRAME
    # =========================================================================
    
    def to_dataframe(self) -> pd.DataFrame:
        """
        Convert all correlations to single DataFrame for export.
        
        Returns:
            DataFrame with columns:
              - index: Portfolio name
              - date: Computation date
              - term: Term label
              - strike: Strike level
              - implied_correlation: ρ value
              - index_volatility: σ_idx
              - num_components: Number of components
        """
        all_rows = []
        
        for term, result in self.results_by_term.items():
            if result is None:
                continue
            
            term_df = result.to_dataframe()
            term_df['date'] = self.date
            all_rows.append(term_df)
        
        if not all_rows:
            return pd.DataFrame()
        
        return pd.concat(all_rows, ignore_index=True)
    
    def sensitivities_to_dataframe(self) -> pd.DataFrame:
        """
        Convert all sensitivities to single DataFrame for export.
        
        Returns:
            DataFrame with columns:
              - index: Portfolio name
              - date: Computation date
              - term: Term label
              - strike: Strike level
              - symbol: Component symbol or 'INDEX_VOL'
              - delta: dρ/dσ (sensitivity)
              - elasticity: (dρ/ρ)/(dσ/σ) in percent
              - type: 'component' or 'index_vol'
        """
        all_rows = []
        
        for term, result in self.results_by_term.items():
            if result is None:
                continue
            
            term_df = result.sensitivities_to_dataframe()
            if term_df.empty:
                continue
            
            term_df['date'] = self.date
            all_rows.append(term_df)
        
        if not all_rows:
            return pd.DataFrame()
        
        return pd.concat(all_rows, ignore_index=True)
    
    # =========================================================================
    # CORRELATION TERM STRUCTURE
    # =========================================================================
    
    def term_structure_dataframe(self, strike: float) -> pd.DataFrame:
        """
        Get term structure (ρ across terms) at specific strike.
        
        Args:
            strike: Strike level
        
        Returns:
            DataFrame with columns:
              - index: Portfolio name
              - date: Computation date
              - term: Term label
              - strike: Strike level
              - implied_correlation: ρ value
        """
        rows = []
        
        for term in self.terms:
            rho = self.get_correlation_at_strike(term, strike)
            
            if rho is None:
                continue
            
            rows.append({
                'index': self.index.portfolio,
                'date': self.date,
                'term': term,
                'strike': strike,
                'implied_correlation': rho,
            })
        
        return pd.DataFrame(rows)
    
    def all_term_structures_dataframe(self) -> pd.DataFrame:
        """
        Get term structures for all strikes.
        
        Returns:
            DataFrame with one row per (term, strike) combination.
            Columns: index, date, term, strike, implied_correlation
        """
        all_rows = []
        
        for term, result in self.results_by_term.items():
            if result is None:
                continue
            
            for strike in result.strikes:
                rho = result.get_correlation(strike)
                all_rows.append({
                    'index': self.index.portfolio,
                    'date': self.date,
                    'term': term,
                    'strike': strike,
                    'implied_correlation': rho,
                })
        
        return pd.DataFrame(all_rows)
    
    # =========================================================================
    # SUMMARY & DISPLAY
    # =========================================================================
    
    def summary(self) -> str:
        """
        Human-readable summary of results.
        
        Returns:
            Summary string with term status and data availability
        """
        successful = ", ".join(self.successful_terms)
        failed = ", ".join(self.failed_terms)
        
        summary_lines = [
            f"TrialResults for {self.index.portfolio} ({self.date})",
            f"  Successful terms ({self.num_successful_terms}/{self.num_terms}): {successful or 'none'}",
        ]
        
        if self.failed_terms:
            summary_lines.append(f"  Failed terms ({self.num_failed_terms}): {failed}")
        
        summary_lines.append(f"  Total strikes computed: {self.total_strikes_computed}")
        
        return "\n".join(summary_lines)
    
    def detail_summary(self) -> str:
        """
        Detailed summary with per-term statistics.
        
        Returns:
            Detailed summary string
        """
        lines = [
            f"TrialResults: {self.index.portfolio} on {self.date}",
            f"Total terms: {self.num_terms} (successful: {self.num_successful_terms}, failed: {self.num_failed_terms})",
            "",
        ]
        
        for term in self.terms:
            result = self.results_by_term.get(term)
            if result is None:
                lines.append(f"  {term}: FAILED")
            else:
                lines.append(f"  {term}: {result.summary()}")
        
        return "\n".join(lines)
    
    def __str__(self) -> str:
        """String representation (summary)."""
        return self.summary()
    
    def __repr__(self) -> str:
        """Detailed representation."""
        return (
            f"TrialResults(index={self.index.portfolio}, date={self.date}, "
            f"terms={self.num_terms}, successful={self.num_successful_terms}, "
            f"strikes={self.total_strikes_computed})"
        )


# =========================================================================
# USAGE EXAMPLES
# =========================================================================

"""
# Create TrialResults (from AnalyticsEngine.compute_all_terms())
trial = TrialResults(
    index=index,
    date=date(2025, 12, 20),
    terms=["1M", "3M", "6M"],
    results_by_term={
        "1M": AnalyticsResult(...),
        "3M": AnalyticsResult(...),
        "6M": None,  # Failed
    }
)

# Summary
print(trial.summary())
# Output:
# TrialResults for SPX_CORR (2025-12-20)
#   Successful terms (2/3): 1M, 3M
#   Failed terms (1): 6M
#   Total strikes computed: 10

# Detailed summary
print(trial.detail_summary())
# Output:
# TrialResults: SPX_CORR on 2025-12-20
# Total terms: 3 (successful: 2, failed: 1)
#   1M: SPX_CORR 1M: 5 strikes, ρ ∈ [0.6520, 0.6850], 5 with sensitivities
#   3M: SPX_CORR 3M: 5 strikes, ρ ∈ [0.6300, 0.6600], 5 with sensitivities
#   6M: FAILED

# Properties
print(trial.num_terms)                          # 3
print(trial.num_successful_terms)               # 2
print(trial.num_failed_terms)                   # 1
print(trial.successful_terms)                   # ["1M", "3M"]
print(trial.failed_terms)                       # ["6M"]
print(trial.all_terms_successful)               # False
print(trial.total_strikes_computed)             # 10 (5 + 5)

# Accessors
result_1m = trial.get_result("1M")              # AnalyticsResult
rho_atm_1m = trial.get_correlation_at_strike("1M", 1.00)  # 0.68
sens = trial.get_sensitivities_at_strike("1M", 1.00)  # (components, idx_delta, idx_elast)

# Export
corr_df = trial.to_dataframe()
# Columns: index, date, term, strike, implied_correlation, index_volatility, num_components
# Rows: All strikes across all successful terms

sens_df = trial.sensitivities_to_dataframe()
# Columns: index, date, term, strike, symbol, delta, elasticity, type
# Rows: All sensitivities across all successful terms

# Term structures (correlations at fixed strike across all terms)
term_struct_atm = trial.term_structure_dataframe(1.00)
# Rows: (1M, 1.00), (3M, 1.00)
# Shows how correlation changes with term at fixed strike

all_term_structs = trial.all_term_structures_dataframe()
# All term structures for all strikes

# Save to CSV
corr_df.to_csv("trial_correlations.csv", index=False)
sens_df.to_csv("trial_sensitivities.csv", index=False)
term_struct_atm.to_csv("term_structure_atm.csv", index=False)
all_term_structs.to_csv("all_term_structures.csv", index=False)
"""

   
