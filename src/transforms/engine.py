# src/analytics/engine.py
"""
Analytics Framework: Extensible computation of market quantities.

Architecture:
  - BaseQuantity: Abstract interface for any computable quantity
  - ImpliedCorrelationQuantity: Strike-indexed correlation calculation
  - CorrelationSensitivityQuantity: Analytical gradients (dρ/dσ_i, dρ/dσ_idx)
  - AnalyticsEngine: Orchestrates multiple quantities across strikes and terms
"""

import logging
import numpy as np
import pandas as pd
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import Optional, List, Tuple, Any, Dict

from src.core.data_models import (
    DataPackage,
    Index,
    IndexWeights,
    ComponentVolatilities,
    IndexVolatility,
    TrialResults,
    AnalyticsResult
)
from src.core.models import (
    ImpliedCorrelationResult,
    CorrelationSensitivity,
    WeightType,
)
from src.core.exceptions import ValidationError, TransformationError

logger = logging.getLogger(__name__)


# ============================================================================
# QUANTITY ABSTRACTION
# ============================================================================

class BaseQuantity(ABC):
    """
    Abstract base class for any computable market quantity.
    
    Each quantity is independently computable at a specific (strike, term).
    """
    
    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this quantity."""
        pass
    
    @abstractmethod
    def compute(
        self,
        index: str,
        term: str,
        strike: float,
        index_volatility: float,
        weights_df: pd.DataFrame,
        vols_df: pd.DataFrame,
        calculation_date: datetime,
        weight_type: WeightType = WeightType.MARKET_CAP,
    ) -> Optional[Any]:
        """
        Compute this quantity at a specific strike.
        
        Args:
            index: Portfolio name
            term: Term label (e.g., "1M")
            strike: Strike level (e.g., 0.95, 1.00, 1.05)
            index_volatility: Index vol at this strike
            weights_df: DataFrame with [symbol, weight]
            vols_df: DataFrame with [symbol, volatility] (at this strike)
            calculation_date: Timestamp
            weight_type: Weight strategy
        
        Returns:
            Computed quantity (type-specific), or None if computation fails
        """
        pass


# ============================================================================
# DATA VALIDATION (SHARED ACROSS ALL QUANTITIES)
# ============================================================================

class DataValidator:
    """Validates weights and volatilities before computation."""
    
    MIN_COMPONENTS = 1  # For analytics, even 1 component is okay
    WEIGHT_SUM_TOLERANCE = 0.01  # ±1%
    MIN_VOL = 0.001  # 0.1%
    MAX_VOL = 5.0    # 500%
    
    @staticmethod
    def validate_weights(weights_df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate weight data structure and ranges."""
        flags = []
        
        if weights_df.empty:
            flags.append("No weights provided")
            return False, flags
        
        if 'weight' not in weights_df.columns:
            flags.append("Missing 'weight' column")
            return False, flags
        
        if 'symbol' not in weights_df.columns:
            flags.append("Missing 'symbol' column")
            return False, flags
        
        if weights_df['weight'].isna().any():
            flags.append("Weight column contains NaN values")
            return False, flags
        
        if (weights_df['weight'] < 0).any():
            flags.append("Negative weights found")
            return False, flags
        
        num_components = len(weights_df)
        if num_components < DataValidator.MIN_COMPONENTS:
            flags.append(
                f"Insufficient components: {num_components} < {DataValidator.MIN_COMPONENTS}"
            )
            return False, flags
        
        weight_sum = weights_df['weight'].sum()
        if abs(weight_sum - 1.0) > DataValidator.WEIGHT_SUM_TOLERANCE:
            flags.append(f"Weights sum to {weight_sum:.6f}, not 1.0")
            return False, flags
        
        logger.debug(
            f"✓ Weights valid: {num_components} components, sum={weight_sum:.6f}"
        )
        return True, flags
    
    @staticmethod
    def validate_vols(vols_df: pd.DataFrame) -> Tuple[bool, List[str]]:
        """Validate volatility data structure and ranges."""
        flags = []
        
        if vols_df.empty:
            flags.append("No volatilities provided")
            return False, flags
        
        if 'volatility' not in vols_df.columns:
            flags.append("Missing 'volatility' column")
            return False, flags
        
        if 'symbol' not in vols_df.columns:
            flags.append("Missing 'symbol' column")
            return False, flags
        
        if vols_df['volatility'].isna().any():
            flags.append("Volatility column contains NaN values")
            return False, flags
        
        if (vols_df['volatility'] < 0).any():
            flags.append("Negative volatilities found")
            return False, flags
        
        min_vol = vols_df['volatility'].min()
        max_vol = vols_df['volatility'].max()
        
        if min_vol < DataValidator.MIN_VOL:
            flags.append(f"Min volatility {min_vol:.4f} < {DataValidator.MIN_VOL}")
            return False, flags
        
        if max_vol > DataValidator.MAX_VOL:
            flags.append(f"Max volatility {max_vol:.4f} > {DataValidator.MAX_VOL}")
            return False, flags
        
        logger.debug(
            f"✓ Vols valid: {len(vols_df)} symbols, range=[{min_vol:.4f}, {max_vol:.4f}]"
        )
        return True, flags
    
    @staticmethod
    def validate_vols_coverage(
        weights_df: pd.DataFrame,
        vols_df: pd.DataFrame,
        index: str,
        term: str,
        strike: float,
    ) -> Tuple[bool, List[str]]:
        """Strict validation: all weight components must have vols at strike."""
        flags = []
        
        expected_symbols = set(weights_df['symbol'])
        have_vols = set(vols_df['symbol'])
        
        missing = expected_symbols - have_vols
        
        if missing:
            missing_list = sorted(list(missing))
            preview = missing_list[:5]
            if len(missing_list) > 5:
                preview_str = ", ".join(preview) + f", ... ({len(missing_list)} total)"
            else:
                preview_str = ", ".join(preview)
            
            msg = f"Missing vols for {len(missing)} components: {preview_str}"
            logger.warning(f"{index} {term} strike={strike}: {msg}")
            flags.append(msg)
            return False, flags
        
        logger.debug(
            f"{index} {term} strike={strike}: All {len(expected_symbols)} "
            f"components have vols"
        )
        return True, flags


# ============================================================================
# CONCRETE QUANTITY: IMPLIED CORRELATION
# ============================================================================

class ImpliedCorrelationQuantity(BaseQuantity):
    """
    Compute implied correlation from index and component volatilities.
    
    Formula (scalar form):
      v_i = w_i * σ_i
      Q = Σ v_i²
      S = Σ v_i
      A = σ_idx² - Q
      B = S² - Q
      ρ = A / B
    
    Valid for all strikes independently.
    """
    
    @property
    def name(self) -> str:
        return "implied_correlation"
    
    def compute(
        self,
        index: str,
        term: str,
        strike: float,
        index_volatility: float,
        weights_df: pd.DataFrame,
        vols_df: pd.DataFrame,
        calculation_date: datetime,
        weight_type: WeightType = WeightType.MARKET_CAP,
    ) -> Optional[ImpliedCorrelationResult]:
        """
        Compute implied correlation at specific strike.
        
        Args:
            index: Portfolio name
            term: Term label
            strike: Strike level (e.g., 0.95, 1.00, 1.05)
            index_volatility: Index vol at this strike
            weights_df: Component weights
            vols_df: Component vols at this strike
            calculation_date: Timestamp
            weight_type: Weight strategy
        
        Returns:
            ImpliedCorrelationResult or None
        """
        
        quality_flags = []
        
        # STEP 1: VALIDATE INPUTS
        weights_valid, weight_flags = DataValidator.validate_weights(weights_df)
        quality_flags.extend(weight_flags)
        
        if not weights_valid:
            logger.warning(f"{index} {term} strike={strike}: Weights invalid")
            return None
        
        vols_valid, vol_flags = DataValidator.validate_vols(vols_df)
        quality_flags.extend(vol_flags)
        
        if not vols_valid:
            logger.warning(f"{index} {term} strike={strike}: Vols invalid")
            return None
        
        # STEP 2: VOL COVERAGE CHECK
        coverage_valid, coverage_flags = DataValidator.validate_vols_coverage(
            weights_df=weights_df,
            vols_df=vols_df,
            index=index,
            term=term,
            strike=strike,
        )
        quality_flags.extend(coverage_flags)
        
        if not coverage_valid:
            return None
        
        # STEP 3: MERGE WEIGHTS + VOLS
        try:
            merged = pd.merge(
                weights_df[['symbol', 'weight']],
                vols_df[['symbol', 'volatility']],
                on='symbol',
                how='inner'
            )
            
            if merged.empty:
                logger.warning(f"{index} {term} strike={strike}: Merge resulted in empty")
                return None
        
        except Exception as e:
            logger.error(f"Merge failed: {e}")
            raise TransformationError(f"Cannot merge weights and vols: {e}")
        
        # STEP 4: CALCULATE CORRELATION
        try:
            w = merged['weight'].to_numpy(dtype='float64')
            sigma = merged['volatility'].to_numpy(dtype='float64')
            
            # Compute: v_i = w_i * σ_i
            v = w * sigma
            
            # Compute: S = Σ v_i, Q = Σ v_i²
            S = np.sum(v)
            Q = np.sum(v**2)
            
            # Compute: A = σ_idx² - Q, B = S² - Q
            index_var_sq = index_volatility ** 2
            A = index_var_sq - Q
            B = S**2 - Q
            
            # Solve: ρ = A / B
            if abs(B) < 1e-10:
                logger.warning(
                    f"{index} {term} strike={strike}: Denominator near zero ({B:.2e})"
                )
                return None
            
            implied_correlation = A / B
            implied_correlation = np.clip(implied_correlation, -1.0, 1.0)
            
            logger.debug(
                f"✓ {index} {term} strike={strike}: ρ={implied_correlation:.4f} "
                f"(σ_idx={index_volatility:.4f})"
            )
        
        except Exception as e:
            logger.error(f"Correlation calculation failed: {e}")
            raise TransformationError(f"Cannot calculate correlation: {e}")
        
        # STEP 5: CREATE RESULT
        try:
            result = ImpliedCorrelationResult(
                index=index,
                term=term,
                strike=strike,
                implied_correlation=float(implied_correlation),
                index_volatility=index_volatility,
                num_components=len(merged),
                calculation_date=calculation_date,
                weight_type=weight_type,
                data_quality_flags=quality_flags,
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Failed to create result object: {e}")
            raise TransformationError(f"Cannot create result: {e}")


# ============================================================================
# CONCRETE QUANTITY: CORRELATION SENSITIVITIES
# ============================================================================

class CorrelationSensitivityQuantity(BaseQuantity):
    """
    Compute sensitivity of implied correlation to component and index vol changes.
    
    Uses **analytical gradients** (one-pass computation):
    
    Component vol sensitivity (for each component i):
      dρ/dσ_i = -2*w_i / B² * [v_i*B + (S - v_i)*A]
    
    Index vol sensitivity (single value):
      dρ/dσ_idx = 2*σ_idx / B
    
    This is O(n) instead of O(n²) for finite differences.
    """
    
    @property
    def name(self) -> str:
        return "correlation_sensitivities"
    
    def compute(
        self,
        index: str,
        term: str,
        strike: float,
        index_volatility: float,
        weights_df: pd.DataFrame,
        vols_df: pd.DataFrame,
        calculation_date: datetime,
        weight_type: WeightType = WeightType.MARKET_CAP,
    ) -> Optional[Tuple[List[CorrelationSensitivity], float, float]]:
        """
        Compute correlation sensitivities at specific strike.
        
        Returns:
            Tuple of (component_sensitivities, index_vol_delta, index_vol_elasticity)
            or None on failure
        """
        
        # Validate inputs
        weights_valid, _ = DataValidator.validate_weights(weights_df)
        vols_valid, _ = DataValidator.validate_vols(vols_df)
        coverage_valid, _ = DataValidator.validate_vols_coverage(
            weights_df, vols_df, index, term, strike
        )
        
        if not (weights_valid and vols_valid and coverage_valid):
            logger.warning(f"{index} {term} strike={strike}: Data validation failed")
            return None
        
        # Merge
        try:
            merged = pd.merge(
                weights_df[['symbol', 'weight']],
                vols_df[['symbol', 'volatility']],
                on='symbol',
                how='inner'
            )
            
            if merged.empty:
                return None
        
        except Exception as e:
            logger.error(f"Merge failed: {e}")
            return None
        
        # Extract arrays
        symbols = merged['symbol'].values
        w = merged['weight'].to_numpy(dtype='float64')
        sigma = merged['volatility'].to_numpy(dtype='float64')
        
        # Precompute correlation components (one-pass)
        try:
            v = w * sigma
            S = np.sum(v)
            Q = np.sum(v**2)
            
            index_var_sq = index_volatility ** 2
            A = index_var_sq - Q
            B = S**2 - Q
            
            if abs(B) < 1e-10:
                logger.warning(f"{index} {term} strike={strike}: Denominator near zero")
                return None
            
            rho = A / B
            rho = np.clip(rho, -1.0, 1.0)
        
        except Exception as e:
            logger.error(f"Failed to precompute correlation: {e}")
            return None
        
        # ─────────────────────────────────────────────────────────────
        # COMPONENT VOL SENSITIVITIES (dρ/dσ_i)
        # ─────────────────────────────────────────────────────────────
        
        sensitivities = []
        
        try:
            for idx, symbol in enumerate(symbols):
                w_i = w[idx]
                v_i = v[idx]
                sigma_i = sigma[idx]
                
                # Analytical gradient formula
                # dρ/dσ_i = -2*w_i / B² * [v_i*B + (S - v_i)*A]
                numerator = v_i * B + (S - v_i) * A
                drho_dsigma = -2 * w_i * numerator / (B**2)
                
                # Elasticity: (dρ/ρ) / (dσ/σ) = dρ/dσ * σ / ρ
                if abs(rho) > 1e-10:
                    elasticity = (drho_dsigma * sigma_i / rho) * 100
                else:
                    elasticity = 0.0
                
                sens = CorrelationSensitivity(
                    symbol=symbol,
                    delta=float(drho_dsigma),
                    elasticity=float(elasticity),
                )
                
                sensitivities.append(sens)
                logger.debug(
                    f"  {symbol}: dρ/dσ={drho_dsigma:.6f}, "
                    f"elasticity={elasticity:.4f}%"
                )
            
            logger.debug(
                f"✓ {index} {term} strike={strike}: Computed "
                f"{len(sensitivities)} component sensitivities"
            )
        
        except Exception as e:
            logger.error(f"Failed to compute component sensitivities: {e}")
            return None
        
        # ─────────────────────────────────────────────────────────────
        # INDEX VOL SENSITIVITY (dρ/dσ_idx)
        # ─────────────────────────────────────────────────────────────
        
        try:
            # Analytical formula: dρ/dσ_idx = 2*σ_idx / B
            drho_d_index_vol = 2 * index_volatility / B
            
            # Elasticity
            if abs(A) > 1e-10:
                elasticity_index_vol = (2 * index_volatility**2 / A) * 100
            else:
                elasticity_index_vol = 0.0
            
            logger.debug(
                f"✓ Index vol sensitivity: dρ/dσ_idx={drho_d_index_vol:.6f}, "
                f"elasticity={elasticity_index_vol:.4f}%"
            )
            
            return sensitivities, float(drho_d_index_vol), float(elasticity_index_vol)
        
        except Exception as e:
            logger.error(f"Failed to compute index vol sensitivity: {e}")
            return None


# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

class AnalyticsEngine:
    """
    Orchestrates computation of multiple quantities across strikes and terms.
    
    Responsibilities:
      1. Load DataPackages from DataLoader
      2. For each strike in each term:
         a. Extract component vols at strike
         b. Extract index vol at strike
         c. Compute all quantities (correlation, sensitivities, etc.)
      3. Aggregate results per term
      4. Return TrialResults
    """
    
    def __init__(self, quantities: Optional[List[BaseQuantity]] = None):
        """
        Args:
            quantities: List of quantities to compute.
                       If None, defaults to ImpliedCorrelation only.
        """
        self.quantities = quantities or [ImpliedCorrelationQuantity()]
        logger.info(
            f"AnalyticsEngine initialized with {len(self.quantities)} quantities: "
            f"{[q.name for q in self.quantities]}"
        )
    
    def compute_all_terms(
        self,
        packages: Dict[str, Optional[DataPackage]],
    ) -> Optional[TrialResults]:
        """
        Compute all quantities for all terms.
        
        Process:
          1. Filter to valid packages
          2. For each term:
             a. Extract weights (same across all strikes)
             b. For each strike:
                i. Extract component and index vols at strike
                ii. Compute all quantities
          3. Aggregate into AnalyticsResult per term
          4. Return TrialResults
        
        Args:
            packages: Dict[term] → DataPackage (from AnalyticsDataLoader)
        
        Returns:
            TrialResults with results_by_term
        """
        
        try:
            # Filter to valid packages
            valid_packages = {
                term: pkg for term, pkg in packages.items()
                if pkg and pkg.is_valid()
            }
            
            if not valid_packages:
                logger.error("No valid DataPackages to compute")
                return None
            
            logger.info(
                f"Computing {len(valid_packages)} terms: "
                f"{list(valid_packages.keys())}"
            )
            
            # Extract index/date
            first_pkg = next(iter(valid_packages.values()))
            index = first_pkg.index
            as_of_date = first_pkg.date
            
            # Compute each term
            results_by_term = {}
            
            for term, pkg in valid_packages.items():
                logger.debug(f"Computing {index.portfolio} {term}")
                
                term_results = self._compute_term(pkg, as_of_date)
                
                if term_results is None:
                    logger.warning(f"Computation failed for {index.portfolio} {term}")
                    results_by_term[term] = None
                else:
                    results_by_term[term] = term_results
            
            # Check if any succeeded
            valid_results = {
                term: res for term, res in results_by_term.items()
                if res is not None
            }
            
            if not valid_results:
                logger.error(f"All terms failed for {index.portfolio}")
                return None
            
            # Aggregate
            trial = TrialResults(
                index=index,
                date=as_of_date,
                terms=sorted(valid_packages.keys()),
                results_by_term=results_by_term,
            )
            
            logger.info(
                f"Computed {len(valid_results)}/{len(valid_packages)} terms "
                f"for {index.portfolio}"
            )
            
            return trial
        
        except Exception as e:
            logger.error(f"Error in compute_all_terms: {e}")
            return None
    
    def _compute_term(
        self,
        pkg: DataPackage,
        as_of_date: date,
    ) -> Optional[AnalyticsResult]:
        """
        Compute all quantities for one term across all strikes.
        
        For each strike:
          1. Get weights (same for all strikes)
          2. Get component vols at this strike
          3. Get index vol at this strike
          4. Compute all quantities (correlation, sensitivities, etc.)
        """
        
        try:
            index = pkg.index
            term = pkg.index_vol.term
            
            # Convert weights to DataFrame
            weights_df = pkg.weights.df.copy()
            
            # For each strike, compute all quantities
            correlation_by_strike = {}
            sensitivities_by_strike = {}
            
            calc_date = datetime.fromordinal(as_of_date.toordinal())
            
            for strike in sorted(index.strikes):
                # Get component vols at this strike
                comp_vols_at_strike = self._get_component_vols_at_strike(
                    pkg.component_vols, strike
                )
                
                if comp_vols_at_strike is None:
                    logger.warning(
                        f"No component vols for {index.portfolio} "
                        f"{term} strike={strike}"
                    )
                    continue
                
                # Get index vol at this strike
                index_vol_at_strike = pkg.index_vol.get_vol_for_strike(strike)
                
                if index_vol_at_strike is None:
                    logger.warning(
                        f"No index vol for {index.portfolio} "
                        f"{term} strike={strike}"
                    )
                    continue
                
                # Compute all quantities for this strike
                for quantity in self.quantities:
                    if quantity.name == "implied_correlation":
                        corr_result = quantity.compute(
                            index=index.portfolio,
                            term=term,
                            strike=strike,
                            index_volatility=index_vol_at_strike,
                            weights_df=weights_df,
                            vols_df=comp_vols_at_strike,
                            calculation_date=calc_date,
                        )
                        
                        if corr_result:
                            correlation_by_strike[strike] = corr_result
                        else:
                            logger.warning(
                                f"Failed to compute correlation for {index.portfolio} "
                                f"{term} strike={strike}"
                            )
                    
                    elif quantity.name == "correlation_sensitivities":
                        sens_result = quantity.compute(
                            index=index.portfolio,
                            term=term,
                            strike=strike,
                            index_volatility=index_vol_at_strike,
                            weights_df=weights_df,
                            vols_df=comp_vols_at_strike,
                            calculation_date=calc_date,
                        )
                        
                        if sens_result:
                            sensitivities_by_strike[strike] = sens_result
                        else:
                            logger.warning(
                                f"Failed to compute sensitivities for {index.portfolio} "
                                f"{term} strike={strike}"
                            )
            
            if not correlation_by_strike:
                logger.warning(f"No strikes computed for {index.portfolio} {term}")
                return None
            
            result = AnalyticsResult(
                index=index,
                term=term,
                as_of_date=as_of_date,
                correlation_by_strike=correlation_by_strike,
                sensitivities_by_strike=sensitivities_by_strike,
            )
            
            logger.info(
                f"Computed {index.portfolio} {term}: "
                f"{len(correlation_by_strike)} strikes, "
                f"{len(sensitivities_by_strike)} with sensitivities"
            )
            
            return result
        
        except Exception as e:
            logger.error(f"Error computing term: {e}")
            return None
    
    def _get_component_vols_at_strike(
        self,
        component_vols: ComponentVolatilities,
        strike: float,
    ) -> Optional[pd.DataFrame]:
        """
        Extract component volatilities at specific strike.
        
        Returns:
            DataFrame with [symbol, volatility] at this strike
            or None if not found
        """
        
        try:
            rows = []
            
            for portfolio, symbol in component_vols.component_pairs:
                key = (portfolio, symbol, strike)
                vol = component_vols.vols.get(key)
                
                if vol is None:
                    logger.debug(
                        f"Missing vol for {portfolio} {symbol} strike={strike}"
                    )
                    return None
                
                rows.append({'symbol': f"{portfolio}:{symbol}", 'volatility': vol})
            
            if not rows:
                return None
            
            return pd.DataFrame(rows)
        
        except Exception as e:
            logger.error(f"Error extracting component vols: {e}")
            return None


# ============================================================================
# Convenience Function: Full Pipeline
# ============================================================================

def execute_full_pipeline(
    data_loader,
    index: Index,
    terms: List[str],
    as_of_date: date,
) -> Optional[TrialResults]:
    """
    Execute complete pipeline: load data → compute correlations and sensitivities.
    
    Args:
        data_loader: Initialized AnalyticsDataLoader
        index: Index to compute
        terms: List of terms
        as_of_date: Date
    
    Returns:
        TrialResults or None if fails
    """
    
    try:
        # Load data
        logger.info(f"Loading data for {index.portfolio}")
        packages = data_loader.load_all_terms(
            index=index,
            terms=terms,
            as_of_date=as_of_date,
        )
        
        # Compute correlations and sensitivities
        logger.info(f"Computing correlations and sensitivities for {index.portfolio}")
        engine = AnalyticsEngine(quantities=[
            ImpliedCorrelationQuantity(),
            CorrelationSensitivityQuantity(),
        ])
        trial = engine.compute_all_terms(packages)
        
        if trial is None:
            logger.error(f"Failed to compute {index.portfolio}")
            return None
        
        logger.info(
            f"Pipeline complete for {index.portfolio}: "
            f"{len([r for r in trial.results_by_term.values() if r])} terms"
        )
        
        return trial
    
    except Exception as e:
        logger.error(f"Error executing pipeline: {e}")
        return None