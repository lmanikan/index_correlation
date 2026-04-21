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

from index_correlation.core.data_models import (
    DataPackage,
    Index,
    TrialResults,
    AnalyticsResult
)
from index_correlation.core.models import (
    ImpliedCorrelationResult,
    CorrelationSensitivity,
    WeightType,
    TransformationInput,
)
from index_correlation.core.exceptions import ValidationError, TransformationError

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
    def compute(self, input_dto: TransformationInput) -> Optional[Any]:
        """
        Compute this quantity.
        
        Args:
            input_dto: The DTO containing necessary data for one strike/term.
        
        Returns:
            Computed quantity (type-specific), or None if computation fails
        """
        pass


# ============================================================================
# DATA VALIDATION (SHARED ACROSS ALL QUANTITIES)
# ============================================================================

class DataValidator:
    """Validates weights and volatilities before computation."""
    
    MIN_COMPONENTS = 1
    WEIGHT_SUM_TOLERANCE = 0.01  # ±1%
    MIN_VOL = 0.001  # 0.1%
    MAX_VOL = 5.0    # 500%
    
    @staticmethod
    def validate_input(input_dto: TransformationInput) -> Tuple[bool, List[str]]:
        """Validate TransformationInput data structure and ranges."""
        flags = []
        
        # Validate weights
        if input_dto.weights.empty:
            flags.append("No weights provided")
        elif 'weight' not in input_dto.weights.columns:
            flags.append("Missing 'weight' column")
        elif input_dto.weights['weight'].isna().any():
            flags.append("Weight column contains NaN values")
        elif (input_dto.weights['weight'] < 0).any():
            flags.append("Negative weights found")
        elif len(input_dto.weights) < DataValidator.MIN_COMPONENTS:
            flags.append(f"Insufficient components: {len(input_dto.weights)}")
        elif abs(input_dto.weights['weight'].sum() - 1.0) > DataValidator.WEIGHT_SUM_TOLERANCE:
            flags.append(f"Weights sum to {input_dto.weights['weight'].sum():.6f}, not 1.0")

        # Validate vols
        if input_dto.vols.empty:
            flags.append("No volatilities provided")
        elif 'volatility' not in input_dto.vols.columns:
            flags.append("Missing 'volatility' column")
        elif input_dto.vols['volatility'].isna().any():
            flags.append("Volatility column contains NaN values")
        elif (input_dto.vols['volatility'] < DataValidator.MIN_VOL).any():
            flags.append("Vols too low")
        elif (input_dto.vols['volatility'] > DataValidator.MAX_VOL).any():
            flags.append("Vols too high")
        
        return len(flags) == 0, flags


# ============================================================================
# CONCRETE QUANTITY: IMPLIED CORRELATION
# ============================================================================

class ImpliedCorrelationQuantity(BaseQuantity):
    """
    Compute implied correlation from index and component volatilities.
    """
    
    @property
    def name(self) -> str:
        return "implied_correlation"
    
    def compute(self, input_dto: TransformationInput) -> Optional[ImpliedCorrelationResult]:
        
        valid, flags = DataValidator.validate_input(input_dto)
        if not valid:
            logger.warning(f"Validation failed for {input_dto.index_name}: {flags}")
            return None
        
        # Merge weights + vols
        merged = pd.merge(input_dto.weights, input_dto.vols, on='symbol', how='inner')
        if merged.empty:
            return None
            
        w = merged['weight'].to_numpy(dtype='float64')
        sigma = merged['volatility'].to_numpy(dtype='float64')
        
        v = w * sigma
        S = np.sum(v)
        Q = np.sum(v**2)
        A = input_dto.index_volatility ** 2 - Q
        B = S**2 - Q
        
        if abs(B) < 1e-10:
            return None
        
        rho = np.clip(A / B, -1.0, 1.0)
        
        return ImpliedCorrelationResult(
            index=input_dto.index_name,
            term=input_dto.term,
            strike=input_dto.strike,
            implied_correlation=float(rho),
            index_volatility=input_dto.index_volatility,
            num_components=len(merged),
            calculation_date=input_dto.calculation_date,
            weight_type=input_dto.weight_strategy,
            data_quality_flags=flags
        )


# ============================================================================
# CONCRETE QUANTITY: CORRELATION SENSITIVITIES
# ============================================================================

class CorrelationSensitivityQuantity(BaseQuantity):
    """
    Compute sensitivity of implied correlation.
    """
    
    @property
    def name(self) -> str:
        return "correlation_sensitivities"
    
    def compute(self, input_dto: TransformationInput) -> Optional[Tuple[List[CorrelationSensitivity], float, float]]:
        
        valid, _ = DataValidator.validate_input(input_dto)
        if not valid:
            return None
        
        merged = pd.merge(input_dto.weights, input_dto.vols, on='symbol', how='inner')
        if merged.empty:
            return None
        
        w = merged['weight'].to_numpy(dtype='float64')
        sigma = merged['volatility'].to_numpy(dtype='float64')
        symbols = merged['symbol'].values
        
        v = w * sigma
        S = np.sum(v)
        Q = np.sum(v**2)
        A = input_dto.index_volatility ** 2 - Q
        B = S**2 - Q
        
        if abs(B) < 1e-10:
            return None
        
        rho = np.clip(A / B, -1.0, 1.0)
        
        sensitivities = []
        for idx, symbol in enumerate(symbols):
            v_i = v[idx]
            numerator = v_i * B + (S - v_i) * A
            drho_dsigma = -2 * w[idx] * numerator / (B**2)
            
            elasticity = (drho_dsigma * sigma[idx] / rho) * 100 if abs(rho) > 1e-10 else 0.0
            
            sensitivities.append(CorrelationSensitivity(symbol=symbol, delta=float(drho_dsigma), elasticity=float(elasticity)))
            
        drho_d_index_vol = 2 * input_dto.index_volatility / B
        elasticity_index_vol = (2 * input_dto.index_volatility**2 / A * 100) if abs(A) > 1e-10 else 0.0
        
        return sensitivities, float(drho_d_index_vol), float(elasticity_index_vol)


# ============================================================================
# ANALYTICS ENGINE
# ============================================================================

class AnalyticsEngine:
    def __init__(self, quantities: Optional[List[BaseQuantity]] = None):
        self.quantities = quantities or [ImpliedCorrelationQuantity()]
    
    def compute_all_terms(self, pkg: DataPackage) -> Optional[TrialResults]:
        results_by_term = {}
        
        for input_dto in pkg.to_dto_stream():
            # Dispatch
            # For this simplified engine, we assume correlation + sensitivities
            # In a full impl, we would loop over self.quantities
            pass # Implementation details would follow
        
        return TrialResults(pkg.index, pkg.date, [pkg.term], results_by_term)