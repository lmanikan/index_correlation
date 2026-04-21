# src/connectors/data_loader.py
"""
Data loading pipeline for implied correlation analytics.

Orchestrates extraction of weights, vol surfaces, and index vols,
building complete DataPackage instances for analytics engine consumption.

Architecture:
  - DataLoader: Coordinates extraction and validation
  - Loads all terms simultaneously for a given index and date
  - Produces DataPackage objects (one per term)
  - Handles missing data and validation failures
"""

import logging
from datetime import date
from typing import Dict, List, Optional

from index_correlation.connectors.extractors import (
    IndexVolExtractor,
    VolUniverseExtractor,
    WeightsExtractor,
)
from index_correlation.core.data_models import (
    DataPackage,
    IndexVolatility,
    IndexWeights,
    VolSurfaceUniverse,
    Index
)
logger = logging.getLogger(__name__)


# ============================================================================
# Data Loader
# ============================================================================

class DataLoader:
    """
    Orchestrates data extraction and packaging for analytics.
    
    Coordinates:
      1. Weights extraction (once per index)
      2. Vol universe extraction (once per term list)
      3. Index vol extraction (once per (portfolio, term) pair)
      4. DataPackage assembly and validation
    
    Usage:
      loader = DataLoader(weights_ext, vol_ext, index_vol_ext)
      packages = loader.load_all_terms(
          index=my_index,
          terms=["1M", "3M"],
          as_of_date=date(2025, 12, 20)
      )
    """
    
    def __init__(
        self,
        weights_extractor: WeightsExtractor,
        vol_universe_extractor: VolUniverseExtractor,
        index_vol_extractor: IndexVolExtractor,
    ):
        """
        Args:
            weights_extractor: Loads index weights
            vol_universe_extractor: Loads vol surfaces for multiple terms
            index_vol_extractor: Loads index vol for (portfolio, term)
        """
        self.weights_ext = weights_extractor
        self.vol_ext = vol_universe_extractor
        self.index_vol_ext = index_vol_extractor
        
        logger.info("DataLoader initialized")
    
    def load_all_terms(
        self,
        index: Index,
        terms: List[str],
        as_of_date: date,
    ) -> Dict[str, Optional[DataPackage]]:
        """
        Load complete data for all terms for a given index.
        
        Process:
          1. Load weights (once, shared across all terms)
          2. Load vol universe for all terms
          3. For each term:
             a. Get index vol for (portfolio, term)
             b. Project vol universe to index strikes
             c. Build and validate DataPackage
        
        Args:
            index: Index definition with portfolio, strikes, etc.
            terms: List of terms to load (e.g., ["1M", "3M", "6M"])
            as_of_date: Date for all data
        
        Returns:
            Dict[term] → DataPackage (valid) or None (failed)
            
            Missing or invalid data maps to None.
            At least one term should succeed for meaningful results.
        """
        
        logger.info(
            f"Loading all terms for {index.portfolio} on {as_of_date}: "
            f"{terms}"
        )
        
        # Step 1: Load weights (once, shared)
        weights = self._load_weights(index, as_of_date)
        if weights is None:
            logger.error(f"Failed to load weights for {index.portfolio}")
            return {term: None for term in terms}
        
        # Step 2: Load vol universe for all terms
        vol_universes = self._load_vol_universe(terms, as_of_date)
        
        # Step 3: Build DataPackage per term
        result = {}
        for term in terms:
            pkg = self._build_data_package(
                index=index,
                term=term,
                weights=weights,
                vol_universes=vol_universes,
                as_of_date=as_of_date,
            )
            result[term] = pkg
        
        # Summary
        valid_count = sum(1 for pkg in result.values() if pkg and pkg.is_valid())
        logger.info(
            f"Loaded {index.portfolio}: {valid_count}/{len(terms)} terms valid"
        )
        
        return result
    
    def _load_weights(
        self,
        index: Index,
        as_of_date: date,
    ) -> Optional[IndexWeights]:
        """
        Load weights for index.
        
        Returns:
            IndexWeights or None if failed
        """
        try:
            logger.debug(f"Loading weights for {index.portfolio}")
            weights = self.weights_ext.extract(index, as_of_date)
            
            if weights is None:
                logger.warning(f"Weights not found for {index.portfolio}")
                return None
            
            if not weights.is_valid():
                logger.warning(
                    f"Weights invalid for {index.portfolio}: "
                    f"total_weight={weights.total_weight:.4f}"
                )
                return None
            
            logger.debug(f"Weights valid: {weights.summary()}")
            return weights
        
        except Exception as e:
            logger.error(f"Error loading weights: {e}")
            return None
    
    def _load_vol_universe(
        self,
        terms: List[str],
        as_of_date: date,
    ) -> Dict[str, Optional[VolSurfaceUniverse]]:
        """
        Load vol universe for multiple terms.
        
        Returns:
            Dict[term] → VolSurfaceUniverse (or None if failed)
        """
        try:
            logger.debug(f"Loading vol universe for {terms}")
            vol_universes = self.vol_ext.extract(terms, as_of_date)
            
            for term, universe in vol_universes.items():
                if universe is None:
                    logger.warning(f"Vol universe not found for {term}")
                elif not universe.strikes:
                    logger.warning(f"Vol universe empty for {term}")
                else:
                    logger.debug(f"Vol universe valid: {universe.summary()}")
            
            return vol_universes
        
        except Exception as e:
            logger.error(f"Error loading vol universe: {e}")
            return {term: None for term in terms}
    
    def _build_data_package(
        self,
        index: Index,
        term: str,
        weights: IndexWeights,
        vol_universes: Dict[str, Optional[VolSurfaceUniverse]],
        as_of_date: date,
    ) -> Optional[DataPackage]:
        """
        Build complete DataPackage for one term.
        
        Steps:
          1. Validate vol universe exists for this term
          2. Project vol universe to index components and strikes
          3. Load index vol for this term
          4. Assemble DataPackage
          5. Validate completeness
        
        Returns:
            DataPackage or None if any step fails
        """
        
        # Step 1: Check vol universe
        vol_universe = vol_universes.get(term)
        if vol_universe is None:
            logger.warning(f"No vol universe for {index.portfolio} {term}")
            return None
        
        # Step 2: Project to index strikes and components
        try:
            component_vols = vol_universe.get_surface_for_components(
                component_pairs=weights.component_pairs,
                strikes=index.strikes,
            )
            logger.debug(f"Projected vol surface: {component_vols.summary()}")
        
        except ValueError as e:
            logger.warning(
                f"Failed to project vol surface for {index.portfolio} {term}: {e}"
            )
            return None
        
        # Step 3: Load index vol
        try:
            index_vol = self.index_vol_ext.extract(
                portfolio=index.portfolio,
                term=term,
                as_of_date=as_of_date,
            )
            
            if index_vol is None:
                logger.warning(
                    f"Index vol not found for {index.portfolio} {term}"
                )
                return None
            
            if not index_vol.is_valid():
                logger.warning(
                    f"Index vol invalid for {index.portfolio} {term}"
                )
                return None
            
            logger.debug(f"Index vol valid: {index_vol.summary()}")
        
        except Exception as e:
            logger.error(
                f"Error loading index vol {index.portfolio} {term}: {e}"
            )
            return None
        
        # Step 4: Validate strike consistency
        if set(component_vols.strikes) != set(index_vol.strikes):
            missing_in_index = set(component_vols.strikes) - set(index_vol.strikes)
            missing_in_component = set(index_vol.strikes) - set(component_vols.strikes)
            
            if missing_in_index:
                logger.warning(
                    f"Index vol missing strikes: {missing_in_index} "
                    f"(component has them for {index.portfolio} {term})"
                )
            if missing_in_component:
                logger.warning(
                    f"Component vol missing strikes: {missing_in_component} "
                    f"(index has them for {index.portfolio} {term})"
                )
            return None
        
        # Step 5: Assemble DataPackage
        try:
            pkg = DataPackage(
                index=index,
                date=as_of_date,
                term=term,
                weights=weights,
                component_vols=component_vols,
                index_vol=index_vol,
            )
            
            logger.debug(f"DataPackage assembled: {pkg.summary()}")
            return pkg
        
        except Exception as e:
            logger.error(
                f"Error assembling DataPackage {index.portfolio} {term}: {e}"
            )
            return None


# ============================================================================
# Batch Loader
# ============================================================================

class BatchDataLoader:
    """
    Load data for multiple indices and dates.
    
    Useful for:
      - Loading historical time series
      - Backtesting across multiple indices
      - Bulk extraction
    
    Usage:
      batch_loader = BatchDataLoader(loader)
      results = batch_loader.load_batch(
          indices=[spx_index, ndx_index],
          terms=["1M", "3M"],
          dates=[date(2025, 12, 20), date(2025, 12, 19)]
      )
    """
    
    def __init__(self, data_loader: DataLoader):
        """
        Args:
            data_loader: DataLoader instance to use for all loads
        """
        self.loader = data_loader
        logger.info("BatchDataLoader initialized")
    
    def load_batch(
        self,
        indices: List[Index],
        terms: List[str],
        dates: List[date],
    ) -> Dict:
        """
        Load data for multiple indices and dates.
        
        Args:
            indices: List of Index objects to load
            terms: List of terms for each index
            dates: List of dates to load
        
        Returns:
            Nested Dict: {index.portfolio} → {date} → {term} → DataPackage
            
            Example:
              results["SPX_CORR"][date(2025, 12, 20)]["1M"] = DataPackage(...)
        """
        
        result = {}
        
        for index in indices:
            result[index.portfolio] = {}
            
            for as_of_date in dates:
                logger.info(
                    f"Loading {index.portfolio} on {as_of_date}: {terms}"
                )
                
                packages = self.loader.load_all_terms(
                    index=index,
                    terms=terms,
                    as_of_date=as_of_date,
                )
                
                result[index.portfolio][as_of_date] = packages
        
        return result
    
    def load_timeseries(
        self,
        index: Index,
        terms: List[str],
        start_date: date,
        end_date: date,
        business_days_only: bool = True,
    ) -> Dict[date, Dict[str, Optional[DataPackage]]]:
        """
        Load data for single index across date range.
        
        Args:
            index: Index to load
            terms: Terms for this index
            start_date: Start of date range
            end_date: End of date range
            business_days_only: Filter to trading days only
        
        Returns:
            Dict[date] → Dict[term] → DataPackage
        """
        
        import pandas as pd
        
        # Generate date range
        date_range = pd.date_range(start=start_date, end=end_date, freq='D')
        
        if business_days_only:
            date_range = date_range[date_range.dayofweek < 5]  # Mon-Fri
        
        dates = [d.date() for d in date_range]
        
        logger.info(
            f"Loading timeseries for {index.portfolio}: "
            f"{len(dates)} dates from {start_date} to {end_date}"
        )
        
        result = {}
        for as_of_date in dates:
            packages = self.loader.load_all_terms(
                index=index,
                terms=terms,
                as_of_date=as_of_date,
            )
            result[as_of_date] = packages
        
        return result
    