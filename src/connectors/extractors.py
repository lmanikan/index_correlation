# src/connectors/extractors.py
"""
Data extraction connectors for implied correlation analytics.

Extractors load data from external sources (CSV, Bloomberg, etc.) and convert
to domain models (IndexWeights, VolSurfaceUniverse, IndexVol).

Architecture:
  - Base classes: WeightsExtractor, VolUniverseExtractor, IndexVolExtractor
  - CSV implementations: CSVWeightsExtractor, CSVVolUniverseExtractor, CSVIndexVolExtractor
  - Other sources: BloombergVolExtractor, etc. (placeholder for future)
"""

import logging
from abc import ABC, abstractmethod
from datetime import date
from typing import Dict, List, Optional, Tuple

import pandas as pd

from src.core.data_models import (
    DataSourceType,
    IndexVolatility,
    IndexWeights,
    VolSurfaceUniverse,
    Index
)

logger = logging.getLogger(__name__)


# ============================================================================
# Abstract Base Classes
# ============================================================================

class WeightsExtractor(ABC):
    """Abstract interface for loading index weights"""
    
    @abstractmethod
    def extract(
        self,
        index: Index,
        as_of_date: date,
    ) -> Optional[IndexWeights]:
        """
        Load weights for index at specific date.
        
        Args:
            index: Index definition (used to identify what to load)
            as_of_date: Date for the weights
        
        Returns:
            IndexWeights with [portfolio, symbol, weight] or None if failed
        """
        pass


class VolUniverseExtractor(ABC):
    """Abstract interface for loading multi-term vol surfaces"""
    
    @abstractmethod
    def extract(
        self,
        terms: List[str],
        as_of_date: date,
    ) -> Dict[str, Optional[VolSurfaceUniverse]]:
        """
        Load vol universe for multiple terms.
        
        CSV data: [portfolio, symbol, term, strike, volatility]
        
        Args:
            terms: List of terms to extract (e.g., ["1M", "3M", "6M"])
            as_of_date: Date for the vols
        
        Returns:
            Dict[term] → VolSurfaceUniverse(term, vols for that term)
            Missing terms map to None.
        """
        pass

class IndexVolExtractor(ABC):
    """Abstract interface for loading strike-indexed index volatility"""
    
    @abstractmethod
    def extract(
        self,
        portfolio: str,
        term: str,
        as_of_date: date,
    ) -> Optional[IndexVolatility]:
        """
        Load strike-indexed index volatility for a portfolio and term.
        
        Args:
            portfolio: Portfolio name (e.g., "SPX_CORR")
            term: Term (e.g., "1M")
            as_of_date: Date for the vols
        
        Returns:
            IndexVol with Dict[strike] → vol for that term or None if failed
        """
        pass

# ============================================================================
# CSV Extractors
# ============================================================================

class CSVWeightsExtractor(WeightsExtractor):
    """
    Load index weights from CSV file.
    
    CSV Format: [portfolio, symbol, weight]
    
    Example:
      portfolio,symbol,weight
      SPX_OPT,SPY US Equity,0.25
      SPX_OPT,IVV US Equity,0.15
      SPX_FUT,ESZ5 Index,0.60
    """
    
    def __init__(self, csv_path: str):
        """
        Args:
            csv_path: Path to CSV file with [portfolio, symbol, weight]
        """
        self.csv_path = csv_path
        logger.info(f"CSVWeightsExtractor initialized with {csv_path}")
    
    def extract(
        self,
        index: Index,
        as_of_date: date,
    ) -> Optional[IndexWeights]:
        """
        Load weights for index at specific date.
        
        Loads all weights from CSV (date-agnostic in simple CSV format).
        """
        try:
            df = pd.read_csv(self.csv_path)
            df.columns = [c.lower().strip() for c in df.columns]
            
            # Validate required columns
            required_cols = {'portfolio', 'symbol', 'weight'}
            if not required_cols.issubset(set(df.columns)):
                logger.error(f"Missing required columns: {required_cols}")
                return None
            
            # Convert weight to float
            df['weight'] = df['weight'].astype(float)
            
            result = IndexWeights(
                df=df[['portfolio', 'symbol', 'weight']],
                source=DataSourceType.CSV_FILE,
                as_of_date=as_of_date,
            )
            
            logger.info(
                f"Loaded weights for {index.portfolio}: "
                f"{result.num_components} components, "
                f"total_weight={result.total_weight:.4f}"
            )
            return result
        
        except Exception as e:
            logger.error(f"Failed to load weights from {self.csv_path}: {e}")
            return None


class CSVVolUniverseExtractor(VolUniverseExtractor):
    """
    Load vol universe from CSV file for multiple terms.
    
    CSV Format: [portfolio, symbol, term, strike, volatility]
    
    Extracts all rows matching requested terms.
    Filters to specific strikes happens during projection.
    
    Example:
      portfolio,symbol,term,strike,volatility
      SPX_OPT,SPY US Equity,1M,0.90,0.315
      SPX_OPT,SPY US Equity,1M,0.95,0.325
      SPX_OPT,SPY US Equity,1M,1.00,0.330
      SPX_OPT,SPY US Equity,3M,0.90,0.320
      SPX_OPT,SPY US Equity,3M,0.95,0.330
      SPX_OPT,SPY US Equity,3M,1.00,0.335
      ...
    """
    
    def __init__(self, csv_path: str):
        """
        Args:
            csv_path: Path to CSV file with [portfolio, symbol, term, strike, volatility]
        """
        self.csv_path = csv_path
        logger.info(f"CSVVolUniverseExtractor initialized with {csv_path}")
    
    def extract(
        self,
        terms: List[str],
        as_of_date: date,
    ) -> Dict[str, Optional[VolSurfaceUniverse]]:
        """
        Load vol universe for multiple terms.
        
        Returns:
            Dict[term] → VolSurfaceUniverse or None if failed
        """
        try:
            df = pd.read_csv(self.csv_path)
            df.columns = [c.lower().strip() for c in df.columns]
            
            # Validate required columns
            required_cols = {'portfolio', 'symbol', 'term', 'strike', 'volatility'}
            if not required_cols.issubset(set(df.columns)):
                logger.error(f"Missing required columns: {required_cols}")
                return {term: None for term in terms}
            
            # Convert numeric columns
            df['strike'] = df['strike'].astype(float)
            df['volatility'] = df['volatility'].astype(float)
            
            result = {}
            
            for term in terms:
                # Filter to this term only
                df_term = df[df['term'].str.lower() == term.lower()]
                
                if df_term.empty:
                    logger.warning(f"No vol data for term {term}")
                    result[term] = None
                    continue
                
                # Build Dict[(portfolio, symbol, strike)] → vol
                vols_dict = {}
                for _, row in df_term.iterrows():
                    key = (
                        row['portfolio'],
                        row['symbol'],
                        float(row['strike']),
                    )
                    vols_dict[key] = float(row['volatility'])
                
                universe = VolSurfaceUniverse(
                    term=term,
                    as_of_date=as_of_date,
                    source=DataSourceType.CSV_FILE,
                    vols=vols_dict,
                )
                
                logger.info(
                    f"Loaded {term}: {len(universe.portfolios)} portfolios × "
                    f"{sum(len(s) for s in universe.symbols.values())} symbols × "
                    f"{len(universe.strikes)} strikes"
                )
                result[term] = universe
            
            return result
        
        except Exception as e:
            logger.error(f"Failed to load vol universe from {self.csv_path}: {e}")
            return {term: None for term in terms}




class CSVIndexVolExtractor(IndexVolExtractor):
    """
    Load strike-indexed index volatility from CSV file.
    
    CSV Format: [portfolio, term, strike, volatility]
    
    Extracts all rows matching portfolio and term.
    Creates Dict[strike] → volatility per term.
    
    Example:
      portfolio,term,strike,volatility
      SPX_CORR,1M,0.90,0.175
      SPX_CORR,1M,0.95,0.177
      SPX_CORR,1M,1.00,0.180
      SPX_CORR,1M,1.05,0.182
      SPX_CORR,1M,1.10,0.184
      SPX_CORR,3M,0.90,0.172
      SPX_CORR,3M,0.95,0.174
      SPX_CORR,3M,1.00,0.175
    """
    
    def __init__(self, csv_path: str):
        """
        Args:
            csv_path: Path to CSV file with [portfolio, term, strike, volatility]
        """
        self.csv_path = csv_path
        logger.info(f"CSVIndexVolExtractor initialized with {csv_path}")
    
    def extract(
        self,
        portfolio: str,
        term: str,
        as_of_date: date,
    ) -> Optional[IndexVolatility]:
        """
        Load strike-indexed index vols for portfolio and term.
        """
        try:
            df = pd.read_csv(self.csv_path)
            df.columns = [c.lower().strip() for c in df.columns]
            
            # Validate required columns
            required_cols = {'portfolio', 'term', 'strike', 'volatility'}
            if not required_cols.issubset(set(df.columns)):
                logger.error(f"Missing required columns: {required_cols}")
                return None
            
            # Convert numeric columns
            df['strike'] = df['strike'].astype(float)
            df['volatility'] = df['volatility'].astype(float)
            
            # Filter to portfolio and term
            df_filtered = df[
                (df['portfolio'] == portfolio) &
                (df['term'].str.lower() == term.lower())
            ]
            
            if df_filtered.empty:
                logger.warning(
                    f"Index vol not found for {portfolio} {term}"
                )
                return None
            
            # Build Dict[strike] → vol
            vols_dict = {}
            for _, row in df_filtered.iterrows():
                strike = float(row['strike'])
                vol = float(row['volatility'])
                vols_dict[strike] = vol
            
            result = IndexVolatility(
                portfolio=portfolio,
                term=term,
                source=DataSourceType.CSV_FILE,
                as_of_date=as_of_date,
                vols=vols_dict,
            )
            
            logger.info(
                f"Loaded index vols {portfolio} {term}: "
                f"{len(result.strikes)} strikes"
            )
            return result
        
        except Exception as e:
            logger.error(
                f"Failed to extract index vol {portfolio} {term}: {e}"
            )
            return None


# ============================================================================
# Helper: Load Extractors from Config
# ============================================================================

def create_extractors_from_config(config: dict):
    """
    Factory function to create extractors from configuration.
    
    Config example:
    {
        "weights": {
            "type": "csv",
            "path": "data/weights.csv"
        },
        "vol_universe": {
            "type": "csv",
            "path": "data/component_vols.csv"
        },
        "index_vol": {
            "type": "csv",
            "path": "data/index_vols.csv"
        }
    }
    
    Args:
        config: Configuration dict with extractor specs
    
    Returns:
        (weights_extractor, vol_universe_extractor, index_vol_extractor)
    """
    
    # Weights extractor
    weights_config = config.get('weights', {})
    weights_type = weights_config.get('type', 'csv')
    
    if weights_type == 'csv':
        weights_extractor = CSVWeightsExtractor(weights_config['path'])
    else:
        raise ValueError(f"Unknown weights extractor type: {weights_type}")
    
    # Vol universe extractor
    vol_config = config.get('vol_universe', {})
    vol_type = vol_config.get('type', 'csv')
    
    if vol_type == 'csv':
        vol_extractor = CSVVolUniverseExtractor(vol_config['path'])
    else:
        raise ValueError(f"Unknown vol extractor type: {vol_type}")
    
    # Index vol extractor
    idx_vol_config = config.get('index_vol', {})
    idx_vol_type = idx_vol_config.get('type', 'csv')
    
    if idx_vol_type == 'csv':
        index_vol_extractor = CSVIndexVolExtractor(idx_vol_config['path'])
    else:
        raise ValueError(f"Unknown index vol extractor type: {idx_vol_type}")
    
    logger.info(
        f"Created extractors: weights={weights_type}, "
        f"vol_universe={vol_type}, index_vol={idx_vol_type}"
    )
    
    return weights_extractor, vol_extractor, index_vol_extractor