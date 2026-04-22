"""
Core domain models and DTOs for the implied correlation pipeline.
This is the Single Source of Truth for all domain objects.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from enum import Enum, StrEnum
from typing import Any

import pandas as pd

# ============================================================================
# Enums
# ============================================================================


class WeightType(StrEnum):
    MARKET_CAP = "MARKET_CAP"
    EQUAL_WEIGHT = "EQUAL_WEIGHT"


class VolType(StrEnum):
    SVOL = "SVOL"
    IVOL = "IVOL"


class DataSourceType(Enum):
    """Data source origin"""

    POSTGRES = "POSTGRES"
    BIGQUERY = "BIGQUERY"
    CSV_FILE = "CSV_FILE"


# ============================================================================
# DTOs (Data Transfer Objects)
# ============================================================================


@dataclass(frozen=True)
class BaseTransformationDTO:
    """Common metadata required for any atomic transformation."""

    index_name: str
    term: str
    strike: float
    calculation_date: datetime
    weight_strategy: WeightType


@dataclass(frozen=True)
class ImpliedCorrelationDTO(BaseTransformationDTO):
    """Input for implied correlation and sensitivity calculations."""

    index_volatility: float
    weights: pd.DataFrame  # Columns: [symbol, weight]
    vols: pd.DataFrame  # Columns: [symbol, volatility]


@dataclass(frozen=True)
class CorrelationSkewDTO(BaseTransformationDTO):
    """Input for correlation skew calculations."""

    index_volatility: float
    weights: pd.DataFrame
    vols: pd.DataFrame
    skew_data: pd.DataFrame  # Additional skew-specific data


# Map of quantity name to its required DTO class
QUANTITY_DTO_MAP = {
    "implied_correlation": ImpliedCorrelationDTO,
    "correlation_sensitivities": ImpliedCorrelationDTO,
    "correlation_skew": CorrelationSkewDTO,
}

# ============================================================================
# Domain Models
# ============================================================================


@dataclass
class Index:
    """The configuration-driven definition of an index."""

    portfolio: str
    symbol: str
    weight_type: WeightType
    num_components: int
    vol_type: VolType
    strikes: list[float]
    quantities: list[str] = field(default_factory=lambda: ["implied_correlation"])
    name: str = field(init=False)

    def __post_init__(self):
        if self.weight_type == WeightType.EQUAL_WEIGHT and self.num_components <= 0:
            raise ValueError("EQUAL_WEIGHT index must have num_components > 0")
        self.name = f"{self.portfolio}_{self.symbol}"


@dataclass
class IndexWeights:
    """Index composition: [portfolio, symbol, weight]."""

    df: pd.DataFrame  # Columns: [portfolio, symbol, weight]
    source: DataSourceType
    as_of_date: date
    component_pairs: list[tuple[str, str]] = field(default_factory=list)
    num_components: int = 0
    total_weight: float = 0.0

    def __post_init__(self):
        if not self.df.empty:
            self.component_pairs = [
                (row["portfolio"], row["symbol"]) for _, row in self.df.iterrows()
            ]
            self.num_components = len(self.component_pairs)
            self.total_weight = self.df["weight"].sum()

    def is_valid(self) -> bool:
        if self.df.empty:
            return False
        if not all(self.df["weight"] > 0):
            return False
        return 0.99 <= self.total_weight <= 1.01


@dataclass
class VolSurfaceUniverse:
    """Global volatility surface for ONE TERM across all (portfolio, symbol, strike)."""

    term: str
    as_of_date: date
    source: DataSourceType
    vols: dict[tuple[str, str, float], float]

    def get_surface_for_components(
        self, component_pairs: list[tuple[str, str]], strikes: list[float]
    ) -> "ComponentVolatilities":
        available_strikes = {k for (p, s, k) in self.vols.keys()}
        missing_strikes = set(strikes) - available_strikes
        if missing_strikes:
            raise ValueError(
                f"Missing strikes in {self.term} universe: {sorted(missing_strikes)}"
            )

        filtered_vols = {
            (p, s, k): v
            for (p, s, k), v in self.vols.items()
            if (p, s) in set(component_pairs) and k in strikes
        }
        return ComponentVolatilities(
            term=self.term,
            as_of_date=self.as_of_date,
            source=self.source,
            vols=filtered_vols,
        )


@dataclass
class ComponentVolatilities:
    """Volatilities for index components for ONE TERM, filtered to index strikes."""

    term: str
    as_of_date: date
    source: DataSourceType
    vols: dict[tuple[str, str, float], float]
    component_pairs: list[tuple[str, str]] = field(default_factory=list)
    strikes: list[float] = field(default_factory=list)
    num_components: int = 0

    def __post_init__(self):
        if self.vols:
            self.strikes = sorted({k for (p, s, k) in self.vols.keys()})
            self.component_pairs = sorted({(p, s) for (p, s, k) in self.vols.keys()})
            self.num_components = len(self.component_pairs)

    def get_vols_for_strike(self, strike: float) -> pd.DataFrame:
        rows = [
            {"portfolio": p, "symbol": s, "volatility": v}
            for (p, s, k), v in self.vols.items()
            if k == strike
        ]
        return pd.DataFrame(rows)

    def is_valid(self) -> bool:
        return bool(self.vols) and all(v > 0 for v in self.vols.values())


@dataclass
class IndexVolatility:
    """Volatility of the index itself, indexed by strike."""

    portfolio: str
    term: str
    source: DataSourceType
    as_of_date: date
    vols: dict[float, float]

    def get_vol_for_strike(self, strike: float) -> float | None:
        return self.vols.get(strike)

    def is_valid(self) -> bool:
        return bool(self.vols) and all(v > 0 for v in self.vols.values())


@dataclass
class DataPackage:
    """Complete input for analytics engine for ONE (index, term, date)."""

    index: Index
    date: date
    term: str
    weights: IndexWeights
    component_vols: ComponentVolatilities
    index_vol: IndexVolatility
    missing_vol_symbols: list[tuple[str, str]] = field(default_factory=list)

    def is_valid(self) -> bool:
        strikes_match = set(self.component_vols.strikes) == set(self.index.strikes)
        return (
            self.weights.is_valid()
            and self.component_vols.is_valid()
            and self.index_vol.is_valid()
            and len(self.missing_vol_symbols) == 0
            and strikes_match
        )

    def to_dto_stream(
        self, requested_quantities: list[str]
    ) -> list[BaseTransformationDTO]:
        dtos = []
        calc_date = datetime.combine(self.date, datetime.min.time())
        needed_dto_types = {
            QUANTITY_DTO_MAP[q] for q in requested_quantities if q in QUANTITY_DTO_MAP
        }

        for strike in self.component_vols.strikes:
            vols_df = self.component_vols.get_vols_for_strike(strike)
            merged = self.weights.df.merge(
                vols_df, on=["portfolio", "symbol"], how="inner"
            )

            if ImpliedCorrelationDTO in needed_dto_types:
                dtos.append(
                    ImpliedCorrelationDTO(
                        index_name=self.index.portfolio,
                        term=self.term,
                        strike=strike,
                        calculation_date=calc_date,
                        weight_strategy=self.index.weight_type,
                        index_volatility=self.index_vol.get_vol_for_strike(strike),
                        weights=merged[["symbol", "weight"]],
                        vols=merged[["symbol", "volatility"]],
                    )
                )

            if CorrelationSkewDTO in needed_dto_types:
                dtos.append(
                    CorrelationSkewDTO(
                        index_name=self.index.portfolio,
                        term=self.term,
                        strike=strike,
                        calculation_date=calc_date,
                        weight_strategy=self.index.weight_type,
                        index_volatility=self.index_vol.get_vol_for_strike(strike),
                        weights=merged[["symbol", "weight"]],
                        vols=merged[["symbol", "volatility"]],
                        skew_data=pd.DataFrame(),
                    )
                )
        return dtos


# ============================================================================
# Output Formats
# ============================================================================


@dataclass
class ImpliedCorrelationResult:
    """Standard output for implied correlation calculations."""

    index: str
    term: str
    strike: float
    implied_correlation: float
    index_volatility: float
    num_components: int
    calculation_date: datetime
    weight_type: WeightType = WeightType.MARKET_CAP
    data_quality_flags: list[str] = field(default_factory=list)


@dataclass
class CorrelationSensitivity:
    """Analytical sensitivity of implied correlation."""

    symbol: str
    delta: float
    elasticity: float


@dataclass
class TrialResults:
    """Unified output format for a set of calculations."""

    index_name: str
    as_of_date: date
    terms: list[str]
    results: dict[str, dict[float, dict[str, Any]]]

    @property
    def index(self):
        from dataclasses import make_dataclass

        return make_dataclass("IndexCompat", [("symbol", str)])(symbol=self.index_name)

    def to_dataframe(self) -> pd.DataFrame:
        rows = []
        for term, strikes in self.results.items():
            for strike, quantities in strikes.items():
                res = quantities.get("implied_correlation")
                if res and hasattr(res, "implied_correlation"):
                    rows.append(
                        {
                            "index": self.index_name,
                            "term": term,
                            "strike": strike,
                            "implied_correlation": res.implied_correlation,
                            "index_volatility": res.index_volatility,
                            "num_components": res.num_components,
                            "calculation_date": res.calculation_date,
                        }
                    )
        return pd.DataFrame(rows)

    def sensitivities_to_dataframe(self) -> pd.DataFrame:
        rows = []
        for term, strikes in self.results.items():
            for strike, quantities in strikes.items():
                sens_list = quantities.get("correlation_sensitivities")
                if sens_list and isinstance(sens_list, list):
                    for s in sens_list:
                        rows.append(
                            {
                                "index_name": self.index_name,
                                "term": term,
                                "strike": strike,
                                "symbol": s.symbol,
                                "delta": s.delta,
                                "elasticity": s.elasticity,
                            }
                        )
        return pd.DataFrame(rows)
