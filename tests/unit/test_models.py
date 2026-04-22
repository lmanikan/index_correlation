from datetime import date

import pandas as pd
import pytest

from index_correlation.core.models import (
    ComponentVolatilities,
    DataPackage,
    DataSourceType,
    Index,
    IndexVolatility,
    IndexWeights,
    VolSurfaceUniverse,
    VolType,
    WeightType,
)


def test_index_model():
    idx = Index(
        portfolio="SPX",
        symbol="SPX",
        weight_type=WeightType.EQUAL_WEIGHT,
        num_components=500,
        vol_type=VolType.LOCAL,
        strikes=[0.9, 1.0, 1.1],
        quantities=["implied_correlation"],
    )
    assert idx.name == "SPX_SPX"

    with pytest.raises(
        ValueError, match="EQUAL_WEIGHT index must have num_components > 0"
    ):
        Index(
            portfolio="SPX",
            symbol="SPX",
            weight_type=WeightType.EQUAL_WEIGHT,
            num_components=0,
            vol_type=VolType.LOCAL,
            strikes=[1.0],
        )


def test_index_weights_validation():
    df = pd.DataFrame(
        [
            {"portfolio": "SPX", "symbol": "AAPL", "weight": 0.6},
            {"portfolio": "SPX", "symbol": "MSFT", "weight": 0.4},
        ]
    )
    weights = IndexWeights(
        df=df, source=DataSourceType.CSV_FILE, as_of_date=date(2025, 1, 1)
    )
    assert weights.is_valid()
    assert weights.num_components == 2
    assert weights.total_weight == 1.0

    # Invalid weight
    df_invalid = pd.DataFrame([{"portfolio": "SPX", "symbol": "AAPL", "weight": -0.1}])
    weights_invalid = IndexWeights(
        df=df_invalid, source=DataSourceType.CSV_FILE, as_of_date=date(2025, 1, 1)
    )
    assert not weights_invalid.is_valid()


def test_vol_surface_universe_projection():
    vols = {
        ("SPX", "AAPL", 1.0): 0.3,
        ("SPX", "MSFT", 1.0): 0.25,
        ("DAX", "SAP", 1.0): 0.2,
    }
    universe = VolSurfaceUniverse(
        term="1M",
        as_of_date=date(2025, 1, 1),
        source=DataSourceType.CSV_FILE,
        vols=vols,
    )

    component_pairs = [("SPX", "AAPL"), ("SPX", "MSFT")]
    comp_vols = universe.get_surface_for_components(component_pairs, strikes=[1.0])

    assert comp_vols.num_components == 2
    assert comp_vols.term == "1M"
    assert 1.0 in comp_vols.strikes


def test_data_package_validity():
    idx = Index(
        portfolio="SPX",
        symbol="SPX",
        weight_type=WeightType.EQUAL_WEIGHT,
        num_components=2,
        vol_type=VolType.LOCAL,
        strikes=[1.0],
    )
    weights = IndexWeights(
        df=pd.DataFrame(
            [
                {"portfolio": "SPX", "symbol": "AAPL", "weight": 0.5},
                {"portfolio": "SPX", "symbol": "MSFT", "weight": 0.5},
            ]
        ),
        source=DataSourceType.CSV_FILE,
        as_of_date=date(2025, 1, 1),
    )
    comp_vols = ComponentVolatilities(
        term="1M",
        as_of_date=date(2025, 1, 1),
        source=DataSourceType.CSV_FILE,
        vols={("SPX", "AAPL", 1.0): 0.3, ("SPX", "MSFT", 1.0): 0.25},
    )
    index_vol = IndexVolatility(
        portfolio="SPX",
        term="1M",
        source=DataSourceType.CSV_FILE,
        as_of_date=date(2025, 1, 1),
        vols={1.0: 0.2},
    )

    pkg = DataPackage(
        index=idx,
        date=date(2025, 1, 1),
        term="1M",
        weights=weights,
        component_vols=comp_vols,
        index_vol=index_vol,
    )

    assert pkg.is_valid()

    # Test to_dto_stream
    dtos = pkg.to_dto_stream()
    assert len(dtos) == 1
    assert dtos[0].strike == 1.0
    assert dtos[0].index_volatility == 0.2
