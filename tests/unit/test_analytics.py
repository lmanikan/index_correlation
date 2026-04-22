from datetime import datetime
from unittest.mock import MagicMock

import pandas as pd
import pytest

from index_correlation.analytics.engine import AnalyticsEngine
from index_correlation.analytics.quantities.correlation import (
    CorrelationSensitivityQuantity,
    ImpliedCorrelationQuantity,
)
from index_correlation.core.models import CorrelationInput, WeightType


def test_implied_correlation_calculation():
    handler = ImpliedCorrelationQuantity()

    # Simple case: 2 components, equal weight
    weights = pd.DataFrame(
        [{"symbol": "A", "weight": 0.5}, {"symbol": "B", "weight": 0.5}]
    )
    vols = pd.DataFrame(
        [{"symbol": "A", "volatility": 0.2}, {"symbol": "B", "volatility": 0.2}]
    )

    dto = CorrelationInput(
        index_name="TEST",
        term="1M",
        strike=1.0,
        calculation_date=datetime.now(),
        index_volatility=0.2,
        weights=weights,
        vols=vols,
        weight_strategy=WeightType.EQUAL_WEIGHT,
    )

    res = handler.compute(dto)
    assert res is not None
    # If all vols are same and index vol is same, rho should be 1.0
    assert pytest.approx(res.implied_correlation) == 1.0


def test_correlation_sensitivity_calculation():
    handler = CorrelationSensitivityQuantity()

    weights = pd.DataFrame(
        [{"symbol": "A", "weight": 0.5}, {"symbol": "B", "weight": 0.5}]
    )
    vols = pd.DataFrame(
        [{"symbol": "A", "volatility": 0.2}, {"symbol": "B", "volatility": 0.2}]
    )

    dto = CorrelationInput(
        index_name="TEST",
        term="1M",
        strike=1.0,
        calculation_date=datetime.now(),
        index_volatility=0.2,
        weights=weights,
        vols=vols,
        weight_strategy=WeightType.EQUAL_WEIGHT,
    )

    res = handler.compute(dto)
    assert res is not None
    assert len(res.component_sensitivities) == 2
    assert isinstance(res.index_vol_delta, float)


def test_analytics_engine_orchestration():
    engine = AnalyticsEngine()

    # Mock DataPackage
    mock_pkg = MagicMock()
    mock_pkg.is_valid.return_value = True
    mock_pkg.index.quantities = ["implied_correlation"]
    mock_pkg.date = datetime.now().date()
    mock_pkg.term = "1M"

    # Mock to_dto_stream
    dto = CorrelationInput(
        index_name="TEST",
        term="1M",
        strike=1.0,
        calculation_date=datetime.now(),
        index_volatility=0.2,
        weights=pd.DataFrame(
            [{"symbol": "A", "weight": 0.5}, {"symbol": "B", "weight": 0.5}]
        ),
        vols=pd.DataFrame(
            [{"symbol": "A", "volatility": 0.2}, {"symbol": "B", "volatility": 0.2}]
        ),
    )
    mock_pkg.to_dto_stream.return_value = [dto]

    results = engine.compute_all_terms({"1M": mock_pkg})

    assert results is not None
    assert "1M" in results.results_by_term
    assert 1.0 in results.results_by_term["1M"].correlation_by_strike
