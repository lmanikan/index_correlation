import numpy as np

from index_correlation.analytics.engine import BaseQuantity
from index_correlation.core.models import (
    CorrelationSensitivity,
    ImpliedCorrelationDTO,
    ImpliedCorrelationResult,
)


class ImpliedCorrelationQuantity(BaseQuantity):
    @property
    def name(self) -> str:
        return "implied_correlation"

    def compute(self, dto: ImpliedCorrelationDTO) -> ImpliedCorrelationResult | None:
        if not isinstance(dto, ImpliedCorrelationDTO):
            raise TypeError(f"Expected ImpliedCorrelationDTO, got {type(dto)}")

        # Basic calculation logic
        w = dto.weights["weight"].values
        sigma = dto.vols["volatility"].values

        v = w * sigma
        S = np.sum(v)
        Q = np.sum(v**2)
        A = dto.index_volatility**2 - Q
        B = S**2 - Q

        if abs(B) < 1e-10:
            return None

        rho = np.clip(A / B, -1.0, 1.0)

        return ImpliedCorrelationResult(
            index=dto.index_name,
            term=dto.term,
            strike=dto.strike,
            implied_correlation=float(rho),
            index_volatility=dto.index_volatility,
            num_components=len(dto.weights),
            calculation_date=dto.calculation_date,
            weight_type=dto.weight_strategy,
        )


class CorrelationSensitivityQuantity(BaseQuantity):
    @property
    def name(self) -> str:
        return "correlation_sensitivities"

    def compute(
        self, dto: ImpliedCorrelationDTO
    ) -> list[CorrelationSensitivity] | None:
        if not isinstance(dto, ImpliedCorrelationDTO):
            raise TypeError(f"Expected ImpliedCorrelationDTO, got {type(dto)}")

        w = dto.weights["weight"].values
        sigma = dto.vols["volatility"].values
        symbols = dto.weights["symbol"].values

        v = w * sigma
        S = np.sum(v)
        Q = np.sum(v**2)
        A = dto.index_volatility**2 - Q
        B = S**2 - Q

        if abs(B) < 1e-10:
            return None

        rho = np.clip(A / B, -1.0, 1.0)

        sensitivities = []
        for idx, symbol in enumerate(symbols):
            v_i = v[idx]
            numerator = v_i * B + (S - v_i) * A
            drho_dsigma = -2 * w[idx] * numerator / (B**2)

            elasticity = (
                (drho_dsigma * sigma[idx] / rho) * 100 if abs(rho) > 1e-10 else 0.0
            )
            sensitivities.append(
                CorrelationSensitivity(
                    symbol=symbol,
                    delta=float(drho_dsigma),
                    elasticity=float(elasticity),
                )
            )

        return sensitivities
