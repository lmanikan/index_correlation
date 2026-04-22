"""
Analytics Framework: Extensible computation of market quantities.

Architecture:
  - BaseQuantity: Abstract interface for any computable quantity
  - AnalyticsEngine: Orchestrates multiple quantities across strikes and terms
"""

import logging
from abc import ABC, abstractmethod
from typing import Any

from index_correlation.core.models import (
    QUANTITY_DTO_MAP,
    BaseTransformationDTO,
    DataPackage,
    TrialResults,
)

logger = logging.getLogger(__name__)


class BaseQuantity(ABC):
    """
    Abstract base class for any computable market quantity.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique name for this quantity (e.g., 'implied_correlation')."""
        pass

    @abstractmethod
    def compute(self, dto: BaseTransformationDTO) -> Any:
        """
        Compute this quantity using the provided DTO.
        """
        pass


class AnalyticsEngine:
    """
    Orchestrator that dispatches DTOs to their respective quantity handlers.
    """

    def __init__(self, quantity_handlers: list[BaseQuantity]):
        self.handlers = {h.name: h for h in quantity_handlers}
        # Reverse map: DTO type -> list of handlers that can process it
        self.dto_to_handlers: dict[type[BaseTransformationDTO], list[BaseQuantity]] = {}
        for h in quantity_handlers:
            dto_type = QUANTITY_DTO_MAP.get(h.name)
            if dto_type:
                if dto_type not in self.dto_to_handlers:
                    self.dto_to_handlers[dto_type] = []
                self.dto_to_handlers[dto_type].append(h)

    def compute_all(
        self, pkg: DataPackage, requested_quantities: list[str]
    ) -> TrialResults:
        """
        Compute requested quantities for all terms and strikes in the package.
        """
        # Results structure: term -> strike -> {quantity_name: result}
        results: dict[str, dict[float, dict[str, Any]]] = {pkg.term: {}}

        # Get DTO stream from package
        dtos = pkg.to_dto_stream(requested_quantities)

        for dto in dtos:
            if dto.strike not in results[pkg.term]:
                results[pkg.term][dto.strike] = {}

            # Find handlers that can process this DTO
            dto_type = type(dto)
            for handler in self.dto_to_handlers.get(dto_type, []):
                if handler.name in requested_quantities:
                    try:
                        res = handler.compute(dto)
                        results[pkg.term][dto.strike][handler.name] = res
                    except Exception as e:
                        logger.error(
                            f"Error computing {handler.name} for {dto.index_name} at {dto.strike}: {e}"
                        )
                        results[pkg.term][dto.strike][handler.name] = None

        return TrialResults(
            index_name=pkg.index.portfolio,
            as_of_date=pkg.date,
            terms=[pkg.term],
            results=results,
        )
