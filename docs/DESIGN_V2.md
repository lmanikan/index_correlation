# Design Document v2: Config-Driven ETL and DTO Analytics

## 1. Overview
This project is an ETL pipeline for implied correlation analytics. This revision focuses on a configuration-driven data ingestion flow and a DTO-based transformation layer to ensure flexibility and robustness.

## 2. Ingestion Flow (The Driver)
The data ingestion follows a strict dependency chain defined by the index configuration:

1.  **Index Resolution:** Load `Index` configuration from `indexes.yaml`.
    - *New:* Configuration now specifies `quantities` to compute (e.g., `["implied_correlation", "correlation_sensitivities"]`).
2.  **Component Resolution:** Fetch the basket components (symbols and weights) for the given index.
3.  **Data Requirement Mapping:** Determine exactly what market data is required based on the requested `quantities` and `strikes`.
4.  **Market Data Fetching:** Fetch volatilities (Index and Components) for the identified symbols, terms, and strikes.
5.  **DTO Assembly:** Package the raw data into `TransformationInput` DTOs (Data Transfer Objects).

## 3. Analytics Layer (The Engine)
The transformation logic is decoupled from data sources using DTOs.

### `TransformationInput` (DTO)
A granular object containing the smallest quantity of data needed for one atomic calculation (usually one strike of one term).
- `index_name`: str
- `term`: str
- `strike`: float
- `index_volatility`: float
- `weights`: pd.DataFrame [symbol, weight]
- `vols`: pd.DataFrame [symbol, volatility]
- `calculation_date`: datetime
- `weight_strategy`: WeightType

### `BaseQuantity` Interface
Standardized interface for all mathematical quantities.
- `compute(input: TransformationInput) -> Optional[Any]`

### `AnalyticsEngine`
Orchestrates the execution:
- Receives a `DataPackage` (the bulk input).
- Slices it into `TransformationInput` DTOs.
- Dispatches to requested `BaseQuantity` implementations.
- Aggregates results into `TrialResults`.

## 4. Proposed `src/index_correlation/` Restructuring

### `src/index_correlation/core/` (Unified Models)
Consolidate `data_models.py` and `models.py`.
- **Primary Models:** `Index`, `DataPackage`, `TrialResults`.
- **Analytics DTOs:** `TransformationInput`, `AnalyticsResult`.
- **Enums:** `WeightType`, `VolType`, `DataSourceType`.

### `src/index_correlation/config/` (Typed Configuration)
- `index_config.py`: Loads `Index` objects including the `quantities` list.
- `database_config.py`: Environment-aware database connection settings.

### `src/index_correlation/extraction/` (Sequential Ingestion)
- `loader.py`: Implements the 5-step ingestion flow.
- `extractors.py`: Atomic database/CSV queries.
- `validation.py`: Data quality gates.

### `src/index_correlation/analytics/` (Math Core)
- `engine.py`: Orchestration logic.
- `quantities/`: Individual quantity implementations (correlation, sens, etc.).

### `src/index_correlation/storage/` (Result Persistence)
- `interface.py`: ABCs for Writers/Readers.
- `backends/`: Postgres and BigQuery implementations.

## 5. Next Steps
1.  **Finalize `src/index_correlation/core/models.py`:** Define the unified models and the new `TransformationInput` DTO.
2.  **Update `src/index_correlation/analytics/engine.py`:** Refactor `BaseQuantity` and `AnalyticsEngine` to use the DTO.
3.  **Refactor `src/index_correlation/extraction/loader.py`:** Implement the sequential ingestion logic.
