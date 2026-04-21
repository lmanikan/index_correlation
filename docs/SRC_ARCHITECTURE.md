# High-Level Design: `src/index_correlation/` Architecture

## 1. Objective
To build a modular, testable, and configuration-driven pipeline for quantitative finance analytics. The architecture follows a **Ports and Adapters (Hexagonal)** pattern to isolate pure mathematical logic from volatile external data sources.

---

## 2. Core Layers

### A. The Shared Kernel (`src/index_correlation/core/`)
**Decision:** Single source of truth for domain models.
- **Role:** Defines the immutable data structures used by all layers.
- **Key Objects:** 
    - `Index`: The configuration-driven definition.
    - `DataPackage`: The bulk container for extracted data.
    - `TransformationInput` (DTO): The atomic unit of work for a single calculation.
    - `TrialResults`: The unified output format.

### B. The Logic Hub (`src/index_correlation/analytics/`)
**Decision:** Specialized DTOs per Quantity.
- **Role:** Implements mathematical models (Implied Correlation, Sensitivities).
- **Structure:**
    - `engine.py`: Orchestrator that dispatches DTOs to their respective quantity handlers.
    - `quantities/`: Individual modules for specific calculations.
- **Interface:** `compute(dto: QuantitySpecificDTO) -> Result`
- **Why?** Enforces a strict data contract. Adding a new quantity like `correlation_skew` only requires defining a new DTO and its corresponding handler, without bloating the common `TransformationInput`.

### C. The Input Port (`src/index_correlation/extraction/`)
**Decision:** Dependency-injected requirements.
- **Role:** Translates an `Index` definition into a fetching strategy.
- **Flow:**
    1. Resolve components from weights table.
    2. Map component list + term/strike requirements to SQL/BQ queries.
    3. Execute targeted fetches.
    4. Assemble the `DataPackage`.

### D. The Output Port (`src/index_correlation/storage/`)
**Decision:** Interface segregation.
- **Role:** Persists results.
- **Structure:**
    - `interface.py`: ABCs for `Writer` and `Reader`.
    - `backends/`: Concrete implementations (Postgres, BigQuery).
    - `schemas.py`: Unified table definitions (The "Contract" with the DB).

---

## 3. Data Flow & Transformations

1.  **Ingestion:** `Loader` reads `Index` config -> calls `Extractors` -> returns `DataPackage`.
2.  **Slicing:** `DataPackage` exposes a method `.to_dto_stream()` which yields granular `TransformationInput` objects.
3.  **Execution:** `AnalyticsEngine` receives the stream -> identifies requested `quantities` -> dispatches to math functions.
4.  **Persistence:** `TrialResults` are passed to `ResultsWriter` -> standard SQL `UPSERT` operations.

---

## 4. Why This is Better Than Current
- **No Circular Imports:** `core/` is the base of the pyramid.
- **Dynamic Growth:** Adding a new math quantity only requires a new class in `analytics/quantities/` and an entry in `indexes.yaml`. No changes to jobs or extractors needed.
- **Resilience:** If the database schema changes, only `src/index_correlation/extraction/extractors.py` needs an update. The rest of the system is agnostic to table structures.
