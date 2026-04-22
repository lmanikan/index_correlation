# PostgreSQL Results Writer Implementation

from datetime import datetime, timedelta

import pandas as pd
from sqlalchemy import Engine, column, table, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from index_correlation.config.results_config import (
    DEFAULT_RESULTS_STORAGE_CONFIG,
    ResultsStorageConfig,
)
from index_correlation.core.models import TrialResults
from index_correlation.storage.interface import (
    ResultsWriter,
    WriterException,
    WriterTableError,
    WriterWriteError,
    _should_write_daily_snapshot,
)
from index_correlation.storage.schemas import ALL_SCHEMAS


class PostgresResultsWriter(ResultsWriter):
    """PostgreSQL implementation of ResultsWriter using SQLAlchemy."""

    def __init__(
        self,
        engine: Engine,
        config: ResultsStorageConfig = DEFAULT_RESULTS_STORAGE_CONFIG,
    ):
        """
        Initialize PostgreSQL writer.

        Args:
            engine: SQLAlchemy Engine connected to PostgreSQL database
            config: Storage configuration
        """
        self.engine = engine
        self.config = config

    def ensure_tables_exist(self) -> None:
        """Create tables if they don't exist."""
        try:
            with self.engine.connect() as conn:
                for schema in ALL_SCHEMAS:
                    sql = schema.create_table_sql_postgresql()
                    conn.execute(text(sql))
                conn.commit()
        except Exception as e:
            raise WriterTableError(f"Failed to create tables: {e}") from e

    def write_trial(self, trial: TrialResults, as_of: datetime) -> None:
        """
        Write trial results to PostgreSQL.

        Flow:
        1. Append correlations to intraday table
        2. Upsert latest sensitivities
        3. Check if snapshot time (index-specific), write daily if needed
        4. Clean up old intraday data
        """
        try:
            # Get DataFrames from trial
            corr_df = trial.to_dataframe()
            sens_df = trial.sensitivities_to_dataframe()

            with self.engine.connect() as conn:
                # 1. Append intraday correlations
                if not corr_df.empty:
                    self._write_intraday_correlations(conn, corr_df, as_of)

                # 2. Upsert latest sensitivities
                if not sens_df.empty:
                    self._upsert_latest_sensitivities(conn, sens_df, as_of)

                # 3. Write daily snapshot if at snapshot time (index-specific)
                # Only writes ONCE per day per index
                if _should_write_daily_snapshot(
                    trial.index.symbol,
                    as_of,
                    self.config,
                ):
                    if not corr_df.empty:
                        self._upsert_daily_correlations(conn, corr_df, as_of)

                # 4. Clean up old intraday data
                self.cleanup_old_intraday(as_of)

                conn.commit()

        except WriterException:
            raise
        except Exception as e:
            raise WriterWriteError(f"Failed to write trial: {e}") from e

    def cleanup_old_intraday(self, as_of: datetime) -> int:
        """Delete intraday data older than retention period."""
        try:
            retention_days = self.config.correlation.five_min_retention_days
            cutoff_date = as_of - timedelta(days=retention_days)

            with self.engine.connect() as conn:
                result = conn.execute(
                    text(
                        "DELETE FROM correlations_intraday "
                        "WHERE as_of_datetime < :cutoff"
                    ),
                    {"cutoff": cutoff_date},
                )
                conn.commit()
                return result.rowcount

        except Exception as e:
            raise WriterWriteError(f"Cleanup failed: {e}") from e

    def _write_intraday_correlations(
        self, conn, df: pd.DataFrame, as_of: datetime
    ) -> None:
        """Append correlations to intraday table."""
        # Prepare data
        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "as_of_datetime": as_of,
                    "index_name": row["index"],
                    "term": row["term"],
                    "strike": row["strike"],
                    "implied_correlation": row["implied_correlation"],
                    "index_volatility": row.get("index_volatility", 0),
                    "num_components": row.get("num_components", 0),
                }
            )

        # Insert
        if records:
            conn.execute(
                text(
                    "INSERT INTO correlations_intraday "
                    "(as_of_datetime, index_name, term, strike, "
                    "implied_correlation, index_volatility, num_components) "
                    "VALUES (:as_of_datetime, :index_name, :term, :strike, "
                    ":implied_correlation, :index_volatility, :num_components)"
                ),
                records,
            )

    def _upsert_latest_sensitivities(
        self, conn, df: pd.DataFrame, as_of: datetime
    ) -> None:
        """Upsert sensitivities to latest table."""
        sensitivities_latest = table(
            "sensitivities_latest",
            column("index_name"),
            column("term"),
            column("strike"),
            column("symbol"),
            column("delta"),
            column("elasticity"),
            column("sens_type"),
            column("as_of_datetime"),
            column("updated_at"),
        )

        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "index_name": row["index"],
                    "term": row["term"],
                    "strike": row["strike"],
                    "symbol": row["symbol"],
                    "delta": row.get("delta", 0),
                    "elasticity": row.get("elasticity", 0),
                    "sens_type": row.get("type", "component"),
                    "as_of_datetime": as_of,
                    "updated_at": datetime.utcnow(),
                }
            )

        if records:
            insert_stmt = pg_insert(sensitivities_latest).values(records)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=["index_name", "term", "strike", "symbol"],
                set_={
                    "delta": insert_stmt.excluded.delta,
                    "elasticity": insert_stmt.excluded.elasticity,
                    "sens_type": insert_stmt.excluded.sens_type,
                    "as_of_datetime": insert_stmt.excluded.as_of_datetime,
                    "updated_at": datetime.utcnow(),
                },
            )
            conn.execute(upsert_stmt)

    def _upsert_daily_correlations(
        self, conn, df: pd.DataFrame, as_of: datetime
    ) -> None:
        """Upsert correlations to daily snapshot table.

        IMPORTANT: Only writes ONCE per day per index.
        Checks if snapshot already exists for this (date, type, index) tuple,
        skips if already snapped today.
        """
        # Get snapshot config for this trial's index
        index_portfolio = df["index"].iloc[0]  # All rows have same index
        snapshot_config = self.config.correlation.get_snapshot_config(index_portfolio)
        snapshot_type = snapshot_config.snapshot_type
        snapshot_date = as_of.date()

        # Check if we already snapped for this index today
        existing_snap = conn.execute(
            text(
                "SELECT COUNT(*) FROM correlations_daily "
                "WHERE snapshot_date = :snap_date "
                "AND snapshot_type = :snap_type "
                "AND index_name = :index_name"
            ),
            {
                "snap_date": snapshot_date,
                "snap_type": snapshot_type,
                "index_name": index_portfolio,
            },
        ).scalar()

        # If we already snapped today, skip
        if existing_snap > 0:
            return

        # Otherwise, write the snapshot
        correlations_daily = table(
            "correlations_daily",
            column("snapshot_date"),
            column("snapshot_type"),
            column("index_name"),
            column("term"),
            column("strike"),
            column("implied_correlation"),
            column("index_volatility"),
            column("num_components"),
            column("as_of_datetime"),
        )

        records = []
        for _, row in df.iterrows():
            records.append(
                {
                    "snapshot_date": snapshot_date,
                    "snapshot_type": snapshot_type,
                    "index_name": row["index"],
                    "term": row["term"],
                    "strike": row["strike"],
                    "implied_correlation": row["implied_correlation"],
                    "index_volatility": row.get("index_volatility", 0),
                    "num_components": row.get("num_components", 0),
                    "as_of_datetime": as_of,
                }
            )

        if records:
            insert_stmt = pg_insert(correlations_daily).values(records)
            upsert_stmt = insert_stmt.on_conflict_do_update(
                index_elements=[
                    "snapshot_date",
                    "snapshot_type",
                    "index_name",
                    "term",
                    "strike",
                ],
                set_={
                    "implied_correlation": insert_stmt.excluded.implied_correlation,
                    "index_volatility": insert_stmt.excluded.index_volatility,
                    "num_components": insert_stmt.excluded.num_components,
                    "as_of_datetime": insert_stmt.excluded.as_of_datetime,
                },
            )
            conn.execute(upsert_stmt)
