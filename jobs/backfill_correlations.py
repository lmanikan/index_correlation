# Backfill Correlations Job

import argparse
import logging
import sys
from datetime import date, datetime

import pandas as pd
from tqdm import tqdm

from index_correlation.analytics.engine import AnalyticsEngine
from index_correlation.config.database_config import get_database_config
from index_correlation.config.index_config import load_indices_from_yaml
from index_correlation.config.results_config import MULTI_REGION_CONFIG
from index_correlation.core.models import Index
from index_correlation.extraction.extractors import VolUniverseExtractor
from index_correlation.storage.writer_factory import get_writer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("backfill_correlations.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class BackfillStats:
    """Track backfill statistics."""

    def __init__(self):
        self.total_dates = 0
        self.total_indices = 0
        self.successful = 0
        self.failed = 0
        self.skipped = 0
        self.start_time = datetime.now()
        self.failed_records: list[tuple] = []  # (date, index, error)

    def record_success(self):
        self.successful += 1

    def record_failure(self, current_date: date, index: Index, error: Exception):
        self.failed += 1
        self.failed_records.append((current_date, index.name, str(error)))

    def record_skip(self):
        self.skipped += 1

    def print_summary(self):
        elapsed = datetime.now() - self.start_time
        total_runs = self.total_dates * self.total_indices

        print("\n" + "=" * 80)
        print("BACKFILL CORRELATIONS - SUMMARY")
        print("=" * 80)
        print(f"Total runs:        {total_runs}")
        print(f"Successful:        {self.successful}")
        print(f"Failed:            {self.failed}")
        print(f"Skipped:           {self.skipped}")
        print(f"Success rate:      {self.successful / total_runs * 100:.1f}%")
        print(f"Elapsed time:      {elapsed}")
        print(f"Avg per run:       {elapsed / total_runs if total_runs > 0 else 0}")

        if self.failed_records:
            print("\nFailed records:")
            for date_str, index_name, error in self.failed_records[:10]:
                print(f"  - {date_str} {index_name}: {error[:60]}")
            if len(self.failed_records) > 10:
                print(f"  ... and {len(self.failed_records) - 10} more")

        print("=" * 80 + "\n")

        logger.info(
            f"Backfill complete. Success: {self.successful}, "
            f"Failed: {self.failed}, Skipped: {self.skipped}"
        )


def get_business_dates(start_date: date, end_date: date) -> pd.DatetimeIndex:
    """Get business days between start and end dates."""
    return pd.bdate_range(start_date, end_date, freq="B")


def load_indices_from_config() -> list[Index]:
    indices = load_indices_from_yaml("indices.yaml")
    logger.info(f"Loaded {len(indices)} indices from YAML")
    return indices


def filter_indices(
    indices: list[Index],
    portfolio: str | None = None,
    symbol: str | None = None,
) -> list[Index]:
    """
    Filter indices by portfolio and/or symbol.

    Args:
        indices: List of all indices
        portfolio: Filter by portfolio name (e.g., "SPX")
        symbol: Filter by symbol (e.g., "SPX")

    Returns:
        List of matching indices

    Raises:
        ValueError: If 0 or >1 indices match
    """
    filtered = indices

    if portfolio:
        filtered = [idx for idx in filtered if idx.portfolio == portfolio]
        logger.info(f"Filtered by portfolio={portfolio}: {len(filtered)} matches")

    if symbol:
        filtered = [idx for idx in filtered if idx.symbol == symbol]
        logger.info(f"Filtered by symbol={symbol}: {len(filtered)} matches")

    # Validate exactly 1 match
    if len(filtered) == 0:
        filters = []
        if portfolio:
            filters.append(f"portfolio={portfolio}")
        if symbol:
            filters.append(f"symbol={symbol}")
        raise ValueError(f"No indices match: {', '.join(filters)}")

    if len(filtered) > 1:
        filters = []
        if portfolio:
            filters.append(f"portfolio={portfolio}")
        if symbol:
            filters.append(f"symbol={symbol}")
        matching = [f"{idx.portfolio}/{idx.symbol}" for idx in filtered]
        raise ValueError(
            f"Multiple indices match: {', '.join(filters)}\n"
            f"Matches: {', '.join(matching)}\n"
            f"Please provide more specific filters"
        )

    logger.info(f"Selected index: {filtered[0].name}")
    return filtered


def backfill_correlations(
    from_date: date,
    to_date: date,
    indices: list[Index],
    db_config_name: str | None = None,  # Name of config to use (or None for default)
    skip_existing: bool = False,
) -> BackfillStats:
    """
    Backfill historical correlations.

    Args:
        from_date: Start date (inclusive)
        to_date: End date (inclusive)
        indices: List of Index objects to backfill
        db_config_name: Name of database config to use (e.g., "postgres", "bigquery")
                       If None, uses default from config file
        skip_existing: Skip dates that already have data

    Returns:
        BackfillStats with summary
    """
    logger.info(f"Starting backfill: {from_date} to {to_date}")

    try:
        # Load database config
        db_config = get_database_config(db_config_name)
        logger.info(f"Using database config: {db_config.type}")

        # Create writer
        writer = get_writer(db_config, results_config=MULTI_REGION_CONFIG)

        # Create extractor (depends on DB type)
        if db_config.type == "postgres":
            from sqlalchemy import create_engine

            engine = create_engine(db_config.url, pool_pre_ping=True)
            extractor = VolUniverseExtractor(engine)
        else:
            # BigQuery extractor
            extractor = VolUniverseExtractor(
                project_id=db_config.project_id,
                dataset=db_config.dataset,
            )

        analytics = AnalyticsEngine()

        logger.info("Ensuring database tables exist...")
        writer.ensure_tables_exist()

    except Exception as e:
        logger.error(f"Failed to initialize: {e}", exc_info=True)
        raise

    logger.info(f"Indices to backfill: {[idx.name for idx in indices]}")

    # Get date range (business days only)
    business_dates = get_business_dates(from_date, to_date)
    logger.info(f"Processing {len(business_dates)} business days")

    stats = BackfillStats()
    stats.total_dates = len(business_dates)
    stats.total_indices = len(indices)

    # Backfill loop
    failed_indices = {}  # Track consecutive failures per index

    with tqdm(
        total=stats.total_dates * stats.total_indices, desc="Backfilling"
    ) as pbar:
        for current_date in business_dates:
            current_date_py = current_date.date()

            for index in indices:
                try:
                    # Skip if too many consecutive failures for this index
                    if index.name in failed_indices and failed_indices[index.name] >= 3:
                        logger.warning(
                            f"Skipping {index.name} on {current_date_py} "
                            "- too many failures"
                        )
                        stats.record_skip()
                        pbar.update(1)
                        continue

                    # Convert to datetime at 09:00 UTC (typically market open)
                    as_of = datetime.combine(
                        current_date_py, datetime.min.time()
                    ).replace(hour=9)

                    # Extract market data
                    logger.debug(f"Extracting {index.name} {current_date_py}")
                    package = extractor.create_data_package(index, as_of=as_of)

                    # Validate package
                    if package is None or len(package.ivol_surface) == 0:
                        logger.warning(
                            f"Empty package for {index.name} {current_date_py}"
                        )
                        stats.record_skip()
                        pbar.update(1)
                        continue

                    # Compute correlations
                    logger.debug(f"Computing {index.name} {current_date_py}")
                    trial = analytics.compute_all_terms(package)

                    assert trial is not None

                    # Write to database
                    logger.debug(f"Writing {index.name} {current_date_py}")
                    writer.write_trial(trial, as_of=as_of)

                    stats.record_success()
                    failed_indices[index.name] = 0  # Reset failure count

                    logger.info(f"✓ {index.name} {current_date_py}")

                except Exception as e:
                    stats.record_failure(current_date_py, index, e)

                    # Track consecutive failures
                    if index.name not in failed_indices:
                        failed_indices[index.name] = 0
                    failed_indices[index.name] += 1

                    logger.error(
                        f"✗ {index.name} {current_date_py}: {e}", exc_info=False
                    )

                finally:
                    pbar.update(1)

    # Print summary
    stats.print_summary()

    return stats


def parse_args():
    parser = argparse.ArgumentParser(description="Backfill historical correlations")

    parser.add_argument(
        "--from_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        required=True,
        help="Start date (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--to_date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d").date(),
        default=date.today(),
        help="End date (YYYY-MM-DD). Default: today",
    )

    parser.add_argument(
        "--db_config",
        type=str,
        default=None,
        help=(
            "Database config name (postgres, bigquery, etc). "
            "Default: uses config file default"
        ),
    )

    parser.add_argument(
        "--portfolio",
        type=str,
        default=None,
        help="Portfolio name (e.g., SPX). If not specified, runs for all",
    )

    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        help="Symbol (e.g., SPX). If not specified, runs for all",
    )

    parser.add_argument(
        "--skip_existing", action="store_true", help="Skip dates that already have data"
    )

    return parser.parse_args()


def main():
    """Main entry point."""
    args = parse_args()

    # Validate dates
    if args.from_date > args.to_date:
        logger.error("from_date must be <= to_date")
        sys.exit(1)

    # Load all indices from config
    logger.info("Loading indices from config...")
    all_indices = load_indices_from_config()

    # Filter by portfolio and/or symbol
    try:
        if args.portfolio or args.symbol:
            indices = filter_indices(
                all_indices, portfolio=args.portfolio, symbol=args.symbol
            )
        else:
            indices = all_indices
            logger.info(f"Using all {len(indices)} indices from config")
    except ValueError as e:
        logger.error(str(e))
        sys.exit(1)

    # Run backfill
    try:
        stats = backfill_correlations(
            from_date=args.from_date,
            to_date=args.to_date,
            indices=indices,
        )

        # Exit with error code if there were failures
        if stats.failed > 0:
            logger.warning(f"Backfill completed with {stats.failed} failures")
            sys.exit(1)
        else:
            logger.info("Backfill completed successfully")
            sys.exit(0)

    except Exception as e:
        logger.error(f"Backfill failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
