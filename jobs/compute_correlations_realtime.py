#!/usr/bin/env python
"""
Real-time correlation compute job – runs every 5 minutes.

Usage:
    python jobs/compute_correlations_realtime.py \\
      --db_url postgresql://user:pass@localhost/analytics

Typical cron schedule:
    */5 * * * * /path/to/venv/bin/python /path/to/jobs/compute_correlations_realtime.py
"""

import argparse
import logging
import sys
from datetime import datetime
from typing import List, Tuple

from sqlalchemy import create_engine, Engine

from index_correlation.config.indices_config import INDICES_CONFIG, Index
from index_correlation.extraction.market_data_extractor import MarketDataExtractor, ExtractionError
from index_correlation.extraction.validation import validate_data_package, ValidationError
from index_correlation.extraction.data_package import DataPackage
from index_correlation.connectors.results_writer import ResultsWriter
from index_correlation.storage.postgres_writer import PostgresResultsWriter
from index_correlation.config.results_config import MULTI_REGION_CONFIG

logger = logging.getLogger(__name__)


class RealtimeComputeError(Exception):
    """Raised when real-time compute fails."""
    pass


def compute_realtime(db_url: str, analytics_engine) -> Tuple[int, int, List[Tuple[str, str]]]:
    """
    Run one iteration of real-time compute.
    
    For each index in INDICES_CONFIG:
    1. Extract market data → DataPackage
    2. Validate package
    3. Compute correlations → TrialResults
    4. Write to database
    
    Args:
        db_url: PostgreSQL connection URL
        analytics_engine: AnalyticsEngine instance (must have compute_all_terms method)
    
    Returns:
        (success_count, failure_count, failures_list)
        where failures_list is [(index_name, error_message), ...]
    """
    now = datetime.utcnow()
    logger.info(f"Starting real-time compute at {now}")
    
    # Initialize components
    try:
        engine = create_engine(db_url)
        extractor = MarketDataExtractor(engine)
        writer = PostgresResultsWriter(engine, config=MULTI_REGION_CONFIG)
    except Exception as e:
        logger.error(f"Failed to initialize components: {e}", exc_info=True)
        raise RealtimeComputeError(f"Initialization failed: {e}")
    
    # Ensure tables exist
    try:
        writer.ensure_tables_exist()
    except Exception as e:
        logger.error(f"Failed to ensure tables exist: {e}", exc_info=True)
        raise RealtimeComputeError(f"Table creation failed: {e}")
    
    success_count = 0
    failure_count = 0
    failures_list = []
    
    # Process each index
    for config in INDICES_CONFIG:
        index_name = f"{config['portfolio']}_{config['symbol']}"
        
        try:
            logger.info(f"Processing {index_name}")
            index = Index(
                portfolio=config["portfolio"],
                symbol=config["symbol"],
                name=index_name,
                close_time_utc=config["close_time_utc"],
                close_time_tolerance_minutes=config["close_time_tolerance_minutes"],
                description=config.get("description", "")
            )
            
            # STEP 1: Extract
            try:
                logger.debug(f"Extracting data for {index_name}")
                package = extractor.create_data_package(index, as_of=now)
                logger.debug(
                    f"Extracted {len(package.ivol_surface)} ivol rows, "
                    f"{len(package.svol_surface)} svol rows for {index_name}"
                )
            except ExtractionError as e:
                logger.error(f"Extraction failed for {index_name}: {e}")
                failure_count += 1
                failures_list.append((index_name, f"Extraction: {e}"))
                continue
            
            # STEP 2: Validate
            try:
                validation_result = validate_data_package(package, strict=False)
                if not validation_result["valid"]:
                    error_msg = "; ".join(validation_result["errors"])
                    logger.error(f"Validation failed for {index_name}: {error_msg}")
                    failure_count += 1
                    failures_list.append((index_name, f"Validation: {error_msg}"))
                    continue
                
                if validation_result["warnings"]:
                    logger.warning(f"Validation warnings for {index_name}: {validation_result['warnings']}")
            
            except ValidationError as e:
                logger.error(f"Validation raised error for {index_name}: {e}")
                failure_count += 1
                failures_list.append((index_name, f"Validation: {e}"))
                continue
            
            # STEP 3: Compute
            try:
                logger.debug(f"Computing correlations for {index_name}")
                trial = analytics_engine.compute_all_terms(package)
                logger.debug(
                    f"Computed trial for {index_name}: "
                    f"{len(trial.correlations)} correlation rows, "
                    f"{len(trial.sensitivities)} sensitivity rows"
                )
            except Exception as e:
                logger.error(f"Compute failed for {index_name}: {e}", exc_info=True)
                failure_count += 1
                failures_list.append((index_name, f"Compute: {e}"))
                continue
            
            # STEP 4: Write
            try:
                logger.debug(f"Writing results for {index_name}")
                writer.write_trial(trial, as_of=now)
                logger.info(f"Successfully wrote results for {index_name}")
                success_count += 1
            except Exception as e:
                logger.error(f"Write failed for {index_name}: {e}", exc_info=True)
                failure_count += 1
                failures_list.append((index_name, f"Write: {e}"))
                continue
        
        except Exception as e:
            logger.error(f"Unexpected error processing {index_name}: {e}", exc_info=True)
            failure_count += 1
            failures_list.append((index_name, f"Unexpected: {e}"))
    
    # Summary
    logger.info(
        f"Real-time compute complete: "
        f"{success_count} successes, {failure_count} failures out of {len(INDICES_CONFIG)} indices"
    )
    
    if failures_list:
        logger.warning(f"Failed indices:")
        for index_name, error in failures_list:
            logger.warning(f"  - {index_name}: {error}")
    
    return success_count, failure_count, failures_list


def main():
    """Entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Real-time correlations compute job (runs every 5 minutes)"
    )
    parser.add_argument(
        "--db_url",
        type=str,
        default="postgresql://localhost/analytics",
        help="Database URL (default: postgresql://localhost/analytics)"
    )
    parser.add_argument(
        "--log_level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level (default: INFO)"
    )
    parser.add_argument(
        "--analytics_engine_path",
        type=str,
        default=None,
        help="(Optional) Custom path to AnalyticsEngine import"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            # Optional: add FileHandler for persistent logs
            # logging.FileHandler(f"logs/realtime_{datetime.now().strftime('%Y%m%d')}.log")
        ]
    )
    
    logger.info("=" * 80)
    logger.info("Real-Time Correlations Compute Job")
    logger.info(f"Started at {datetime.utcnow()}")
    logger.info("=" * 80)
    
    # Import AnalyticsEngine
    try:
        if args.analytics_engine_path:
            # Custom import path
            import importlib.util
            spec = importlib.util.spec_from_file_location("analytics", args.analytics_engine_path)
            analytics_module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(analytics_module)
            analytics_engine = analytics_module.AnalyticsEngine()
        else:
            # Default import (adjust path as needed)
            from index_correlation.analytics import AnalyticsEngine
            analytics_engine = AnalyticsEngine()
    except ImportError as e:
        logger.error(f"Failed to import AnalyticsEngine: {e}")
        logger.info("Note: Update --analytics_engine_path or adjust src/analytics.py import")
        sys.exit(1)
    
    # Run compute
    try:
        success_count, failure_count, failures_list = compute_realtime(args.db_url, analytics_engine)
        
        # Exit code: 0 if all succeeded, 1 if any failed
        exit_code = 0 if failure_count == 0 else 1
        
        logger.info(f"Exiting with code {exit_code}")
        sys.exit(exit_code)
    
    except RealtimeComputeError as e:
        logger.error(f"Compute error: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
