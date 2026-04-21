#!/usr/bin/env python
"""
Cleanup job – deletes old intraday data based on retention policy.

Usage:
    python jobs/cleanup_old_data.py \\
      --db_url postgresql://user:pass@localhost/analytics

Typical cron schedule (daily at 2:00 AM UTC):
    0 2 * * * /path/to/venv/bin/python /path/to/jobs/cleanup_old_data.py
"""

import argparse
import logging
import sys
from datetime import datetime

from sqlalchemy import create_engine

from index_correlation.storage.postgres_writer import PostgresResultsWriter
from index_correlation.config.results_config import GLOBAL_STORAGE_CONFIG, MULTI_REGION_CONFIG

logger = logging.getLogger(__name__)


def cleanup_old_data(db_url: str, dry_run: bool = False) -> int:
    """
    Delete intraday data older than retention window.
    
    Uses GLOBAL_STORAGE_CONFIG.intraday_retention_days to determine cutoff date.
    
    Args:
        db_url: PostgreSQL connection URL
        dry_run: If True, show what would be deleted without deleting
    
    Returns:
        Number of rows deleted
    """
    now = datetime.utcnow()
    
    logger.info(f"Starting cleanup at {now}")
    logger.info(f"Retention policy: {GLOBAL_STORAGE_CONFIG.intraday_retention_days} days")
    
    if dry_run:
        logger.info("DRY RUN MODE - no data will be deleted")
    
    # Initialize writer
    try:
        engine = create_engine(db_url)
        writer = PostgresResultsWriter(engine, config=MULTI_REGION_CONFIG)
    except Exception as e:
        logger.error(f"Failed to initialize PostgresResultsWriter: {e}", exc_info=True)
        raise
    
    # Run cleanup
    try:
        if dry_run:
            # Simulate what would be deleted
            from datetime import timedelta
            retention_days = GLOBAL_STORAGE_CONFIG.intraday_retention_days
            cutoff = now - timedelta(days=retention_days)
            
            logger.info(f"Would delete rows with as_of_datetime < {cutoff}")
            logger.info(f"(To actually delete, re-run without --dry_run)")
            
            # Query to see what would be deleted
            from sqlalchemy import text
            query = f"""
            SELECT COUNT(*) as row_count
            FROM correlations_intraday
            WHERE as_of_datetime < '{cutoff.isoformat()}'
            """
            try:
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    row_count = result.scalar() or 0
                logger.info(f"Would delete {row_count} intraday rows")
            except Exception as e:
                logger.warning(f"Could not estimate deletion count: {e}")
            
            return 0
        else:
            rows_deleted = writer.cleanup_old_intraday(as_of=now)
            logger.info(f"Cleanup complete: deleted {rows_deleted} old intraday rows")
            return rows_deleted
    
    except Exception as e:
        logger.error(f"Cleanup failed: {e}", exc_info=True)
        raise


def main():
    """Entry point for CLI."""
    parser = argparse.ArgumentParser(
        description="Cleanup old intraday data based on retention policy"
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
        "--dry_run",
        action="store_true",
        help="Show what would be deleted without actually deleting"
    )
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)-8s] %(name)s: %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            # Optional: add FileHandler for persistent logs
            # logging.FileHandler(f"logs/cleanup_{datetime.now().strftime('%Y%m%d')}.log")
        ]
    )
    
    logger.info("=" * 80)
    logger.info("Cleanup Old Intraday Data Job")
    logger.info(f"Started at {datetime.utcnow()}")
    logger.info("=" * 80)
    
    # Run cleanup
    try:
        rows_deleted = cleanup_old_data(args.db_url, dry_run=args.dry_run)
        logger.info(f"Exiting with success (deleted {rows_deleted} rows)")
        sys.exit(0)
    
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
