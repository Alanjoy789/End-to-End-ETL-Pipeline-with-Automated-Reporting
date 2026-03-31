"""
Pipeline Orchestrator
=====================
Runs the complete ETL pipeline: Extract → Transform → Load → Report.
Handles logging setup, error handling, and pipeline run tracking.
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.extract import extract, load_config
from src.transform import transform
from src.load import load, log_pipeline_run
from src.report import generate_report


def setup_logging(config: dict) -> None:
    """Configure logging to both console and file."""
    log_dir = Path(config["paths"]["logs"])
    log_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = log_dir / f"pipeline_{timestamp}.log"

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler(sys.stdout),
        ],
    )


def run_pipeline(config_path: str = "config/pipeline_config.yaml") -> None:
    """
    Execute the full ETL pipeline.

    Steps:
    1. Load configuration
    2. EXTRACT — Fetch weather data from API
    3. TRANSFORM — Clean, validate, and enrich
    4. LOAD — Upsert into SQLite database
    5. REPORT — Generate HTML dashboard
    6. Log pipeline run metadata
    """
    config = load_config(config_path)
    setup_logging(config)

    logger = logging.getLogger(__name__)
    logger.info("=" * 60)
    logger.info("ETL PIPELINE STARTED")
    logger.info("=" * 60)

    run_start = datetime.now(timezone.utc)
    cities_count = len(config["api"]["cities"])

    try:
        # ---- EXTRACT ----
        logger.info("[1/4] EXTRACTING data from OpenWeatherMap API...")
        raw_data = extract(config)

        if not raw_data:
            raise RuntimeError("Extraction returned no data. Check API key and network.")

        # ---- TRANSFORM ----
        logger.info("[2/4] TRANSFORMING raw data...")
        df = transform(raw_data, config)

        if df.empty:
            raise RuntimeError("Transformation produced empty DataFrame.")

        # ---- LOAD ----
        logger.info("[3/4] LOADING data into database...")
        load_summary = load(df, config)

        # ---- REPORT ----
        logger.info("[4/4] GENERATING dashboard report...")
        report_path = generate_report(config)

        # ---- LOG SUCCESS ----
        status = "SUCCESS" if load_summary["failed"] == 0 else "PARTIAL"
        log_pipeline_run(
            config=config,
            run_start=run_start,
            status=status,
            cities_attempted=cities_count,
            records_loaded=load_summary["inserted"],
            records_skipped=load_summary["skipped"],
        )

        logger.info("=" * 60)
        logger.info("ETL PIPELINE COMPLETED SUCCESSFULLY")
        logger.info(f"  Cities processed : {len(raw_data)}/{cities_count}")
        logger.info(f"  Records inserted : {load_summary['inserted']}")
        logger.info(f"  Records skipped  : {load_summary['skipped']}")
        logger.info(f"  Report generated : {report_path}")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"PIPELINE FAILED: {e}", exc_info=True)

        log_pipeline_run(
            config=config,
            run_start=run_start,
            status="FAILED",
            cities_attempted=cities_count,
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    run_pipeline()
