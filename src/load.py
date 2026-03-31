"""
Load Module
===========
Loads transformed weather data into a SQLite database.
Implements upsert logic and pipeline run logging.
"""

import logging
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)


def get_connection(db_path: str) -> sqlite3.Connection:
    """Create or connect to the SQLite database."""
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def initialize_database(conn: sqlite3.Connection, config: dict) -> None:
    """
    Create tables if they don't exist.

    Tables:
    - weather_data: Main fact table with weather observations
    - pipeline_runs: Metadata log for each ETL execution
    """
    table_name = config["database"]["table_name"]
    log_table = config["database"]["log_table"]

    conn.executescript(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            city            TEXT NOT NULL,
            country         TEXT,
            latitude        REAL,
            longitude       REAL,
            temperature_c   REAL,
            feels_like_c    REAL,
            temp_min_c      REAL,
            temp_max_c      REAL,
            humidity_pct    REAL,
            pressure_hpa    REAL,
            wind_speed_mps  REAL,
            wind_direction_deg REAL,
            cloudiness_pct  REAL,
            weather_condition TEXT,
            weather_description TEXT,
            weather_icon    TEXT,
            visibility_m    REAL,
            sunrise_utc     TEXT,
            sunset_utc      TEXT,
            data_timestamp_utc TEXT,
            extracted_at_utc TEXT,
            temperature_category TEXT,
            humidity_category TEXT,
            wind_category   TEXT,
            comfort_index   REAL,
            loaded_at_utc   TEXT DEFAULT (datetime('now')),
            UNIQUE(city, data_timestamp_utc)
        );

        CREATE INDEX IF NOT EXISTS idx_{table_name}_city
            ON {table_name}(city);
        CREATE INDEX IF NOT EXISTS idx_{table_name}_timestamp
            ON {table_name}(data_timestamp_utc);

        CREATE TABLE IF NOT EXISTS {log_table} (
            run_id          INTEGER PRIMARY KEY AUTOINCREMENT,
            run_start_utc   TEXT NOT NULL,
            run_end_utc     TEXT,
            status          TEXT NOT NULL DEFAULT 'RUNNING',
            cities_attempted INTEGER DEFAULT 0,
            records_loaded  INTEGER DEFAULT 0,
            records_skipped INTEGER DEFAULT 0,
            error_message   TEXT,
            duration_seconds REAL
        );
    """)
    conn.commit()
    logger.info("Database tables initialized")


def load(df: pd.DataFrame, config: dict) -> dict:
    """
    Load DataFrame into the SQLite database with upsert logic.

    Parameters
    ----------
    df : pd.DataFrame
        Transformed weather data.
    config : dict
        Pipeline configuration.

    Returns
    -------
    dict
        Load summary with counts of inserted, skipped, and failed records.
    """
    db_path = config["database"]["path"]
    table_name = config["database"]["table_name"]

    conn = get_connection(db_path)
    initialize_database(conn, config)

    summary = {"inserted": 0, "skipped": 0, "failed": 0}

    if df.empty:
        logger.warning("Empty DataFrame — nothing to load")
        conn.close()
        return summary

    # Add load timestamp
    df = df.copy()
    df["loaded_at_utc"] = datetime.now(timezone.utc).isoformat()

    # Get column list (excluding auto-increment id)
    columns = [c for c in df.columns if c != "id"]
    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)

    # Upsert: INSERT OR IGNORE based on (city, data_timestamp_utc) unique constraint
    insert_sql = f"""
        INSERT OR IGNORE INTO {table_name} ({col_names})
        VALUES ({placeholders})
    """

    for _, row in df.iterrows():
        try:
            values = [
                None if pd.isna(row.get(c)) else row.get(c)
                for c in columns
            ]
            cursor = conn.execute(insert_sql, values)

            if cursor.rowcount > 0:
                summary["inserted"] += 1
            else:
                summary["skipped"] += 1
                logger.debug(f"Skipped duplicate: {row.get('city')} @ {row.get('data_timestamp_utc')}")

        except sqlite3.Error as e:
            summary["failed"] += 1
            logger.error(f"Failed to insert {row.get('city')}: {e}")

    conn.commit()
    conn.close()

    logger.info(
        f"Load complete: {summary['inserted']} inserted, "
        f"{summary['skipped']} skipped, {summary['failed']} failed"
    )
    return summary


def log_pipeline_run(config: dict, run_start: datetime, status: str,
                     cities_attempted: int = 0, records_loaded: int = 0,
                     records_skipped: int = 0, error_message: str = None) -> None:
    """
    Log pipeline execution metadata.

    Parameters
    ----------
    config : dict
        Pipeline configuration.
    run_start : datetime
        Pipeline start timestamp.
    status : str
        Run status (SUCCESS / FAILED / PARTIAL).
    cities_attempted : int
        Number of cities attempted.
    records_loaded : int
        Records successfully loaded.
    records_skipped : int
        Duplicate records skipped.
    error_message : str, optional
        Error details if run failed.
    """
    db_path = config["database"]["path"]
    log_table = config["database"]["log_table"]

    run_end = datetime.now(timezone.utc)
    duration = (run_end - run_start).total_seconds()

    conn = get_connection(db_path)
    conn.execute(
        f"""
        INSERT INTO {log_table}
            (run_start_utc, run_end_utc, status, cities_attempted,
             records_loaded, records_skipped, error_message, duration_seconds)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            run_start.isoformat(),
            run_end.isoformat(),
            status,
            cities_attempted,
            records_loaded,
            records_skipped,
            error_message,
            round(duration, 2),
        ),
    )
    conn.commit()
    conn.close()

    logger.info(f"Pipeline run logged: status={status}, duration={duration:.2f}s")


def get_latest_data(config: dict) -> pd.DataFrame:
    """
    Retrieve the most recent weather observation per city.

    Used by the reporting module to generate dashboards.
    """
    db_path = config["database"]["path"]
    table_name = config["database"]["table_name"]

    conn = get_connection(db_path)

    query = f"""
        SELECT *
        FROM {table_name} w
        INNER JOIN (
            SELECT city, MAX(data_timestamp_utc) AS max_ts
            FROM {table_name}
            GROUP BY city
        ) latest ON w.city = latest.city AND w.data_timestamp_utc = latest.max_ts
        ORDER BY w.temperature_c DESC
    """

    df = pd.read_sql_query(query, conn)
    conn.close()
    return df


def get_pipeline_history(config: dict, limit: int = 10) -> pd.DataFrame:
    """Retrieve recent pipeline run logs."""
    db_path = config["database"]["path"]
    log_table = config["database"]["log_table"]

    conn = get_connection(db_path)
    df = pd.read_sql_query(
        f"SELECT * FROM {log_table} ORDER BY run_id DESC LIMIT ?",
        conn,
        params=(limit,),
    )
    conn.close()
    return df
