"""Tests for the Load module."""

import os
import sqlite3
import pytest
import pandas as pd
from datetime import datetime, timezone

from src.load import get_connection, initialize_database, load, get_latest_data


TEST_DB = "data/test_weather.db"
TEST_CONFIG = {
    "database": {
        "path": TEST_DB,
        "table_name": "weather_data",
        "log_table": "pipeline_runs",
    },
    "paths": {
        "raw_data": "data/raw",
        "processed_data": "data/processed",
        "reports": "dashboards",
        "logs": "logs",
    },
}


def _sample_df():
    """Create a sample DataFrame matching the expected schema."""
    return pd.DataFrame([
        {
            "city": "London",
            "country": "GB",
            "latitude": 51.51,
            "longitude": -0.13,
            "temperature_c": 12.5,
            "feels_like_c": 11.0,
            "temp_min_c": 10.0,
            "temp_max_c": 14.0,
            "humidity_pct": 85.0,
            "pressure_hpa": 1015.0,
            "wind_speed_mps": 5.2,
            "wind_direction_deg": 180.0,
            "cloudiness_pct": 75.0,
            "weather_condition": "Rain",
            "weather_description": "light rain",
            "weather_icon": "10d",
            "visibility_m": 8000.0,
            "sunrise_utc": "2023-11-14T07:00:00+00:00",
            "sunset_utc": "2023-11-14T16:30:00+00:00",
            "data_timestamp_utc": "2023-11-14T12:00:00+00:00",
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "temperature_category": "Mild",
            "humidity_category": "High",
            "wind_category": "Moderate",
            "comfort_index": 65.0,
        },
        {
            "city": "Tokyo",
            "country": "JP",
            "latitude": 35.68,
            "longitude": 139.69,
            "temperature_c": 22.0,
            "feels_like_c": 21.5,
            "temp_min_c": 20.0,
            "temp_max_c": 24.0,
            "humidity_pct": 60.0,
            "pressure_hpa": 1018.0,
            "wind_speed_mps": 2.1,
            "wind_direction_deg": 90.0,
            "cloudiness_pct": 20.0,
            "weather_condition": "Clear",
            "weather_description": "clear sky",
            "weather_icon": "01d",
            "visibility_m": 10000.0,
            "sunrise_utc": "2023-11-14T06:15:00+00:00",
            "sunset_utc": "2023-11-14T16:45:00+00:00",
            "data_timestamp_utc": "2023-11-14T12:00:00+00:00",
            "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
            "temperature_category": "Warm",
            "humidity_category": "Moderate",
            "wind_category": "Light",
            "comfort_index": 88.5,
        },
    ])


@pytest.fixture(autouse=True)
def cleanup_test_db():
    """Remove test database before and after each test."""
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)
    yield
    if os.path.exists(TEST_DB):
        os.remove(TEST_DB)


class TestDatabaseSetup:
    """Tests for database initialization."""

    def test_creates_tables(self):
        conn = get_connection(TEST_DB)
        initialize_database(conn, TEST_CONFIG)

        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert "weather_data" in tables
        assert "pipeline_runs" in tables

    def test_creates_indices(self):
        conn = get_connection(TEST_DB)
        initialize_database(conn, TEST_CONFIG)

        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        indices = [row[0] for row in cursor.fetchall()]
        conn.close()

        assert any("city" in idx for idx in indices)
        assert any("timestamp" in idx for idx in indices)


class TestLoad:
    """Tests for the data loading function."""

    def test_loads_records(self):
        df = _sample_df()
        summary = load(df, TEST_CONFIG)

        assert summary["inserted"] == 2
        assert summary["skipped"] == 0
        assert summary["failed"] == 0

    def test_upsert_prevents_duplicates(self):
        df = _sample_df()

        # First load
        load(df, TEST_CONFIG)
        # Second load (same data)
        summary = load(df, TEST_CONFIG)

        assert summary["inserted"] == 0
        assert summary["skipped"] == 2

    def test_empty_dataframe(self):
        df = pd.DataFrame()
        summary = load(df, TEST_CONFIG)

        assert summary["inserted"] == 0

    def test_data_retrievable_after_load(self):
        df = _sample_df()
        load(df, TEST_CONFIG)

        result = get_latest_data(TEST_CONFIG)
        assert len(result) == 2
        assert "London" in result["city"].values
        assert "Tokyo" in result["city"].values
