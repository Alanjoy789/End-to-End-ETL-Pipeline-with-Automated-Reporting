"""Tests for the Transform module."""

import pytest
import pandas as pd
from src.transform import parse_raw_record, validate_record, enrich_dataframe, _unix_to_iso


SAMPLE_RAW = {
    "coord": {"lon": -0.13, "lat": 51.51},
    "weather": [{"main": "Rain", "description": "light rain", "icon": "10d"}],
    "main": {
        "temp": 12.5,
        "feels_like": 11.0,
        "temp_min": 10.0,
        "temp_max": 14.0,
        "pressure": 1015,
        "humidity": 85,
    },
    "visibility": 8000,
    "wind": {"speed": 5.2, "deg": 180},
    "clouds": {"all": 75},
    "dt": 1700000000,
    "sys": {"country": "GB", "sunrise": 1699950000, "sunset": 1699985000},
    "name": "London",
}


class TestParseRawRecord:
    """Tests for raw record parsing."""

    def test_parses_all_fields(self):
        record = parse_raw_record(SAMPLE_RAW)
        assert record["city"] == "London"
        assert record["country"] == "GB"
        assert record["temperature_c"] == 12.5
        assert record["humidity_pct"] == 85
        assert record["wind_speed_mps"] == 5.2
        assert record["weather_condition"] == "Rain"

    def test_handles_missing_fields(self):
        minimal = {"name": "Test", "main": {"temp": 20, "humidity": 50}, "weather": [{}]}
        record = parse_raw_record(minimal)
        assert record["city"] == "Test"
        assert record["wind_speed_mps"] is None
        assert record["country"] == "N/A"

    def test_extracted_at_is_set(self):
        record = parse_raw_record(SAMPLE_RAW)
        assert record["extracted_at_utc"] is not None
        assert "T" in record["extracted_at_utc"]  # ISO format


class TestValidateRecord:
    """Tests for data validation."""

    def test_valid_record_passes(self):
        record = parse_raw_record(SAMPLE_RAW)
        is_valid, issues = validate_record(record)
        assert is_valid is True
        assert len(issues) == 0

    def test_out_of_range_temperature_flagged(self):
        record = parse_raw_record(SAMPLE_RAW)
        record["temperature_c"] = 99  # Above valid max of 60
        is_valid, issues = validate_record(record)
        assert is_valid is False
        assert any("temperature_c" in i for i in issues)

    def test_missing_required_field_flagged(self):
        record = parse_raw_record(SAMPLE_RAW)
        record["city"] = None
        is_valid, issues = validate_record(record)
        assert is_valid is False
        assert any("city" in i for i in issues)

    def test_out_of_range_humidity_flagged(self):
        record = parse_raw_record(SAMPLE_RAW)
        record["humidity_pct"] = 150
        is_valid, issues = validate_record(record)
        assert is_valid is False


class TestEnrichDataframe:
    """Tests for data enrichment."""

    def _make_df(self, temp=20, humidity=50, wind=3):
        return pd.DataFrame([{
            "temperature_c": temp,
            "humidity_pct": humidity,
            "wind_speed_mps": wind,
        }])

    def test_temperature_categories(self):
        df = enrich_dataframe(self._make_df(temp=-5))
        assert df["temperature_category"].iloc[0] == "Freezing"

        df = enrich_dataframe(self._make_df(temp=5))
        assert df["temperature_category"].iloc[0] == "Cold"

        df = enrich_dataframe(self._make_df(temp=25))
        assert df["temperature_category"].iloc[0] == "Warm"

        df = enrich_dataframe(self._make_df(temp=35))
        assert df["temperature_category"].iloc[0] == "Hot"

    def test_comfort_index_in_range(self):
        df = enrich_dataframe(self._make_df())
        assert 0 <= df["comfort_index"].iloc[0] <= 100

    def test_all_enrichment_columns_added(self):
        df = enrich_dataframe(self._make_df())
        assert "temperature_category" in df.columns
        assert "humidity_category" in df.columns
        assert "wind_category" in df.columns
        assert "comfort_index" in df.columns


class TestUnixToIso:
    """Tests for timestamp conversion."""

    def test_converts_timestamp(self):
        result = _unix_to_iso(1700000000)
        assert result is not None
        assert "2023-11-14" in result

    def test_none_input(self):
        assert _unix_to_iso(None) is None
