"""
Transform Module
================
Cleans, validates, and enriches raw weather API data into a structured DataFrame.
Handles missing values, type casting, and feature engineering.
"""

import logging
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

logger = logging.getLogger(__name__)

# Valid ranges for data quality checks
VALID_RANGES = {
    "temperature_c": (-90, 60),
    "humidity_pct": (0, 100),
    "wind_speed_mps": (0, 120),
    "pressure_hpa": (870, 1085),
}


def parse_raw_record(raw: dict) -> dict:
    """
    Parse a single raw API response into a flat dictionary.

    Parameters
    ----------
    raw : dict
        Single raw response from OpenWeatherMap API.

    Returns
    -------
    dict
        Flattened and renamed fields.
    """
    return {
        "city": raw.get("name", "Unknown"),
        "country": raw.get("sys", {}).get("country", "N/A"),
        "latitude": raw.get("coord", {}).get("lat"),
        "longitude": raw.get("coord", {}).get("lon"),
        "temperature_c": raw.get("main", {}).get("temp"),
        "feels_like_c": raw.get("main", {}).get("feels_like"),
        "temp_min_c": raw.get("main", {}).get("temp_min"),
        "temp_max_c": raw.get("main", {}).get("temp_max"),
        "humidity_pct": raw.get("main", {}).get("humidity"),
        "pressure_hpa": raw.get("main", {}).get("pressure"),
        "wind_speed_mps": raw.get("wind", {}).get("speed"),
        "wind_direction_deg": raw.get("wind", {}).get("deg"),
        "cloudiness_pct": raw.get("clouds", {}).get("all"),
        "weather_condition": raw.get("weather", [{}])[0].get("main", "Unknown"),
        "weather_description": raw.get("weather", [{}])[0].get("description", "N/A"),
        "weather_icon": raw.get("weather", [{}])[0].get("icon", "01d"),
        "visibility_m": raw.get("visibility"),
        "sunrise_utc": _unix_to_iso(raw.get("sys", {}).get("sunrise")),
        "sunset_utc": _unix_to_iso(raw.get("sys", {}).get("sunset")),
        "data_timestamp_utc": _unix_to_iso(raw.get("dt")),
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
    }


def _unix_to_iso(ts: int | None) -> str | None:
    """Convert UNIX timestamp to ISO 8601 string."""
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def validate_record(record: dict) -> tuple[bool, list[str]]:
    """
    Validate a single record against expected ranges.

    Returns
    -------
    tuple[bool, list[str]]
        (is_valid, list_of_issues)
    """
    issues = []

    for field, (low, high) in VALID_RANGES.items():
        value = record.get(field)
        if value is not None and not (low <= value <= high):
            issues.append(f"{field}={value} outside range [{low}, {high}]")

    # Required fields check
    for req_field in ["city", "temperature_c", "humidity_pct"]:
        if record.get(req_field) is None:
            issues.append(f"Missing required field: {req_field}")

    return len(issues) == 0, issues


def enrich_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add derived columns for analytics.

    Enrichments:
    - temperature_category: Freezing / Cold / Mild / Warm / Hot
    - humidity_category: Low / Moderate / High / Very High
    - wind_category: Calm / Light / Moderate / Strong / Storm
    - comfort_index: Simple comfort score (0-100)
    """
    df = df.copy()

    # Temperature category
    df["temperature_category"] = pd.cut(
        df["temperature_c"],
        bins=[-100, 0, 10, 20, 30, 100],
        labels=["Freezing", "Cold", "Mild", "Warm", "Hot"],
    )

    # Humidity category
    df["humidity_category"] = pd.cut(
        df["humidity_pct"],
        bins=[-1, 30, 60, 80, 101],
        labels=["Low", "Moderate", "High", "Very High"],
    )

    # Wind category (m/s)
    df["wind_category"] = pd.cut(
        df["wind_speed_mps"],
        bins=[-1, 1, 5, 10, 20, 200],
        labels=["Calm", "Light", "Moderate", "Strong", "Storm"],
    )

    # Comfort index: 100 = ideal (20°C, 50% humidity, low wind)
    temp_score = 100 - abs(df["temperature_c"] - 20) * 3
    humidity_score = 100 - abs(df["humidity_pct"] - 50) * 1.2
    wind_score = 100 - df["wind_speed_mps"] * 5
    df["comfort_index"] = (
        (temp_score * 0.5 + humidity_score * 0.3 + wind_score * 0.2)
        .clip(0, 100)
        .round(1)
    )

    return df


def transform(raw_data: list[dict], config: dict) -> pd.DataFrame:
    """
    Full transformation pipeline.

    Steps:
    1. Parse raw API responses → flat dicts
    2. Validate each record
    3. Build DataFrame, handle types
    4. Enrich with derived features
    5. Save processed CSV

    Parameters
    ----------
    raw_data : list[dict]
        List of raw API responses.
    config : dict
        Pipeline configuration.

    Returns
    -------
    pd.DataFrame
        Cleaned and enriched DataFrame.
    """
    if not raw_data:
        logger.warning("No raw data to transform")
        return pd.DataFrame()

    # Step 1 & 2: Parse and validate
    parsed_records = []
    for raw in raw_data:
        record = parse_raw_record(raw)
        is_valid, issues = validate_record(record)

        if is_valid:
            parsed_records.append(record)
        else:
            logger.warning(f"Validation issues for {record.get('city')}: {issues}")
            parsed_records.append(record)  # Keep but log issues

    # Step 3: Build DataFrame
    df = pd.DataFrame(parsed_records)

    numeric_cols = [
        "temperature_c", "feels_like_c", "temp_min_c", "temp_max_c",
        "humidity_pct", "pressure_hpa", "wind_speed_mps",
        "wind_direction_deg", "cloudiness_pct", "visibility_m",
        "latitude", "longitude",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Step 4: Enrich
    df = enrich_dataframe(df)

    logger.info(f"Transformation complete: {len(df)} records, {len(df.columns)} columns")

    # Step 5: Save processed CSV
    _save_processed_data(df, config)

    return df


def _save_processed_data(df: pd.DataFrame, config: dict) -> None:
    """Save processed DataFrame to CSV."""
    proc_path = Path(config["paths"]["processed_data"])
    proc_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = proc_path / f"processed_weather_{timestamp}.csv"

    df.to_csv(filepath, index=False)
    logger.info(f"Processed data saved to {filepath}")
