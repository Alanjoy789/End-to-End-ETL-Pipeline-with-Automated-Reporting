"""
Extract Module
==============
Pulls current weather data from the OpenWeatherMap API for configured cities.
Implements retry logic, error handling, and raw data archival.
"""

import json
import logging
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import requests
import yaml

logger = logging.getLogger(__name__)


def load_config(config_path: str = "config/pipeline_config.yaml") -> dict:
    """Load pipeline configuration from YAML file."""
    with open(config_path, "r") as f:
        config = yaml.safe_load(f)
    return config


def fetch_weather_for_city(
    city: str,
    api_key: str,
    base_url: str,
    units: str = "metric",
    timeout: int = 10,
    max_retries: int = 3,
    retry_delay: int = 2,
) -> dict[str, Any] | None:
    """
    Fetch current weather data for a single city.

    Uses exponential backoff retry logic for resilience.

    Parameters
    ----------
    city : str
        City name (e.g., "London", "New York").
    api_key : str
        OpenWeatherMap API key.
    base_url : str
        API base URL.
    units : str
        Temperature units (metric/imperial/standard).
    timeout : int
        Request timeout in seconds.
    max_retries : int
        Maximum retry attempts.
    retry_delay : int
        Base delay between retries (doubles each attempt).

    Returns
    -------
    dict or None
        Parsed JSON response, or None if all retries failed.
    """
    params = {"q": city, "appid": api_key, "units": units}

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(f"Fetching weather for '{city}' (attempt {attempt}/{max_retries})")
            response = requests.get(base_url, params=params, timeout=timeout)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Successfully fetched data for '{city}'")
            return data

        except requests.exceptions.HTTPError as e:
            logger.warning(f"HTTP error for '{city}': {e}")
            if response.status_code == 401:
                logger.error("Invalid API key. Check config/pipeline_config.yaml")
                return None
            if response.status_code == 404:
                logger.error(f"City '{city}' not found. Check spelling.")
                return None
        except requests.exceptions.ConnectionError:
            logger.warning(f"Connection error for '{city}' (attempt {attempt})")
        except requests.exceptions.Timeout:
            logger.warning(f"Timeout for '{city}' (attempt {attempt})")
        except requests.exceptions.RequestException as e:
            logger.warning(f"Request error for '{city}': {e}")

        if attempt < max_retries:
            wait = retry_delay * (2 ** (attempt - 1))
            logger.info(f"Retrying in {wait}s...")
            time.sleep(wait)

    logger.error(f"All {max_retries} attempts failed for '{city}'")
    return None


def extract(config: dict) -> list[dict[str, Any]]:
    """
    Extract weather data for all configured cities.

    Parameters
    ----------
    config : dict
        Pipeline configuration dictionary.

    Returns
    -------
    list[dict]
        List of raw API responses (one per successful city).
    """
    api_conf = config["api"]
    api_key = api_conf["key"]
    base_url = api_conf["base_url"]
    cities = api_conf["cities"]
    units = api_conf.get("units", "metric")
    timeout = api_conf.get("timeout", 10)
    max_retries = api_conf.get("max_retries", 3)
    retry_delay = api_conf.get("retry_delay", 2)

    if api_key == "YOUR_API_KEY_HERE":
        raise ValueError(
            "API key not set! Edit config/pipeline_config.yaml with your OpenWeatherMap key."
        )

    raw_results = []

    for city in cities:
        data = fetch_weather_for_city(
            city=city,
            api_key=api_key,
            base_url=base_url,
            units=units,
            timeout=timeout,
            max_retries=max_retries,
            retry_delay=retry_delay,
        )
        if data:
            raw_results.append(data)

    logger.info(f"Extraction complete: {len(raw_results)}/{len(cities)} cities successful")

    # Archive raw data
    _save_raw_data(raw_results, config)

    return raw_results


def _save_raw_data(data: list[dict], config: dict) -> None:
    """Save raw API responses to JSON for auditability."""
    raw_path = Path(config["paths"]["raw_data"])
    raw_path.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filepath = raw_path / f"raw_weather_{timestamp}.json"

    with open(filepath, "w") as f:
        json.dump(data, f, indent=2)

    logger.info(f"Raw data saved to {filepath}")
