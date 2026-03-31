"""Tests for the Extract module."""

import pytest
from unittest.mock import patch, MagicMock
from src.extract import fetch_weather_for_city, load_config


# Sample API response fixture
SAMPLE_API_RESPONSE = {
    "coord": {"lon": -0.1257, "lat": 51.5085},
    "weather": [{"id": 800, "main": "Clear", "description": "clear sky", "icon": "01d"}],
    "main": {
        "temp": 15.2,
        "feels_like": 14.1,
        "temp_min": 13.0,
        "temp_max": 17.5,
        "pressure": 1013,
        "humidity": 72,
    },
    "visibility": 10000,
    "wind": {"speed": 3.6, "deg": 220},
    "clouds": {"all": 0},
    "dt": 1700000000,
    "sys": {"country": "GB", "sunrise": 1699950000, "sunset": 1699985000},
    "name": "London",
}


class TestFetchWeatherForCity:
    """Tests for the API fetch function."""

    @patch("src.extract.requests.get")
    def test_successful_fetch(self, mock_get):
        """Test successful API response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_API_RESPONSE
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        result = fetch_weather_for_city(
            city="London",
            api_key="test_key",
            base_url="https://api.openweathermap.org/data/2.5/weather",
        )

        assert result is not None
        assert result["name"] == "London"
        assert result["main"]["temp"] == 15.2

    @patch("src.extract.requests.get")
    def test_city_not_found_returns_none(self, mock_get):
        """Test 404 response for invalid city."""
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = Exception("404")
        mock_get.return_value = mock_response

        result = fetch_weather_for_city(
            city="FakeCity123",
            api_key="test_key",
            base_url="https://api.openweathermap.org/data/2.5/weather",
            max_retries=1,
        )

        assert result is None

    @patch("src.extract.requests.get")
    def test_invalid_api_key_returns_none(self, mock_get):
        """Test 401 response for bad API key."""
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.raise_for_status.side_effect = Exception("401")
        mock_get.return_value = mock_response

        result = fetch_weather_for_city(
            city="London",
            api_key="bad_key",
            base_url="https://api.openweathermap.org/data/2.5/weather",
            max_retries=1,
        )

        assert result is None

    @patch("src.extract.requests.get")
    def test_correct_params_sent(self, mock_get):
        """Test that correct parameters are sent to the API."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = SAMPLE_API_RESPONSE
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        fetch_weather_for_city(
            city="Tokyo",
            api_key="my_key",
            base_url="https://api.example.com",
            units="imperial",
        )

        mock_get.assert_called_once()
        call_kwargs = mock_get.call_args
        assert call_kwargs[1]["params"]["q"] == "Tokyo"
        assert call_kwargs[1]["params"]["appid"] == "my_key"
        assert call_kwargs[1]["params"]["units"] == "imperial"


class TestLoadConfig:
    """Tests for configuration loading."""

    def test_config_loads_successfully(self):
        """Test that the YAML config file loads."""
        config = load_config("config/pipeline_config.yaml")
        assert "api" in config
        assert "database" in config
        assert "cities" in config["api"]
        assert isinstance(config["api"]["cities"], list)
