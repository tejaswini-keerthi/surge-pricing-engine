"""
simulator/weather_client.py
===========================
Fetches real-time weather data from Open-Meteo API (free, no key required).

Updates weather cache in Redis every 10 minutes.
Weather data is used by the simulator and ML feature engineering.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

import httpx
import redis
import json
from loguru import logger

from config.settings import settings


# =============================================================================
# WMO WEATHER CODE → SEVERITY MAPPING
# =============================================================================

def wmo_code_to_severity(wmo_code: int) -> int:
    """
    Convert WMO weather code to 1-5 severity scale.

    WMO codes:
        0       = clear sky
        1-3     = mainly clear to overcast
        45,48   = fog
        51-55   = drizzle
        61-65   = rain
        71-75   = snow
        80-82   = rain showers
        95      = thunderstorm

    Args:
        wmo_code: WMO weather observation code

    Returns:
        Severity integer 1-5
    """
    if wmo_code == 0:
        return 1  # clear
    elif wmo_code in [1, 2, 3]:
        return 2  # cloudy
    elif wmo_code in [45, 48, 51, 52, 53]:
        return 2  # fog / light drizzle
    elif wmo_code in [55, 61, 62, 63, 80, 81]:
        return 3  # moderate rain
    elif wmo_code in [64, 65, 71, 72, 73, 82]:
        return 4  # heavy rain / snow
    elif wmo_code in [74, 75, 95, 96, 99]:
        return 5  # blizzard / thunderstorm
    return 1


# =============================================================================
# WEATHER CLIENT
# =============================================================================

class WeatherClient:
    """
    Fetches weather from Open-Meteo and caches in Redis.

    Open-Meteo is completely free with no API key required.
    We fetch weather every 10 minutes per city to stay well
    within the 10,000 daily request limit.
    """

    BASE_URL = "https://api.open-meteo.com/v1/forecast"

    def __init__(self) -> None:
        self.redis_client = redis.Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            decode_responses=True,
        )
        self.cities = settings.cities
        logger.info("WeatherClient initialized")

    def _fetch_weather(self, latitude: float, longitude: float) -> dict[str, Any]:
        """
        Fetch current weather from Open-Meteo API.

        Args:
            latitude: City latitude
            longitude: City longitude

        Returns:
            Dictionary with temperature, precipitation, weathercode, windspeed
        """
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "current_weather": True,
            "hourly": "precipitation,apparent_temperature",
            "forecast_days": 1,
        }

        try:
            with httpx.Client(timeout=10.0) as client:
                response = client.get(self.BASE_URL, params=params)
                response.raise_for_status()
                data = response.json()

                current = data.get("current_weather", {})
                hourly = data.get("hourly", {})

                # Get current hour precipitation
                current_hour = datetime.now().hour
                precipitation = 0.0
                if "precipitation" in hourly:
                    precip_list = hourly["precipitation"]
                    if len(precip_list) > current_hour:
                        precipitation = precip_list[current_hour]

                return {
                    "temperature": current.get("temperature", 15.0),
                    "windspeed": current.get("windspeed", 0.0),
                    "weathercode": int(current.get("weathercode", 0)),
                    "precipitation": precipitation,
                }

        except httpx.TimeoutException:
            logger.warning(f"Weather API timeout for ({latitude}, {longitude})")
            return self._default_weather()

        except Exception as e:
            logger.error(f"Weather fetch failed: {e}")
            return self._default_weather()

    def _default_weather(self) -> dict[str, Any]:
        """Return default clear weather when API is unavailable."""
        return {
            "temperature": 15.0,
            "windspeed": 0.0,
            "weathercode": 0,
            "precipitation": 0.0,
        }

    def _cache_weather(self, city_name: str, weather_data: dict[str, Any]) -> None:
        """
        Cache weather data in Redis.

        Args:
            city_name: City identifier
            weather_data: Weather dictionary to cache
        """
        key = f"weather:{city_name}"
        weather_data["severity"] = wmo_code_to_severity(
            weather_data["weathercode"]
        )
        weather_data["updated_at"] = datetime.now().isoformat()

        self.redis_client.setex(
            key,
            settings.weather.cache_ttl_seconds,
            json.dumps(weather_data),
        )
        logger.debug(
            f"Weather cached for {city_name}: "
            f"severity={weather_data['severity']}, "
            f"temp={weather_data['temperature']}°C"
        )

    def get_cached_weather(self, city_name: str) -> dict[str, Any]:
        """
        Get cached weather for a city from Redis.

        Args:
            city_name: City identifier

        Returns:
            Weather dictionary or default if not cached
        """
        key = f"weather:{city_name}"
        cached = self.redis_client.get(key)

        if cached:
            return json.loads(cached)

        logger.warning(f"No cached weather for {city_name}, using default")
        return {**self._default_weather(), "severity": 1}

    def update_all_cities(self) -> None:
        """Fetch and cache weather for all configured cities."""
        for city in self.cities:
            logger.info(f"Fetching weather for {city.name}")
            weather = self._fetch_weather(city.latitude, city.longitude)
            self._cache_weather(city.name, weather)

    def run(self) -> None:
        """
        Continuously update weather every WEATHER_UPDATE_INTERVAL_SECONDS.

        Runs as a background process alongside the simulator.
        """
        logger.info(
            f"Weather updater started — "
            f"interval: {settings.weather.update_interval_seconds}s"
        )

        while True:
            try:
                self.update_all_cities()
            except Exception as e:
                logger.error(f"Weather update cycle failed: {e}")

            time.sleep(settings.weather.update_interval_seconds)


if __name__ == "__main__":
    client = WeatherClient()
    client.run()