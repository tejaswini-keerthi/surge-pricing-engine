"""
ml/feature_engineering.py
==========================
Computes all 21 ML features per zone per time window.

Features:
    Demand (4):   1min, 5min, 15min counts + spike factor
    Supply (2):   available drivers + supply ratio
    Time (6):     hour, day, weekend, rush hour, late night, lunch
    Calendar (2): is_holiday + days_to_next_holiday
    Weather (3):  temperature, precipitation, severity
    Historical(2):avg demand + avg surge from Cassandra
    Events (2):   is_special_event + event_multiplier
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any

import holidays as hd
import pytz
from loguru import logger

from config.settings import settings


# =============================================================================
# TIME FEATURES
# =============================================================================

def compute_time_features(local_dt: datetime) -> dict[str, Any]:
    """
    Compute time-based features from local datetime.

    Args:
        local_dt: Datetime in city's local timezone

    Returns:
        Dictionary of time features
    """
    hour = local_dt.hour
    dow = local_dt.weekday()

    return {
        "hour_of_day": hour,
        "day_of_week": dow,
        "is_weekend": dow >= 5,
        "is_rush_hour": (
            (7 <= hour <= 9 or 17 <= hour <= 19) and dow < 5
        ),
        "is_late_night": hour in [22, 23, 0, 1, 2],
        "is_lunch_hour": 11 <= hour <= 13,
    }


# =============================================================================
# CALENDAR FEATURES
# =============================================================================

def compute_calendar_features(current_date: object) -> dict[str, Any]:
    """
    Compute holiday-based features.

    Args:
        current_date: datetime.date object

    Returns:
        Dictionary with is_holiday and days_to_next_holiday
    """
    try:
        us_holidays = hd.US()
        is_holiday = current_date in us_holidays

        # Find days to next holiday
        days_to_next = settings.holidays.lookahead_days
        for i in range(1, settings.holidays.lookahead_days + 1):
            future_date = current_date + timedelta(days=i)
            if future_date in us_holidays:
                days_to_next = i
                break

        return {
            "is_holiday": is_holiday,
            "days_to_next_holiday": days_to_next,
        }

    except Exception as e:
        logger.warning(f"Calendar feature computation failed: {e}")
        return {
            "is_holiday": False,
            "days_to_next_holiday": 7,
        }


# =============================================================================
# WEATHER FEATURES
# =============================================================================

def compute_weather_features(
    weather_data: dict[str, Any],
) -> dict[str, Any]:
    """
    Extract weather features from Open-Meteo data.

    Args:
        weather_data: Dictionary from WeatherClient

    Returns:
        Dictionary of weather features
    """
    return {
        "temperature": float(weather_data.get("temperature", 15.0)),
        "precipitation": float(weather_data.get("precipitation", 0.0)),
        "weather_severity": int(weather_data.get("severity", 1)),
    }


# =============================================================================
# DEMAND FEATURES
# =============================================================================

def compute_demand_features(
    demand_1min: int,
    demand_5min: int,
    demand_15min: int,
    historical_avg_demand: float,
) -> dict[str, Any]:
    """
    Compute demand-based features including spike factor.

    Args:
        demand_1min: Ride requests in last 60 seconds
        demand_5min: Ride requests in last 5 minutes
        demand_15min: Ride requests in last 15 minutes
        historical_avg_demand: Historical average for this zone/hour/day

    Returns:
        Dictionary of demand features
    """
    spike_factor = (
        demand_1min / historical_avg_demand
        if historical_avg_demand > 0
        else 1.0
    )

    return {
        "demand_last_1min": demand_1min,
        "demand_last_5min": demand_5min,
        "demand_last_15min": demand_15min,
        "demand_spike_factor": round(spike_factor, 3),
    }


# =============================================================================
# SUPPLY FEATURES
# =============================================================================

def compute_supply_features(
    available_drivers: int,
    demand_1min: int,
) -> dict[str, Any]:
    """
    Compute supply-based features.

    Args:
        available_drivers: Number of available drivers in zone
        demand_1min: Current demand count

    Returns:
        Dictionary of supply features
    """
    supply_ratio = (
        available_drivers / demand_1min
        if demand_1min > 0
        else float(available_drivers)
    )

    return {
        "available_drivers": available_drivers,
        "supply_ratio": round(supply_ratio, 3),
    }


# =============================================================================
# FULL FEATURE VECTOR
# =============================================================================

def compute_features(
    zone_id: str,
    city: str,
    demand_1min: int,
    demand_5min: int,
    demand_15min: int,
    available_drivers: int,
    weather_data: dict[str, Any],
    historical_avg_demand: float,
    historical_avg_surge: float,
    is_special_event: bool,
    event_multiplier: float,
    timezone: str = "America/New_York",
) -> dict[str, Any]:
    """
    Compute the complete 21-feature vector for a zone.

    Args:
        zone_id: Zone identifier
        city: City name
        demand_1min: 1-minute demand count
        demand_5min: 5-minute demand count
        demand_15min: 15-minute demand count
        available_drivers: Driver supply count
        weather_data: Weather dictionary from WeatherClient
        historical_avg_demand: Historical demand baseline
        historical_avg_surge: Historical surge baseline
        is_special_event: Whether an event is active
        event_multiplier: Event demand multiplier
        timezone: City timezone string

    Returns:
        Complete feature dictionary with all 21 features
    """
    try:
        tz = pytz.timezone(timezone)
        local_dt = datetime.now(tz)

        # Compute all feature groups
        time_features = compute_time_features(local_dt)
        calendar_features = compute_calendar_features(local_dt.date())
        weather_features = compute_weather_features(weather_data)
        demand_features = compute_demand_features(
            demand_1min, demand_5min, demand_15min, historical_avg_demand
        )
        supply_features = compute_supply_features(available_drivers, demand_1min)

        # Combine all features
        features = {
            "zone_id": zone_id,
            "city": city,
            "timestamp": local_dt.isoformat(),
            # Demand (4)
            **demand_features,
            # Supply (2)
            **supply_features,
            # Time (6)
            **time_features,
            # Calendar (2)
            **calendar_features,
            # Weather (3)
            **weather_features,
            # Historical (2)
            "historical_avg_demand": round(historical_avg_demand, 2),
            "historical_avg_surge": round(historical_avg_surge, 2),
            # Events (2)
            "is_special_event": is_special_event,
            "event_demand_multiplier": round(event_multiplier, 2),
        }

        return features

    except Exception as e:
        logger.error(f"Feature computation failed for {zone_id}: {e}")
        return _default_features(zone_id, city)


def _default_features(zone_id: str, city: str) -> dict[str, Any]:
    """Return safe default features when computation fails."""
    return {
        "zone_id": zone_id,
        "city": city,
        "timestamp": datetime.now().isoformat(),
        "demand_last_1min": 0,
        "demand_last_5min": 0,
        "demand_last_15min": 0,
        "demand_spike_factor": 1.0,
        "available_drivers": 30,
        "supply_ratio": 1.0,
        "hour_of_day": 12,
        "day_of_week": 0,
        "is_weekend": False,
        "is_rush_hour": False,
        "is_late_night": False,
        "is_lunch_hour": False,
        "is_holiday": False,
        "days_to_next_holiday": 7,
        "temperature": 15.0,
        "precipitation": 0.0,
        "weather_severity": 1,
        "historical_avg_demand": 20.0,
        "historical_avg_surge": 1.0,
        "is_special_event": False,
        "event_demand_multiplier": 1.0,
    }


# Feature names in exact order expected by ML model
FEATURE_COLUMNS = [
    "demand_last_1min",
    "demand_last_5min",
    "demand_last_15min",
    "demand_spike_factor",
    "available_drivers",
    "supply_ratio",
    "hour_of_day",
    "day_of_week",
    "is_weekend",
    "is_rush_hour",
    "is_late_night",
    "is_lunch_hour",
    "is_holiday",
    "days_to_next_holiday",
    "temperature",
    "precipitation",
    "weather_severity",
    "historical_avg_demand",
    "historical_avg_surge",
    "is_special_event",
    "event_demand_multiplier",
]