"""
tests/test_feature_engineering.py
===================================
Unit tests for ML feature engineering.
"""

from __future__ import annotations

import pytest
from ml.feature_engineering import (
    compute_time_features,
    compute_weather_features,
    compute_demand_features,
    compute_supply_features,
    FEATURE_COLUMNS,
)


class TestTimeFeatures:
    """Tests for time-based feature computation."""

    def test_rush_hour_morning_weekday(self):
        """Monday 8am should be rush hour."""
        from datetime import datetime
        import pytz
        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2026, 5, 18, 8, 0, 0))
        features = compute_time_features(dt)
        assert features["is_rush_hour"] is True
        assert features["is_weekend"] is False

    def test_weekend(self):
        """Saturday should be weekend."""
        from datetime import datetime
        import pytz
        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2026, 5, 23, 12, 0, 0))
        features = compute_time_features(dt)
        assert features["is_weekend"] is True
        assert features["is_rush_hour"] is False

    def test_late_night(self):
        """11pm should be late night."""
        from datetime import datetime
        import pytz
        tz = pytz.timezone("America/New_York")
        dt = tz.localize(datetime(2026, 5, 18, 23, 0, 0))
        features = compute_time_features(dt)
        assert features["is_late_night"] is True


class TestWeatherFeatures:
    """Tests for weather feature extraction."""

    def test_clear_weather(self):
        """Clear weather should give severity 1."""
        weather = {"temperature": 20.0, "precipitation": 0.0, "severity": 1}
        features = compute_weather_features(weather)
        assert features["weather_severity"] == 1
        assert features["precipitation"] == 0.0

    def test_heavy_rain(self):
        """Heavy rain should give severity 4."""
        weather = {"temperature": 8.0, "precipitation": 8.0, "severity": 4}
        features = compute_weather_features(weather)
        assert features["weather_severity"] == 4


class TestDemandFeatures:
    """Tests for demand feature computation."""

    def test_spike_factor_calculation(self):
        """Spike factor should be demand/historical."""
        features = compute_demand_features(
            demand_1min=120,
            demand_5min=480,
            demand_15min=1200,
            historical_avg_demand=65.0,
        )
        assert features["demand_last_1min"] == 120
        assert round(features["demand_spike_factor"], 2) == round(120 / 65.0, 2)

    def test_zero_historical_no_division_error(self):
        """Zero historical average should not cause division error."""
        features = compute_demand_features(
            demand_1min=50,
            demand_5min=200,
            demand_15min=500,
            historical_avg_demand=0.0,
        )
        assert features["demand_spike_factor"] == 1.0


class TestFeatureColumns:
    """Tests for feature column definitions."""

    def test_exactly_21_features(self):
        """FEATURE_COLUMNS should have exactly 21 features."""
        assert len(FEATURE_COLUMNS) == 21

    def test_no_duplicate_columns(self):
        """Feature columns should have no duplicates."""
        assert len(FEATURE_COLUMNS) == len(set(FEATURE_COLUMNS))