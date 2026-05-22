"""
tests/conftest.py
=================
Shared pytest fixtures used across all test files.

Fixtures here are automatically available to all tests
without needing to import them.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


# =============================================================================
# SETTINGS FIXTURE
# =============================================================================

@pytest.fixture
def mock_settings():
    """Mock settings object for tests that don't need real config."""
    settings = MagicMock()
    settings.kafka.broker = "localhost:9092"
    settings.kafka.topic_ride_requests = "ride-requests"
    settings.kafka.topic_dlq = "ride-requests-dlq"
    settings.kafka.batch_size = 100
    settings.cassandra.host = "localhost"
    settings.cassandra.port = 9042
    settings.cassandra.keyspace = "surge_pricing"
    settings.redis.host = "localhost"
    settings.redis.port = 6379
    settings.redis.db = 0
    settings.redis.ttl_seconds = 120
    settings.surge.threshold_low = 1.5
    settings.surge.threshold_medium = 2.0
    settings.surge.threshold_high = 3.0
    settings.surge.multiplier_low = 1.2
    settings.surge.multiplier_medium = 1.5
    settings.surge.multiplier_high = 2.0
    settings.surge.multiplier_max = 3.0
    settings.ml.confidence_threshold = 0.7
    settings.ml.model_path = "ml/models/surge_predictor.pkl"
    return settings


# =============================================================================
# SAMPLE DATA FIXTURES
# =============================================================================

@pytest.fixture
def sample_ride_event():
    """A valid ride request event."""
    return {
        "event_id": "test-123",
        "zone_id": "dr5ru6",
        "city": "new_york",
        "latitude": 40.7128,
        "longitude": -74.0060,
        "timestamp_ms": 1716048000000,
        "available_drivers": 15,
        "weather_severity": 1,
        "demand_count": 45,
    }


@pytest.fixture
def sample_surge_result():
    """A valid surge result dictionary."""
    return {
        "zone_id": "dr5ru6",
        "city": "new_york",
        "demand_1min": 120,
        "demand_5min": 480,
        "demand_15min": 1200,
        "available_drivers": 15,
        "supply_ratio": 0.125,
        "rule_multiplier": 2.0,
        "ml_multiplier": 2.1,
        "ml_confidence": 0.94,
        "final_multiplier": 2.1,
        "is_special_event": False,
        "weather_severity": 1,
    }


@pytest.fixture
def sample_features():
    """A complete 21-feature vector."""
    return {
        "zone_id": "dr5ru6",
        "city": "new_york",
        "demand_last_1min": 120,
        "demand_last_5min": 480,
        "demand_last_15min": 1200,
        "demand_spike_factor": 1.84,
        "available_drivers": 15,
        "supply_ratio": 0.125,
        "hour_of_day": 18,
        "day_of_week": 4,
        "is_weekend": False,
        "is_rush_hour": True,
        "is_late_night": False,
        "is_lunch_hour": False,
        "is_holiday": False,
        "days_to_next_holiday": 7,
        "temperature": 8.4,
        "precipitation": 2.3,
        "weather_severity": 3,
        "historical_avg_demand": 65.2,
        "historical_avg_surge": 1.8,
        "is_special_event": False,
        "event_demand_multiplier": 1.0,
    }