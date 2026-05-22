"""
config/settings.py
==================
Central configuration management for the Surge Pricing Engine.

All configuration is loaded from environment variables (via .env file).
This module provides a single, validated, typed settings object that
every other module imports. No other module should read environment
variables directly.

Usage:
    from config.settings import settings

    broker = settings.kafka.broker
    port = settings.cassandra.port
    threshold = settings.surge.threshold_low
"""

from __future__ import annotations

import os
from typing import List
from pydantic import BaseModel, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from loguru import logger


# =============================================================================
# NESTED CONFIGURATION MODELS
# Each class groups related settings together for clean dot-notation access
# =============================================================================


class KafkaSettings(BaseModel):
    """Kafka broker and topic configuration."""

    broker: str
    topic_ride_requests: str
    topic_surge_results: str
    topic_features: str
    topic_dlq: str
    group_id: str
    auto_offset_reset: str
    batch_size: int

    @field_validator("auto_offset_reset")
    @classmethod
    def validate_offset_reset(cls, v: str) -> str:
        """Ensure offset reset is a valid Kafka value."""
        valid = {"earliest", "latest", "none"}
        if v not in valid:
            raise ValueError(f"auto_offset_reset must be one of {valid}, got '{v}'")
        return v

    @field_validator("batch_size")
    @classmethod
    def validate_batch_size(cls, v: int) -> int:
        """Ensure batch size is positive and reasonable."""
        if v < 1:
            raise ValueError("batch_size must be at least 1")
        if v > 10000:
            raise ValueError("batch_size cannot exceed 10000 — too large for stable throughput")
        return v


class CassandraSettings(BaseModel):
    """Cassandra database connection configuration."""

    host: str
    port: int
    keyspace: str
    replication_factor: int

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        """Ensure port is in valid range."""
        if not 1 <= v <= 65535:
            raise ValueError(f"port must be between 1 and 65535, got {v}")
        return v

    @field_validator("replication_factor")
    @classmethod
    def validate_replication_factor(cls, v: int) -> int:
        """Replication factor must be at least 1."""
        if v < 1:
            raise ValueError("replication_factor must be at least 1")
        return v


class RedisSettings(BaseModel):
    """Redis cache connection and behavior configuration."""

    host: str
    port: int
    db: int
    feature_db: int
    ttl_seconds: int

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError(f"port must be between 1 and 65535, got {v}")
        return v

    @field_validator("ttl_seconds")
    @classmethod
    def validate_ttl(cls, v: int) -> int:
        if v < 10:
            raise ValueError("ttl_seconds must be at least 10 — too short causes cache thrashing")
        return v

    @field_validator("db", "feature_db")
    @classmethod
    def validate_db_number(cls, v: int) -> int:
        """Redis supports databases 0-15."""
        if not 0 <= v <= 15:
            raise ValueError(f"Redis db must be between 0 and 15, got {v}")
        return v


class APISettings(BaseModel):
    """FastAPI server configuration."""

    host: str
    port: int
    reload: bool

    @field_validator("port")
    @classmethod
    def validate_port(cls, v: int) -> int:
        if not 1 <= v <= 65535:
            raise ValueError(f"port must be between 1 and 65535, got {v}")
        return v


class SimulatorSettings(BaseModel):
    """Ride event simulator configuration."""

    events_per_second: int
    num_zones: int
    num_cities: int
    batch_size: int
    demand_spike_probability: float

    @field_validator("events_per_second")
    @classmethod
    def validate_events_per_second(cls, v: int) -> int:
        if v < 1:
            raise ValueError("events_per_second must be at least 1")
        if v > 100000:
            raise ValueError("events_per_second > 100000 will overwhelm a local machine")
        return v

    @field_validator("demand_spike_probability")
    @classmethod
    def validate_probability(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"demand_spike_probability must be between 0 and 1, got {v}")
        return v


class CityConfig(BaseModel):
    """Configuration for a single simulated city."""

    name: str
    latitude: float
    longitude: float
    timezone: str
    base_demand: int
    num_zones: int

    @field_validator("latitude")
    @classmethod
    def validate_latitude(cls, v: float) -> float:
        if not -90.0 <= v <= 90.0:
            raise ValueError(f"latitude must be between -90 and 90, got {v}")
        return v

    @field_validator("longitude")
    @classmethod
    def validate_longitude(cls, v: float) -> float:
        if not -180.0 <= v <= 180.0:
            raise ValueError(f"longitude must be between -180 and 180, got {v}")
        return v

    @field_validator("base_demand")
    @classmethod
    def validate_base_demand(cls, v: int) -> int:
        if v < 1:
            raise ValueError("base_demand must be at least 1")
        return v


class SurgePricingSettings(BaseModel):
    """Surge pricing thresholds and multipliers."""

    threshold_low: float
    threshold_medium: float
    threshold_high: float
    multiplier_low: float
    multiplier_medium: float
    multiplier_high: float
    multiplier_max: float

    @model_validator(mode="after")
    def validate_thresholds_ascending(self) -> "SurgePricingSettings":
        """Ensure thresholds are in ascending order."""
        if not self.threshold_low < self.threshold_medium < self.threshold_high:
            raise ValueError(
                f"Thresholds must be ascending: "
                f"low({self.threshold_low}) < "
                f"medium({self.threshold_medium}) < "
                f"high({self.threshold_high})"
            )
        return self

    @model_validator(mode="after")
    def validate_multipliers_ascending(self) -> "SurgePricingSettings":
        """Ensure multipliers are in ascending order."""
        if not self.multiplier_low < self.multiplier_medium < self.multiplier_high < self.multiplier_max:
            raise ValueError(
                "Multipliers must be ascending: "
                f"low({self.multiplier_low}) < "
                f"medium({self.multiplier_medium}) < "
                f"high({self.multiplier_high}) < "
                f"max({self.multiplier_max})"
            )
        return self


class WeatherSettings(BaseModel):
    """Open-Meteo weather API configuration."""

    update_interval_seconds: int
    cache_ttl_seconds: int

    @model_validator(mode="after")
    def validate_cache_longer_than_update(self) -> "WeatherSettings":
        """Cache TTL must exceed update interval to prevent gaps."""
        if self.cache_ttl_seconds <= self.update_interval_seconds:
            raise ValueError(
                f"cache_ttl_seconds ({self.cache_ttl_seconds}) must exceed "
                f"update_interval_seconds ({self.update_interval_seconds}) "
                "to prevent cache gaps between API calls"
            )
        return self


class EventsSettings(BaseModel):
    """Special events simulation configuration."""

    probability: float
    min_duration_minutes: int
    max_duration_minutes: int
    min_demand_multiplier: float
    max_demand_multiplier: float

    @model_validator(mode="after")
    def validate_duration_range(self) -> "EventsSettings":
        if self.min_duration_minutes >= self.max_duration_minutes:
            raise ValueError("min_duration_minutes must be less than max_duration_minutes")
        return self

    @model_validator(mode="after")
    def validate_multiplier_range(self) -> "EventsSettings":
        if self.min_demand_multiplier >= self.max_demand_multiplier:
            raise ValueError("min_demand_multiplier must be less than max_demand_multiplier")
        return self


class SupplySettings(BaseModel):
    """Driver supply simulation configuration."""

    base_drivers_per_zone: int
    rain_factor: float
    heavy_rain_factor: float
    snow_factor: float
    late_night_factor: float
    rush_hour_factor: float
    weekend_night_factor: float

    @field_validator(
        "rain_factor",
        "heavy_rain_factor",
        "snow_factor",
        "late_night_factor",
        "rush_hour_factor",
        "weekend_night_factor",
    )
    @classmethod
    def validate_factor_range(cls, v: float) -> float:
        """Supply factors must be positive and reasonable."""
        if v <= 0:
            raise ValueError("Supply factors must be positive")
        if v > 3.0:
            raise ValueError("Supply factors above 3.0 are unrealistic")
        return v


class MLSettings(BaseModel):
    """Machine learning model configuration."""

    model_path: str
    feature_window_1min: int
    feature_window_5min: int
    feature_window_15min: int
    confidence_threshold: float
    training_min_samples: int
    retrain_interval_hours: int

    @model_validator(mode="after")
    def validate_windows_ascending(self) -> "MLSettings":
        """Feature windows must be in ascending order."""
        if not (
            self.feature_window_1min
            < self.feature_window_5min
            < self.feature_window_15min
        ):
            raise ValueError(
                "Feature windows must be ascending: "
                f"1min({self.feature_window_1min}) < "
                f"5min({self.feature_window_5min}) < "
                f"15min({self.feature_window_15min})"
            )
        return self

    @field_validator("confidence_threshold")
    @classmethod
    def validate_confidence(cls, v: float) -> float:
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"confidence_threshold must be between 0 and 1, got {v}")
        return v


class HolidaySettings(BaseModel):
    """Holiday detection and demand impact configuration."""

    country: str
    demand_multiplier: float
    eve_demand_multiplier: float
    lookahead_days: int

    @field_validator("lookahead_days")
    @classmethod
    def validate_lookahead(cls, v: int) -> int:
        if not 1 <= v <= 30:
            raise ValueError("lookahead_days must be between 1 and 30")
        return v


class LoggingSettings(BaseModel):
    """Logging configuration."""

    level: str
    file: str
    rotation: str
    retention: str

    @field_validator("level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        valid = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
        if v.upper() not in valid:
            raise ValueError(f"log level must be one of {valid}, got '{v}'")
        return v.upper()


# =============================================================================
# MAIN SETTINGS CLASS
# Reads all environment variables and creates nested configuration objects
# =============================================================================


class Settings(BaseSettings):
    """
    Central settings class for the Surge Pricing Engine.

    Reads all configuration from environment variables.
    Environment variables are loaded from .env file automatically.

    All nested settings objects are created and validated at startup.
    If any required variable is missing or invalid, the application
    fails immediately with a clear error message.
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ── Kafka ────────────────────────────────────────────────────────────────
    kafka_broker: str
    kafka_topic_ride_requests: str
    kafka_topic_surge_results: str
    kafka_topic_features: str
    kafka_topic_dlq: str
    kafka_group_id: str
    kafka_auto_offset_reset: str = "earliest"
    simulator_batch_size: int = 100

    # ── Cassandra ────────────────────────────────────────────────────────────
    cassandra_host: str
    cassandra_port: int = 9042
    cassandra_keyspace: str
    cassandra_replication_factor: int = 1

    # ── Redis ────────────────────────────────────────────────────────────────
    redis_host: str
    redis_port: int = 6379
    redis_db: int = 0
    redis_feature_db: int = 1
    redis_ttl_seconds: int = 120

    # ── API ──────────────────────────────────────────────────────────────────
    api_host: str = "0.0.0.0"
    api_port: int = 8000
    api_reload: bool = True

    # ── Simulator ────────────────────────────────────────────────────────────
    simulator_events_per_second: int = 1000
    simulator_num_zones: int = 50
    simulator_num_cities: int = 3
    simulator_demand_spike_probability: float = 0.05

    # ── Cities ───────────────────────────────────────────────────────────────
    nyc_name: str = "new_york"
    nyc_latitude: float = 40.7128
    nyc_longitude: float = -74.0060
    nyc_timezone: str = "America/New_York"
    nyc_base_demand: int = 100
    nyc_num_zones: int = 20

    sf_name: str = "san_francisco"
    sf_latitude: float = 37.7749
    sf_longitude: float = -122.4194
    sf_timezone: str = "America/Los_Angeles"
    sf_base_demand: int = 70
    sf_num_zones: int = 15

    chicago_name: str = "chicago"
    chicago_latitude: float = 41.8781
    chicago_longitude: float = -87.6298
    chicago_timezone: str = "America/Chicago"
    chicago_base_demand: int = 80
    chicago_num_zones: int = 15

    # ── Surge Pricing ────────────────────────────────────────────────────────
    surge_threshold_low: float = 1.5
    surge_threshold_medium: float = 2.0
    surge_threshold_high: float = 3.0
    surge_multiplier_low: float = 1.2
    surge_multiplier_medium: float = 1.5
    surge_multiplier_high: float = 2.0
    surge_multiplier_max: float = 3.0

    # ── Weather ──────────────────────────────────────────────────────────────
    weather_update_interval_seconds: int = 600
    weather_cache_ttl_seconds: int = 660

    # ── Events ───────────────────────────────────────────────────────────────
    events_probability: float = 0.02
    events_min_duration_minutes: int = 60
    events_max_duration_minutes: int = 240
    events_min_demand_multiplier: float = 2.0
    events_max_demand_multiplier: float = 4.0

    # ── Supply ───────────────────────────────────────────────────────────────
    supply_base_drivers_per_zone: int = 30
    supply_rain_factor: float = 0.70
    supply_heavy_rain_factor: float = 0.50
    supply_snow_factor: float = 0.60
    supply_late_night_factor: float = 0.50
    supply_rush_hour_factor: float = 1.30
    supply_weekend_night_factor: float = 0.65

    # ── ML ───────────────────────────────────────────────────────────────────
    ml_model_path: str = "ml/models/surge_predictor.pkl"
    ml_feature_window_1min: int = 60
    ml_feature_window_5min: int = 300
    ml_feature_window_15min: int = 900
    ml_confidence_threshold: float = 0.7
    ml_training_min_samples: int = 1000
    ml_retrain_interval_hours: int = 24

    # ── Holidays ─────────────────────────────────────────────────────────────
    holiday_country: str = "US"
    holiday_demand_multiplier: float = 1.8
    holiday_eve_demand_multiplier: float = 1.4
    holiday_lookahead_days: int = 7

    # ── Logging ──────────────────────────────────────────────────────────────
    log_level: str = "INFO"
    log_file: str = "logs/pipeline.log"
    log_rotation: str = "100 MB"
    log_retention: str = "7 days"

    # =========================================================================
    # COMPUTED PROPERTIES
    # These build nested config objects from the flat environment variables
    # =========================================================================

    @property
    def kafka(self) -> KafkaSettings:
        """Returns validated Kafka configuration object."""
        return KafkaSettings(
            broker=self.kafka_broker,
            topic_ride_requests=self.kafka_topic_ride_requests,
            topic_surge_results=self.kafka_topic_surge_results,
            topic_features=self.kafka_topic_features,
            topic_dlq=self.kafka_topic_dlq,
            group_id=self.kafka_group_id,
            auto_offset_reset=self.kafka_auto_offset_reset,
            batch_size=self.simulator_batch_size,
        )

    @property
    def cassandra(self) -> CassandraSettings:
        """Returns validated Cassandra configuration object."""
        return CassandraSettings(
            host=self.cassandra_host,
            port=self.cassandra_port,
            keyspace=self.cassandra_keyspace,
            replication_factor=self.cassandra_replication_factor,
        )

    @property
    def redis(self) -> RedisSettings:
        """Returns validated Redis configuration object."""
        return RedisSettings(
            host=self.redis_host,
            port=self.redis_port,
            db=self.redis_db,
            feature_db=self.redis_feature_db,
            ttl_seconds=self.redis_ttl_seconds,
        )

    @property
    def api(self) -> APISettings:
        """Returns validated API configuration object."""
        return APISettings(
            host=self.api_host,
            port=self.api_port,
            reload=self.api_reload,
        )

    @property
    def simulator(self) -> SimulatorSettings:
        """Returns validated simulator configuration object."""
        return SimulatorSettings(
            events_per_second=self.simulator_events_per_second,
            num_zones=self.simulator_num_zones,
            num_cities=self.simulator_num_cities,
            batch_size=self.simulator_batch_size,
            demand_spike_probability=self.simulator_demand_spike_probability,
        )

    @property
    def cities(self) -> List[CityConfig]:
        """Returns list of all city configurations."""
        return [
            CityConfig(
                name=self.nyc_name,
                latitude=self.nyc_latitude,
                longitude=self.nyc_longitude,
                timezone=self.nyc_timezone,
                base_demand=self.nyc_base_demand,
                num_zones=self.nyc_num_zones,
            ),
            CityConfig(
                name=self.sf_name,
                latitude=self.sf_latitude,
                longitude=self.sf_longitude,
                timezone=self.sf_timezone,
                base_demand=self.sf_base_demand,
                num_zones=self.sf_num_zones,
            ),
            CityConfig(
                name=self.chicago_name,
                latitude=self.chicago_latitude,
                longitude=self.chicago_longitude,
                timezone=self.chicago_timezone,
                base_demand=self.chicago_base_demand,
                num_zones=self.chicago_num_zones,
            ),
        ]

    @property
    def surge(self) -> SurgePricingSettings:
        """Returns validated surge pricing configuration object."""
        return SurgePricingSettings(
            threshold_low=self.surge_threshold_low,
            threshold_medium=self.surge_threshold_medium,
            threshold_high=self.surge_threshold_high,
            multiplier_low=self.surge_multiplier_low,
            multiplier_medium=self.surge_multiplier_medium,
            multiplier_high=self.surge_multiplier_high,
            multiplier_max=self.surge_multiplier_max,
        )

    @property
    def weather(self) -> WeatherSettings:
        """Returns validated weather configuration object."""
        return WeatherSettings(
            update_interval_seconds=self.weather_update_interval_seconds,
            cache_ttl_seconds=self.weather_cache_ttl_seconds,
        )

    @property
    def events(self) -> EventsSettings:
        """Returns validated events configuration object."""
        return EventsSettings(
            probability=self.events_probability,
            min_duration_minutes=self.events_min_duration_minutes,
            max_duration_minutes=self.events_max_duration_minutes,
            min_demand_multiplier=self.events_min_demand_multiplier,
            max_demand_multiplier=self.events_max_demand_multiplier,
        )

    @property
    def supply(self) -> SupplySettings:
        """Returns validated supply configuration object."""
        return SupplySettings(
            base_drivers_per_zone=self.supply_base_drivers_per_zone,
            rain_factor=self.supply_rain_factor,
            heavy_rain_factor=self.supply_heavy_rain_factor,
            snow_factor=self.supply_snow_factor,
            late_night_factor=self.supply_late_night_factor,
            rush_hour_factor=self.supply_rush_hour_factor,
            weekend_night_factor=self.supply_weekend_night_factor,
        )

    @property
    def ml(self) -> MLSettings:
        """Returns validated ML configuration object."""
        return MLSettings(
            model_path=self.ml_model_path,
            feature_window_1min=self.ml_feature_window_1min,
            feature_window_5min=self.ml_feature_window_5min,
            feature_window_15min=self.ml_feature_window_15min,
            confidence_threshold=self.ml_confidence_threshold,
            training_min_samples=self.ml_training_min_samples,
            retrain_interval_hours=self.ml_retrain_interval_hours,
        )

    @property
    def holidays(self) -> HolidaySettings:
        """Returns validated holiday configuration object."""
        return HolidaySettings(
            country=self.holiday_country,
            demand_multiplier=self.holiday_demand_multiplier,
            eve_demand_multiplier=self.holiday_eve_demand_multiplier,
            lookahead_days=self.holiday_lookahead_days,
        )

    @property
    def logging(self) -> LoggingSettings:
        """Returns validated logging configuration object."""
        return LoggingSettings(
            level=self.log_level,
            file=self.log_file,
            rotation=self.log_rotation,
            retention=self.log_retention,
        )


# =============================================================================
# SINGLETON INSTANCE
# Created once at module import time.
# All other modules import this instance directly.
# =============================================================================

settings = Settings()

logger.info("Configuration loaded successfully")
logger.info(f"Kafka broker: {settings.kafka.broker}")
logger.info(f"Cassandra: {settings.cassandra.host}:{settings.cassandra.port}")
logger.info(f"Redis: {settings.redis.host}:{settings.redis.port}")
logger.info(f"Cities: {[c.name for c in settings.cities]}")
logger.info(f"ML model path: {settings.ml.model_path}")