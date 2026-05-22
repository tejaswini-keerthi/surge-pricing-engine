"""
streaming/watermark_config.py
==============================
Windowing and watermark configuration for Spark Structured Streaming.

Centralizes all window definitions so they can be tuned
without touching the main streaming job logic.
"""

from __future__ import annotations

from dataclasses import dataclass

from config.settings import settings


@dataclass
class WindowConfig:
    """
    Configuration for a single Spark streaming window.

    Attributes:
        duration_seconds: How long the window covers
        slide_seconds: How often the window updates
        watermark_seconds: How long to wait for late events
        name: Human-readable name for logging
    """
    duration_seconds: int
    slide_seconds: int
    watermark_seconds: int
    name: str

    @property
    def duration_str(self) -> str:
        """Spark-compatible duration string e.g. '60 seconds'"""
        return f"{self.duration_seconds} seconds"

    @property
    def slide_str(self) -> str:
        """Spark-compatible slide string e.g. '10 seconds'"""
        return f"{self.slide_seconds} seconds"

    @property
    def watermark_str(self) -> str:
        """Spark-compatible watermark string e.g. '120 seconds'"""
        return f"{self.watermark_seconds} seconds"


# =============================================================================
# WINDOW DEFINITIONS
# Three windows capture demand at different time scales
# =============================================================================

WINDOW_1MIN = WindowConfig(
    duration_seconds=settings.ml.feature_window_1min,
    slide_seconds=10,
    watermark_seconds=120,
    name="1min_demand",
)

WINDOW_5MIN = WindowConfig(
    duration_seconds=settings.ml.feature_window_5min,
    slide_seconds=10,
    watermark_seconds=120,
    name="5min_demand",
)

WINDOW_15MIN = WindowConfig(
    duration_seconds=settings.ml.feature_window_15min,
    slide_seconds=10,
    watermark_seconds=120,
    name="15min_demand",
)

ALL_WINDOWS = [WINDOW_1MIN, WINDOW_5MIN, WINDOW_15MIN]


@dataclass
class StreamConfig:
    """
    General Spark Structured Streaming configuration.

    Attributes:
        trigger_interval_seconds: How often Spark processes a micro-batch
        checkpoint_location: Where Spark saves state for fault tolerance
        max_offsets_per_trigger: Max Kafka messages per micro-batch
        output_mode: Spark output mode (update/complete/append)
    """
    trigger_interval_seconds: int = 1
    checkpoint_location: str = "checkpoint/surge_streaming"
    max_offsets_per_trigger: int = 10000
    output_mode: str = "update"

    @property
    def trigger_str(self) -> str:
        """Spark-compatible trigger string."""
        return f"{self.trigger_interval_seconds} seconds"


STREAM_CONFIG = StreamConfig()