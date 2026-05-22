"""
tests/test_kafka_producer.py
=============================
Unit tests for Kafka producer logic.

Uses mocking so tests run without a real Kafka broker.
"""

from __future__ import annotations

import pytest
from unittest.mock import MagicMock, patch


class TestEventValidation:
    """Tests for event validation logic."""

    def test_valid_event_passes(self, sample_ride_event):
        """Valid event with all required fields should pass."""
        with patch("simulator.kafka_producer.KafkaProducer"):
            from simulator.kafka_producer import RideEventProducer
            with patch.object(RideEventProducer, "_connect"):
                producer = RideEventProducer.__new__(RideEventProducer)
                producer.producer = MagicMock()
                producer.topic = "ride-requests"
                producer.dlq_topic = "ride-requests-dlq"
                assert producer._validate_event(sample_ride_event) is True

    def test_missing_zone_id_fails(self, sample_ride_event):
        """Event missing zone_id should fail validation."""
        del sample_ride_event["zone_id"]
        with patch("simulator.kafka_producer.KafkaProducer"):
            from simulator.kafka_producer import RideEventProducer
            with patch.object(RideEventProducer, "_connect"):
                producer = RideEventProducer.__new__(RideEventProducer)
                producer.producer = MagicMock()
                producer.topic = "ride-requests"
                producer.dlq_topic = "ride-requests-dlq"
                assert producer._validate_event(sample_ride_event) is False

    def test_missing_timestamp_fails(self, sample_ride_event):
        """Event missing timestamp_ms should fail validation."""
        del sample_ride_event["timestamp_ms"]
        with patch("simulator.kafka_producer.KafkaProducer"):
            from simulator.kafka_producer import RideEventProducer
            with patch.object(RideEventProducer, "_connect"):
                producer = RideEventProducer.__new__(RideEventProducer)
                producer.producer = MagicMock()
                producer.topic = "ride-requests"
                producer.dlq_topic = "ride-requests-dlq"
                assert producer._validate_event(sample_ride_event) is False


class TestSerializeEvent:
    """Tests for event serialization."""

    def test_serializes_to_bytes(self, sample_ride_event):
        """Event should serialize to bytes."""
        from simulator.kafka_producer import serialize_event
        result = serialize_event(sample_ride_event)
        assert isinstance(result, bytes)

    def test_serialized_contains_zone_id(self, sample_ride_event):
        """Serialized bytes should contain zone_id value."""
        from simulator.kafka_producer import serialize_event
        result = serialize_event(sample_ride_event)
        assert b"dr5ru6" in result