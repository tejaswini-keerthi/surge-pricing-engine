"""
simulator/kafka_producer.py
===========================
Publishes ride request events to Kafka topics.

Handles:
- Kafka connection with retry logic
- JSON serialization of events
- Dead letter queue for malformed events
- Batched publishing for efficiency
"""

from __future__ import annotations

import json
import time
from typing import Any

from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable
from loguru import logger

from config.settings import settings


# =============================================================================
# SERIALIZER
# =============================================================================

def serialize_event(event: dict[str, Any]) -> bytes:
    """
    Serialize event dictionary to JSON bytes.

    Args:
        event: Event dictionary

    Returns:
        JSON encoded bytes
    """
    return json.dumps(event, default=str).encode("utf-8")


# =============================================================================
# PRODUCER
# =============================================================================

class RideEventProducer:
    """
    Kafka producer for ride request events.

    Publishes events to the ride-requests topic.
    Routes malformed events to the dead letter queue.
    Retries connection on failure with exponential backoff.
    """

    def __init__(self) -> None:
        self.producer: KafkaProducer | None = None
        self.topic = settings.kafka.topic_ride_requests
        self.dlq_topic = settings.kafka.topic_dlq
        self._connect()

    def _connect(self, max_retries: int = 5) -> None:
        """
        Connect to Kafka with exponential backoff retry.

        Args:
            max_retries: Maximum connection attempts
        """
        for attempt in range(1, max_retries + 1):
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=settings.kafka.broker,
                    value_serializer=serialize_event,
                    acks="all",
                    retries=3,
                    batch_size=settings.kafka.batch_size * 1024,
                    linger_ms=10,
                    compression_type="gzip",
                )
                logger.info(
                    f"Connected to Kafka at {settings.kafka.broker}"
                )
                return

            except NoBrokersAvailable:
                wait = 2 ** attempt
                logger.warning(
                    f"Kafka not available (attempt {attempt}/{max_retries}). "
                    f"Retrying in {wait}s..."
                )
                time.sleep(wait)

        raise ConnectionError(
            f"Could not connect to Kafka at {settings.kafka.broker} "
            f"after {max_retries} attempts"
        )

    def _validate_event(self, event: dict[str, Any]) -> bool:
        """
        Validate that an event has all required fields.

        Args:
            event: Event dictionary to validate

        Returns:
            True if valid, False otherwise
        """
        required_fields = [
            "event_id",
            "zone_id",
            "city",
            "latitude",
            "longitude",
            "timestamp_ms",
            "available_drivers",
        ]
        return all(field in event for field in required_fields)

    def _send_to_dlq(self, event: dict[str, Any], reason: str) -> None:
        """
        Send malformed event to dead letter queue.

        Args:
            event: The problematic event
            reason: Why it was rejected
        """
        dlq_event = {
            "original_event": event,
            "failure_reason": reason,
            "failed_at_ms": int(time.time() * 1000),
        }
        try:
            self.producer.send(self.dlq_topic, value=dlq_event)
            logger.warning(f"Event sent to DLQ: {reason} | event_id={event.get('event_id', 'unknown')}")
        except Exception as e:
            logger.error(f"Failed to send to DLQ: {e}")

    def publish_batch(self, events: list[dict[str, Any]]) -> tuple[int, int]:
        """
        Publish a batch of events to Kafka.

        Valid events go to ride-requests topic.
        Invalid events go to dead letter queue.

        Args:
            events: List of event dictionaries

        Returns:
            Tuple of (successful_count, failed_count)
        """
        if not self.producer:
            logger.error("Producer not initialized")
            return 0, len(events)

        successful = 0
        failed = 0

        for event in events:
            try:
                # Validate event
                if not self._validate_event(event):
                    self._send_to_dlq(event, "missing required fields")
                    failed += 1
                    continue

                # Publish to Kafka
                self.producer.send(
                    self.topic,
                    value=event,
                    key=event["zone_id"].encode("utf-8"),
                )
                successful += 1

            except KafkaError as e:
                logger.error(f"Kafka error publishing event: {e}")
                self._send_to_dlq(event, f"kafka_error: {str(e)}")
                failed += 1

            except Exception as e:
                logger.error(f"Unexpected error: {e}")
                self._send_to_dlq(event, f"unexpected_error: {str(e)}")
                failed += 1

        # Flush to ensure delivery
        self.producer.flush()

        if failed > 0:
            logger.warning(f"Batch published: {successful} success, {failed} failed")
        else:
            logger.debug(f"Batch published: {successful} events")

        return successful, failed

    def close(self) -> None:
        """Flush and close the Kafka producer."""
        if self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer closed")


# =============================================================================
# ENTRY POINT
# =============================================================================

def run_simulator() -> None:
    """
    Main entry point — runs the simulator and producer together.

    Continuously generates ride events and publishes to Kafka.
    """
    from simulator.ride_simulator import RideSimulator

    logger.info("Starting Ride Event Simulator")

    simulator = RideSimulator()
    producer = RideEventProducer()

    total_published = 0
    total_failed = 0

    try:
        for batch in simulator.generate_events():
            successful, failed = producer.publish_batch(batch)
            total_published += successful
            total_failed += failed

            if total_published % 10000 == 0:
                logger.info(
                    f"Total published: {total_published:,} | "
                    f"Failed: {total_failed:,}"
                )

    except KeyboardInterrupt:
        logger.info("Simulator stopped by user")
    finally:
        producer.close()
        logger.info(
            f"Final stats — Published: {total_published:,} | "
            f"Failed: {total_failed:,}"
        )


if __name__ == "__main__":
    run_simulator()