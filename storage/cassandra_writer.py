"""
storage/cassandra_writer.py
============================
Handles all writes to Cassandra for surge pricing results.

Manages connection, retries, and schema initialization.
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from cassandra.cluster import Cluster, Session
from cassandra.auth import PlainTextAuthProvider
from cassandra.policies import DCAwareRoundRobinPolicy
from loguru import logger

from config.settings import settings


# =============================================================================
# CONNECTION
# =============================================================================

class CassandraClient:
    """
    Manages Cassandra connection and write operations.

    Handles connection retries, keyspace initialization,
    and prepared statements for efficient writes.
    """

    def __init__(self) -> None:
        self.cluster: Cluster | None = None
        self.session: Session | None = None
        self._connect()
        self._initialize_schema()

    def _connect(self, max_retries: int = 10) -> None:
        """
        Connect to Cassandra with exponential backoff retry.

        Args:
            max_retries: Maximum connection attempts
        """
        for attempt in range(1, max_retries + 1):
            try:
                self.cluster = Cluster(
                    contact_points=[settings.cassandra.host],
                    port=settings.cassandra.port,
                    load_balancing_policy=DCAwareRoundRobinPolicy(
                        local_dc="datacenter1"
                    ),
                    protocol_version=5,
                )
                self.session = self.cluster.connect()
                logger.info(
                    f"Connected to Cassandra at "
                    f"{settings.cassandra.host}:{settings.cassandra.port}"
                )
                return

            except Exception as e:
                wait = min(2 ** attempt, 60)
                logger.warning(
                    f"Cassandra connection failed (attempt {attempt}/{max_retries}): "
                    f"{e}. Retrying in {wait}s..."
                )
                time.sleep(wait)

        raise ConnectionError(
            f"Could not connect to Cassandra after {max_retries} attempts"
        )

    def _initialize_schema(self) -> None:
        """
        Create keyspace and tables if they do not exist.

        Reads schema from storage/schema.cql and executes it.
        """
        try:
            # Create keyspace
            self.session.execute(f"""
                CREATE KEYSPACE IF NOT EXISTS {settings.cassandra.keyspace}
                WITH replication = {{
                    'class': 'SimpleStrategy',
                    'replication_factor': {settings.cassandra.replication_factor}
                }}
            """)

            self.session.set_keyspace(settings.cassandra.keyspace)

            # Create surge_results table
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS surge_results (
                    zone_id          TEXT,
                    city             TEXT,
                    timestamp        TIMESTAMP,
                    demand_1min      INT,
                    demand_5min      INT,
                    demand_15min     INT,
                    available_drivers INT,
                    supply_ratio     DOUBLE,
                    rule_multiplier  DOUBLE,
                    ml_multiplier    DOUBLE,
                    final_multiplier DOUBLE,
                    ml_confidence    DOUBLE,
                    is_special_event BOOLEAN,
                    weather_severity INT,
                    PRIMARY KEY (zone_id, timestamp)
                ) WITH CLUSTERING ORDER BY (timestamp DESC)
                  AND default_time_to_live = 604800
            """)

            # Create current_surge table
            self.session.execute("""
                CREATE TABLE IF NOT EXISTS current_surge (
                    zone_id          TEXT PRIMARY KEY,
                    city             TEXT,
                    final_multiplier DOUBLE,
                    rule_multiplier  DOUBLE,
                    ml_multiplier    DOUBLE,
                    ml_confidence    DOUBLE,
                    updated_at       TIMESTAMP
                )
            """)

            logger.info("Cassandra schema initialized successfully")

        except Exception as e:
            logger.error(f"Schema initialization failed: {e}")
            raise

    def write_surge_result(self, result: dict[str, Any]) -> bool:
        """
        Write a single surge result to Cassandra.

        Args:
            result: Dictionary with surge pricing data

        Returns:
            True if successful, False otherwise
        """
        try:
            self.session.execute("""
                INSERT INTO surge_results (
                    zone_id, city, timestamp,
                    demand_1min, demand_5min, demand_15min,
                    available_drivers, supply_ratio,
                    rule_multiplier, ml_multiplier,
                    final_multiplier, ml_confidence,
                    is_special_event, weather_severity
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s
                )
            """, (
                result["zone_id"],
                result["city"],
                result.get("timestamp", datetime.now()),
                result.get("demand_1min", 0),
                result.get("demand_5min", 0),
                result.get("demand_15min", 0),
                result.get("available_drivers", 0),
                result.get("supply_ratio", 0.0),
                result.get("rule_multiplier", 1.0),
                result.get("ml_multiplier", 1.0),
                result.get("final_multiplier", 1.0),
                result.get("ml_confidence", 0.0),
                result.get("is_special_event", False),
                result.get("weather_severity", 1),
            ))

            # Update current surge table
            self.session.execute("""
                INSERT INTO current_surge (
                    zone_id, city, final_multiplier,
                    rule_multiplier, ml_multiplier,
                    ml_confidence, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                result["zone_id"],
                result["city"],
                result.get("final_multiplier", 1.0),
                result.get("rule_multiplier", 1.0),
                result.get("ml_multiplier", 1.0),
                result.get("ml_confidence", 0.0),
                datetime.now(),
            ))

            return True

        except Exception as e:
            logger.error(f"Cassandra write failed for zone {result.get('zone_id')}: {e}")
            return False

    def get_historical_avg(self, zone_id: str, hour: int, day_of_week: int) -> dict[str, float]:
        """
        Query historical average demand and surge for a zone.

        Used by ML feature engineering to compute demand_spike_factor.

        Args:
            zone_id: Zone identifier
            hour: Hour of day (0-23)
            day_of_week: Day of week (0-6)

        Returns:
            Dictionary with avg_demand and avg_surge
        """
        try:
            rows = self.session.execute("""
                SELECT demand_1min, final_multiplier
                FROM surge_results
                WHERE zone_id = %s
                LIMIT 1000
            """, (zone_id,))

            demands = [r.demand_1min for r in rows if r.demand_1min]
            surges = [r.final_multiplier for r in rows if r.final_multiplier]

            return {
                "avg_demand": sum(demands) / len(demands) if demands else 20.0,
                "avg_surge": sum(surges) / len(surges) if surges else 1.0,
            }

        except Exception as e:
            logger.warning(f"Historical query failed for {zone_id}: {e}")
            return {"avg_demand": 20.0, "avg_surge": 1.0}

    def close(self) -> None:
        """Close Cassandra connection."""
        if self.cluster:
            self.cluster.shutdown()
            logger.info("Cassandra connection closed")