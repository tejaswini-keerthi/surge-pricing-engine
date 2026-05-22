"""
storage/redis_cache.py
=======================
Manages Redis cache for surge prices and ML features.

Two separate Redis databases:
- DB 0: Surge price cache (read by FastAPI)
- DB 1: ML feature store (read by ML training)
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import redis
from loguru import logger

from config.settings import settings


# =============================================================================
# SURGE CACHE — DB 0
# =============================================================================

class SurgeCache:
    """
    Redis cache for latest surge multiplier per zone.

    Provides sub-5ms reads for the FastAPI serving layer.
    Data expires after TTL seconds to prevent stale prices.
    """

    def __init__(self) -> None:
        self.client = redis.Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            decode_responses=True,
        )
        logger.info(
            f"SurgeCache connected to Redis "
            f"{settings.redis.host}:{settings.redis.port} db={settings.redis.db}"
        )

    def set_surge(self, zone_id: str, surge_data: dict[str, Any]) -> bool:
        """
        Cache surge data for a zone.

        Args:
            zone_id: Zone identifier
            surge_data: Dictionary with multiplier and metadata

        Returns:
            True if successful
        """
        try:
            key = f"surge:{zone_id}"
            surge_data["cached_at"] = datetime.now().isoformat()
            self.client.setex(
                key,
                settings.redis.ttl_seconds,
                json.dumps(surge_data),
            )
            return True
        except Exception as e:
            logger.error(f"Redis set failed for zone {zone_id}: {e}")
            return False

    def get_surge(self, zone_id: str) -> dict[str, Any] | None:
        """
        Get cached surge data for a zone.

        Args:
            zone_id: Zone identifier

        Returns:
            Surge dictionary or None if expired/missing
        """
        try:
            key = f"surge:{zone_id}"
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Redis get failed for zone {zone_id}: {e}")
            return None

    def get_all_surges(self) -> dict[str, dict[str, Any]]:
        """
        Get cached surge data for all zones.

        Used by Streamlit dashboard to render the full surge map.

        Returns:
            Dictionary mapping zone_id to surge data
        """
        try:
            keys = self.client.keys("surge:*")
            result = {}
            if keys:
                pipeline = self.client.pipeline()
                for key in keys:
                    pipeline.get(key)
                values = pipeline.execute()

                for key, value in zip(keys, values):
                    if value:
                        zone_id = key.replace("surge:", "")
                        result[zone_id] = json.loads(value)

            return result
        except Exception as e:
            logger.error(f"Redis get_all failed: {e}")
            return {}

    def set_batch(self, surge_results: list[dict[str, Any]]) -> int:
        """
        Cache multiple surge results in one pipeline operation.

        Args:
            surge_results: List of surge result dictionaries

        Returns:
            Number of successfully cached results
        """
        try:
            pipeline = self.client.pipeline()
            for result in surge_results:
                key = f"surge:{result['zone_id']}"
                result["cached_at"] = datetime.now().isoformat()
                pipeline.setex(
                    key,
                    settings.redis.ttl_seconds,
                    json.dumps(result),
                )
            pipeline.execute()
            return len(surge_results)
        except Exception as e:
            logger.error(f"Redis batch set failed: {e}")
            return 0


# =============================================================================
# FEATURE STORE — DB 1
# =============================================================================

class FeatureStore:
    """
    Redis feature store for ML model inputs.

    Stores the latest 21 features per zone for model serving.
    Uses a separate Redis database from surge cache for isolation.
    """

    def __init__(self) -> None:
        self.client = redis.Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.feature_db,
            decode_responses=True,
        )
        logger.info(
            f"FeatureStore connected to Redis "
            f"{settings.redis.host}:{settings.redis.port} "
            f"db={settings.redis.feature_db}"
        )

    def set_features(self, zone_id: str, features: dict[str, Any]) -> bool:
        """
        Store ML features for a zone.

        Args:
            zone_id: Zone identifier
            features: Dictionary of 21 ML features

        Returns:
            True if successful
        """
        try:
            key = f"features:{zone_id}"
            features["stored_at"] = datetime.now().isoformat()
            self.client.setex(
                key,
                settings.redis.ttl_seconds * 2,
                json.dumps(features),
            )
            return True
        except Exception as e:
            logger.error(f"Feature store set failed for {zone_id}: {e}")
            return False

    def get_features(self, zone_id: str) -> dict[str, Any] | None:
        """
        Get ML features for a zone.

        Args:
            zone_id: Zone identifier

        Returns:
            Features dictionary or None if missing
        """
        try:
            key = f"features:{zone_id}"
            data = self.client.get(key)
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Feature store get failed for {zone_id}: {e}")
            return None

    def get_all_features(self) -> dict[str, dict[str, Any]]:
        """
        Get features for all zones.

        Used by ML training script to build training dataset.

        Returns:
            Dictionary mapping zone_id to features
        """
        try:
            keys = self.client.keys("features:*")
            result = {}
            if keys:
                pipeline = self.client.pipeline()
                for key in keys:
                    pipeline.get(key)
                values = pipeline.execute()

                for key, value in zip(keys, values):
                    if value:
                        zone_id = key.replace("features:", "")
                        result[zone_id] = json.loads(value)

            return result
        except Exception as e:
            logger.error(f"Feature store get_all failed: {e}")
            return {}