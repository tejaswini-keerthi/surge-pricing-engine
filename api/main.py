"""
api/main.py
===========
FastAPI application serving surge pricing data.

Endpoints:
    GET  /health                    - Service health check
    GET  /surge/{zone_id}           - Current surge for one zone
    GET  /surge/all                 - Current surge for all zones
    POST /surge/batch               - Current surge for multiple zones
    GET  /surge/{zone_id}/history   - Historical surge for a zone
    GET  /zones                     - List all active zones
"""

from __future__ import annotations

import time
from datetime import datetime
from typing import Any

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from loguru import logger

from config.settings import settings
from api.models import (
    SurgeResponse,
    AllSurgesResponse,
    HealthResponse,
    ZoneListResponse,
    SurgeHistoryResponse,
    ZoneSurgeRequest,
)
from storage.redis_cache import SurgeCache
from storage.cassandra_writer import CassandraClient
from ml.serve_model import surge_predictor
from streaming.surge_calculator import get_surge_tier


# =============================================================================
# APP INITIALIZATION
# =============================================================================

app = FastAPI(
    title="Surge Pricing Engine API",
    description="Real-time ride surge pricing powered by Kafka, Spark, and XGBoost",
    version="1.0.0",
    docs_url="/docs",
    redoc_url="/redoc",
)

# Allow Streamlit dashboard to call this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Service start time for uptime calculation
START_TIME = time.time()

# Initialize storage clients
surge_cache = SurgeCache()
cassandra_client: CassandraClient | None = None


def get_cassandra() -> CassandraClient:
    """Lazy initialize Cassandra client."""
    global cassandra_client
    if cassandra_client is None:
        cassandra_client = CassandraClient()
    return cassandra_client


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def build_surge_response(
    zone_id: str,
    cached_data: dict[str, Any],
) -> SurgeResponse:
    """
    Build SurgeResponse from cached Redis data.

    Args:
        zone_id: Zone identifier
        cached_data: Data from Redis cache

    Returns:
        SurgeResponse object
    """
    final_multiplier = cached_data.get("final_multiplier", 1.0)

    return SurgeResponse(
        zone_id=zone_id,
        city=cached_data.get("city", "unknown"),
        final_multiplier=final_multiplier,
        rule_multiplier=cached_data.get("rule_multiplier", 1.0),
        ml_multiplier=cached_data.get("ml_multiplier", 1.0),
        ml_confidence=cached_data.get("ml_confidence", 0.0),
        ml_available=surge_predictor.is_loaded,
        surge_tier=get_surge_tier(final_multiplier),
        updated_at=datetime.fromisoformat(
            cached_data.get("updated_at", datetime.now().isoformat())
        ),
    )


# =============================================================================
# ENDPOINTS
# =============================================================================

@app.get("/health", response_model=HealthResponse)
async def health_check() -> HealthResponse:
    """
    Check health of all connected services.

    Returns status of Kafka, Cassandra, Redis, and ML model.
    """
    redis_ok = False
    cassandra_ok = False

    try:
        surge_cache.client.ping()
        redis_ok = True
    except Exception:
        pass

    try:
        get_cassandra()
        cassandra_ok = True
    except Exception:
        pass

    all_healthy = redis_ok and cassandra_ok
    status = "healthy" if all_healthy else "degraded"

    return HealthResponse(
        status=status,
        kafka_connected=True,
        cassandra_connected=cassandra_ok,
        redis_connected=redis_ok,
        ml_model_loaded=surge_predictor.is_loaded,
        uptime_seconds=round(time.time() - START_TIME, 2),
        timestamp=datetime.now(),
    )


@app.get("/surge/all", response_model=AllSurgesResponse)
async def get_all_surges() -> AllSurgesResponse:
    """
    Get current surge multipliers for all active zones.

    Used by Streamlit dashboard to render the full surge map.
    """
    all_data = surge_cache.get_all_surges()

    if not all_data:
        return AllSurgesResponse(
            zones={},
            total_zones=0,
            surging_zones=0,
            timestamp=datetime.now(),
        )

    zones = {}
    surging_count = 0

    for zone_id, data in all_data.items():
        response = build_surge_response(zone_id, data)
        zones[zone_id] = response
        if response.final_multiplier > 1.0:
            surging_count += 1

    return AllSurgesResponse(
        zones=zones,
        total_zones=len(zones),
        surging_zones=surging_count,
        timestamp=datetime.now(),
    )


@app.get("/surge/{zone_id}", response_model=SurgeResponse)
async def get_surge(zone_id: str) -> SurgeResponse:
    """
    Get current surge multiplier for a specific zone.

    Reads from Redis cache for sub-10ms response time.
    Returns 1.0x (no surge) if zone data is not cached.

    Args:
        zone_id: Geohash zone identifier
    """
    cached_data = surge_cache.get_surge(zone_id)

    if not cached_data:
        logger.warning(f"No surge data cached for zone {zone_id}")
        return SurgeResponse(
            zone_id=zone_id,
            city="unknown",
            final_multiplier=1.0,
            rule_multiplier=1.0,
            ml_multiplier=1.0,
            ml_confidence=0.0,
            ml_available=surge_predictor.is_loaded,
            surge_tier="normal",
            updated_at=datetime.now(),
        )

    return build_surge_response(zone_id, cached_data)


@app.post("/surge/batch", response_model=dict[str, SurgeResponse])
async def get_surge_batch(request: ZoneSurgeRequest) -> dict[str, SurgeResponse]:
    """
    Get surge multipliers for multiple zones in one request.

    Args:
        request: ZoneSurgeRequest with list of zone_ids
    """
    result = {}
    for zone_id in request.zone_ids:
        cached_data = surge_cache.get_surge(zone_id)
        if cached_data:
            result[zone_id] = build_surge_response(zone_id, cached_data)

    return result


@app.get("/surge/{zone_id}/history", response_model=SurgeHistoryResponse)
async def get_surge_history(
    zone_id: str,
    limit: int = 100,
) -> SurgeHistoryResponse:
    """
    Get historical surge data for a zone from Cassandra.

    Args:
        zone_id: Geohash zone identifier
        limit: Maximum number of historical records to return
    """
    try:
        cassandra = get_cassandra()
        rows = cassandra.session.execute("""
            SELECT zone_id, city, timestamp, final_multiplier,
                   rule_multiplier, ml_multiplier, demand_1min
            FROM surge_results
            WHERE zone_id = %s
            LIMIT %s
        """, (zone_id, limit))

        history = [
            {
                "timestamp": row.timestamp.isoformat() if row.timestamp else None,
                "final_multiplier": row.final_multiplier,
                "rule_multiplier": row.rule_multiplier,
                "ml_multiplier": row.ml_multiplier,
                "demand_1min": row.demand_1min,
            }
            for row in rows
        ]

        return SurgeHistoryResponse(
            zone_id=zone_id,
            city=history[0]["city"] if history else "unknown",
            history=history,
            count=len(history),
        )

    except Exception as e:
        logger.error(f"History query failed for {zone_id}: {e}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to retrieve history for zone {zone_id}",
        )


@app.get("/zones", response_model=ZoneListResponse)
async def get_zones() -> ZoneListResponse:
    """
    List all active zones with cached surge data.
    """
    all_data = surge_cache.get_all_surges()
    zone_ids = list(all_data.keys())
    cities = list({data.get("city", "unknown") for data in all_data.values()})

    return ZoneListResponse(
        zones=zone_ids,
        total=len(zone_ids),
        cities=cities,
    )


@app.post("/ml/reload")
async def reload_model() -> dict[str, Any]:
    """
    Reload ML model from disk.

    Call this after running ml/train_model.py to load the new model.
    """
    success = surge_predictor.reload()
    return {
        "success": success,
        "ml_model_loaded": surge_predictor.is_loaded,
        "timestamp": datetime.now().isoformat(),
    }


# =============================================================================
# ENTRY POINT
# =============================================================================

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "api.main:app",
        host=settings.api.host,
        port=settings.api.port,
        reload=settings.api.reload,
    )