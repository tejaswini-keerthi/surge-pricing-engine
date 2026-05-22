"""
api/models.py
=============
Pydantic models for FastAPI request and response schemas.

Defines the shape of data going in and out of the API.
Pydantic validates all values automatically.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import BaseModel, Field


# =============================================================================
# RESPONSE MODELS
# =============================================================================

class SurgeResponse(BaseModel):
    """Response model for GET /surge/{zone_id}"""

    zone_id: str = Field(..., description="Geohash zone identifier")
    city: str = Field(..., description="City name")
    final_multiplier: float = Field(..., description="Final surge multiplier applied to fare", ge=1.0)
    rule_multiplier: float = Field(..., description="Rule-based surge multiplier", ge=1.0)
    ml_multiplier: float = Field(..., description="ML predicted surge multiplier", ge=1.0)
    ml_confidence: float = Field(..., description="ML model confidence score", ge=0.0, le=1.0)
    ml_available: bool = Field(..., description="Whether ML model is loaded and available")
    surge_tier: str = Field(..., description="Human readable tier: normal/low/medium/high/maximum")
    updated_at: datetime = Field(..., description="When this surge was last calculated")

    class Config:
        json_schema_extra = {
            "example": {
                "zone_id": "dr5ru6",
                "city": "new_york",
                "final_multiplier": 2.1,
                "rule_multiplier": 2.0,
                "ml_multiplier": 2.1,
                "ml_confidence": 0.94,
                "ml_available": True,
                "surge_tier": "high",
                "updated_at": "2026-05-22T14:23:00",
            }
        }


class AllSurgesResponse(BaseModel):
    """Response model for GET /surge/all"""

    zones: dict[str, SurgeResponse] = Field(
        ..., description="Map of zone_id to surge data"
    )
    total_zones: int = Field(..., description="Total number of active zones")
    surging_zones: int = Field(..., description="Number of zones with surge > 1.0")
    timestamp: datetime = Field(..., description="Response timestamp")


class HealthResponse(BaseModel):
    """Response model for GET /health"""

    status: str = Field(..., description="Service status: healthy/degraded/unhealthy")
    kafka_connected: bool
    cassandra_connected: bool
    redis_connected: bool
    ml_model_loaded: bool
    uptime_seconds: float
    timestamp: datetime


class ZoneListResponse(BaseModel):
    """Response model for GET /zones"""

    zones: list[str] = Field(..., description="List of all active zone IDs")
    total: int = Field(..., description="Total zone count")
    cities: list[str] = Field(..., description="List of active cities")


class SurgeHistoryResponse(BaseModel):
    """Response model for GET /surge/{zone_id}/history"""

    zone_id: str
    city: str
    history: list[dict] = Field(..., description="List of historical surge records")
    count: int


# =============================================================================
# REQUEST MODELS
# =============================================================================

class ZoneSurgeRequest(BaseModel):
    """Request model for POST /surge/batch"""

    zone_ids: list[str] = Field(
        ...,
        description="List of zone IDs to get surge for",
        min_length=1,
        max_length=100,
    )