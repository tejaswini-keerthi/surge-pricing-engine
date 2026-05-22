-- =============================================================================
-- Cassandra Schema for Surge Pricing Engine
-- =============================================================================
-- Run this file once to initialize all tables before starting the pipeline
-- Usage: cqlsh -f storage/schema.cql
-- =============================================================================

-- Create keyspace
CREATE KEYSPACE IF NOT EXISTS surge_pricing
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
}
AND durable_writes = true;

USE surge_pricing;

-- =============================================================================
-- SURGE RESULTS TABLE
-- Stores every surge calculation result per zone
-- Partition key: zone_id (all data for a zone on same node)
-- Clustering key: timestamp (sorted newest first)
-- =============================================================================
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
  AND default_time_to_live = 604800;

-- =============================================================================
-- CURRENT SURGE TABLE
-- Stores only the latest surge per zone (for fast lookups)
-- =============================================================================
CREATE TABLE IF NOT EXISTS current_surge (
    zone_id          TEXT PRIMARY KEY,
    city             TEXT,
    final_multiplier DOUBLE,
    rule_multiplier  DOUBLE,
    ml_multiplier    DOUBLE,
    ml_confidence    DOUBLE,
    updated_at       TIMESTAMP
);

-- =============================================================================
-- ML FEATURES TABLE
-- Stores computed features for model training
-- =============================================================================
CREATE TABLE IF NOT EXISTS ml_features (
    zone_id                 TEXT,
    timestamp               TIMESTAMP,
    city                    TEXT,
    demand_last_1min        INT,
    demand_last_5min        INT,
    demand_last_15min       INT,
    demand_spike_factor     DOUBLE,
    available_drivers       INT,
    supply_ratio            DOUBLE,
    hour_of_day             INT,
    day_of_week             INT,
    is_weekend              BOOLEAN,
    is_rush_hour            BOOLEAN,
    is_late_night           BOOLEAN,
    is_lunch_hour           BOOLEAN,
    is_holiday              BOOLEAN,
    days_to_next_holiday    INT,
    temperature             DOUBLE,
    precipitation           DOUBLE,
    weather_severity        INT,
    historical_avg_demand   DOUBLE,
    historical_avg_surge    DOUBLE,
    is_special_event        BOOLEAN,
    event_demand_multiplier DOUBLE,
    actual_multiplier       DOUBLE,
    PRIMARY KEY (zone_id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp DESC)
  AND default_time_to_live = 2592000;

-- =============================================================================
-- ZONE METADATA TABLE
-- Stores static information about each zone
-- =============================================================================
CREATE TABLE IF NOT EXISTS zone_metadata (
    zone_id     TEXT PRIMARY KEY,
    city        TEXT,
    geohash     TEXT,
    latitude    DOUBLE,
    longitude   DOUBLE,
    created_at  TIMESTAMP
);