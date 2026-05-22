"""
streaming/spark_streaming_job.py
=================================
Main Spark Structured Streaming job for surge pricing.

Reads ride request events from Kafka, computes demand per zone
using sliding windows, calculates surge multipliers using both
rule-based and ML approaches, and writes results to Cassandra and Redis.

Pipeline:
    Kafka → Spark Structured Streaming → surge_calculator
          → Cassandra (permanent storage)
          → Redis (fast cache for API)
"""

from __future__ import annotations

import json
from datetime import datetime

from loguru import logger
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from config.settings import settings
from streaming.watermark_config import WINDOW_1MIN, WINDOW_5MIN, WINDOW_15MIN, STREAM_CONFIG
from streaming.surge_calculator import calculate_rule_based_surge, calculate_final_surge


# =============================================================================
# SPARK SESSION
# =============================================================================

def create_spark_session() -> SparkSession:
    """
    Create and configure Spark session with Kafka and Cassandra support.

    Returns:
        Configured SparkSession
    """
    spark = (
        SparkSession.builder
        .appName("SurgePricingEngine")
        .config("spark.jars.packages",
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1")
        .config("spark.cassandra.connection.host", settings.cassandra.host)
        .config("spark.cassandra.connection.port", str(settings.cassandra.port))
        .config("spark.sql.shuffle.partitions", "10")
        .config("spark.sql.streaming.checkpointLocation", STREAM_CONFIG.checkpoint_location)
        .config("spark.streaming.kafka.maxRatePerPartition", "1000")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    logger.info("Spark session created")
    return spark


# =============================================================================
# SCHEMA
# =============================================================================

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("zone_id", StringType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("timestamp_ms", LongType(), True),
    StructField("available_drivers", IntegerType(), True),
    StructField("weather_severity", IntegerType(), True),
    StructField("demand_count", IntegerType(), True),
])


# =============================================================================
# KAFKA SOURCE
# =============================================================================

def read_from_kafka(spark: SparkSession) -> DataFrame:
    """
    Create streaming DataFrame from Kafka topic.

    Args:
        spark: Active SparkSession

    Returns:
        Streaming DataFrame with parsed ride request events
    """
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.broker)
        .option("subscribe", settings.kafka.topic_ride_requests)
        .option("startingOffsets", settings.kafka.auto_offset_reset)
        .option("maxOffsetsPerTrigger", STREAM_CONFIG.max_offsets_per_trigger)
        .option("failOnDataLoss", "false")
        .load()
    )

    # Parse JSON value and add event timestamp
    parsed_df = (
        raw_df
        .select(
            F.from_json(
                F.col("value").cast("string"),
                EVENT_SCHEMA
            ).alias("data")
        )
        .select("data.*")
        .withColumn(
            "event_time",
            (F.col("timestamp_ms") / 1000).cast("timestamp")
        )
        .filter(F.col("zone_id").isNotNull())
        .filter(F.col("event_time").isNotNull())
    )

    logger.info(f"Reading from Kafka topic: {settings.kafka.topic_ride_requests}")
    return parsed_df


# =============================================================================
# WINDOW AGGREGATIONS
# =============================================================================

def compute_windowed_demand(
    df: DataFrame,
    window_config: object,
) -> DataFrame:
    """
    Compute demand count per zone for a sliding window.

    Args:
        df: Streaming DataFrame with event_time and zone_id
        window_config: WindowConfig object

    Returns:
        Aggregated DataFrame with demand count per zone per window
    """
    return (
        df
        .withWatermark("event_time", window_config.watermark_str)
        .groupBy(
            F.window("event_time", window_config.duration_str, window_config.slide_str),
            F.col("zone_id"),
            F.col("city"),
        )
        .agg(
            F.count("event_id").alias("demand_count"),
            F.avg("available_drivers").cast(IntegerType()).alias("avg_drivers"),
            F.avg("weather_severity").cast(IntegerType()).alias("weather_severity"),
        )
        .select(
            F.col("zone_id"),
            F.col("city"),
            F.col("window.end").alias("window_end"),
            F.col("demand_count"),
            F.col("avg_drivers"),
            F.col("weather_severity"),
        )
    )


# =============================================================================
# SURGE CALCULATION UDF
# =============================================================================

@F.udf(returnType=DoubleType())
def rule_based_surge_udf(demand: int, supply: int) -> float:
    """Spark UDF wrapper for rule-based surge calculation."""
    return calculate_rule_based_surge(demand or 0, supply or 1)


@F.udf(returnType=StringType())
def surge_tier_udf(multiplier: float) -> str:
    """Spark UDF wrapper for surge tier labeling."""
    from streaming.surge_calculator import get_surge_tier
    return get_surge_tier(multiplier or 1.0)


# =============================================================================
# WRITE TO CASSANDRA
# =============================================================================

def write_to_cassandra(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write surge results batch to Cassandra.

    Called by Spark's foreachBatch for each micro-batch.

    Args:
        batch_df: DataFrame containing surge results for this batch
        batch_id: Spark batch identifier
    """
    try:
        count = batch_df.count()
        if count == 0:
            return

        (
            batch_df
            .write
            .format("org.apache.spark.sql.cassandra")
            .options(
                table="surge_results",
                keyspace=settings.cassandra.keyspace,
            )
            .mode("append")
            .save()
        )

        logger.info(f"Batch {batch_id}: wrote {count} records to Cassandra")

    except Exception as e:
        logger.error(f"Cassandra write failed for batch {batch_id}: {e}")


# =============================================================================
# WRITE TO REDIS
# =============================================================================

def write_to_redis(batch_df: DataFrame, batch_id: int) -> None:
    """
    Write latest surge per zone to Redis cache.

    Called by Spark's foreachBatch for each micro-batch.

    Args:
        batch_df: DataFrame containing surge results
        batch_id: Spark batch identifier
    """
    import redis as redis_lib

    try:
        rows = batch_df.collect()
        if not rows:
            return

        redis_client = redis_lib.Redis(
            host=settings.redis.host,
            port=settings.redis.port,
            db=settings.redis.db,
            decode_responses=True,
        )

        pipeline = redis_client.pipeline()

        for row in rows:
            key = f"surge:{row.zone_id}"
            value = json.dumps({
                "zone_id": row.zone_id,
                "city": row.city,
                "final_multiplier": row.final_multiplier,
                "rule_multiplier": row.rule_multiplier,
                "ml_multiplier": row.ml_multiplier,
                "ml_confidence": row.ml_confidence,
                "updated_at": datetime.now().isoformat(),
            })
            pipeline.setex(key, settings.redis.ttl_seconds, value)

        pipeline.execute()
        logger.debug(f"Batch {batch_id}: updated {len(rows)} zones in Redis")

    except Exception as e:
        logger.error(f"Redis write failed for batch {batch_id}: {e}")


# =============================================================================
# MAIN STREAMING JOB
# =============================================================================

def run_streaming_job() -> None:
    """
    Main entry point for the Spark Structured Streaming job.

    Creates Spark session, reads from Kafka, computes windowed
    demand aggregations, calculates surge prices, and writes
    results to Cassandra and Redis.
    """
    logger.info("Starting Surge Pricing Streaming Job")

    spark = create_spark_session()

    # Read events from Kafka
    events_df = read_from_kafka(spark)

    # Compute demand for all three windows simultaneously
    demand_1min = compute_windowed_demand(events_df, WINDOW_1MIN)
    demand_5min = compute_windowed_demand(events_df, WINDOW_5MIN)
    demand_15min = compute_windowed_demand(events_df, WINDOW_15MIN)

    # Join all three window results on zone_id and window_end
    combined_df = (
        demand_1min.alias("w1")
        .join(
            demand_5min.alias("w5"),
            on=["zone_id", "city"],
            how="inner",
        )
        .join(
            demand_15min.alias("w15"),
            on=["zone_id", "city"],
            how="inner",
        )
        .select(
            F.col("zone_id"),
            F.col("city"),
            F.col("w1.demand_count").alias("demand_1min"),
            F.col("w5.demand_count").alias("demand_5min"),
            F.col("w15.demand_count").alias("demand_15min"),
            F.col("w1.avg_drivers").alias("available_drivers"),
            F.col("w1.weather_severity").alias("weather_severity"),
            F.col("w1.window_end").alias("timestamp"),
        )
    )

    # Calculate rule-based surge
    surge_df = combined_df.withColumn(
        "rule_multiplier",
        rule_based_surge_udf(
            F.col("demand_1min"),
            F.col("available_drivers"),
        )
    )

    # Add placeholder ML columns (filled by ML serving layer)
    surge_df = (
        surge_df
        .withColumn("ml_multiplier", F.col("rule_multiplier"))
        .withColumn("ml_confidence", F.lit(0.0))
        .withColumn("final_multiplier", F.col("rule_multiplier"))
        .withColumn("surge_tier", surge_tier_udf(F.col("final_multiplier")))
        .withColumn("is_special_event", F.lit(False))
    )

    # Write to Cassandra
    cassandra_query = (
        surge_df
        .writeStream
        .outputMode(STREAM_CONFIG.output_mode)
        .trigger(processingTime=STREAM_CONFIG.trigger_str)
        .foreachBatch(write_to_cassandra)
        .option("checkpointLocation", f"{STREAM_CONFIG.checkpoint_location}/cassandra")
        .start()
    )

    # Write to Redis
    redis_query = (
        surge_df
        .writeStream
        .outputMode(STREAM_CONFIG.output_mode)
        .trigger(processingTime=STREAM_CONFIG.trigger_str)
        .foreachBatch(write_to_redis)
        .option("checkpointLocation", f"{STREAM_CONFIG.checkpoint_location}/redis")
        .start()
    )

    logger.info("Streaming queries started — waiting for data")

    # Wait for both queries to terminate
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run_streaming_job()