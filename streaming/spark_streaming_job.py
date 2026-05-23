"""
streaming/spark_streaming_job.py
=================================
Spark Structured Streaming job — stateless version.
Each micro-batch aggregates the events it received, then writes to Redis.
This avoids the Windows-incompatible state store delta-file issue.
"""

from __future__ import annotations

import json
import os
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
from streaming.surge_calculator import calculate_rule_based_surge


def create_spark_session() -> SparkSession:
    os.environ["PYSPARK_SUBMIT_ARGS"] = (
        "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 pyspark-shell"
    )
    spark = (
        SparkSession.builder
        .appName("SurgePricingEngine")
        .master("local[2]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    logger.info("Spark session created")
    return spark


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


def read_from_kafka(spark: SparkSession) -> DataFrame:
    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka.broker)
        .option("subscribe", settings.kafka.topic_ride_requests)
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 5000)
        .option("failOnDataLoss", "false")
        .load()
    )
    parsed_df = (
        raw_df.select(F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("zone_id").isNotNull())
    )
    logger.info(f"Reading from Kafka topic: {settings.kafka.topic_ride_requests}")
    return parsed_df


def process_batch(batch_df: DataFrame, batch_id: int) -> None:
    """Aggregate per-batch and write to Redis (stateless, no checkpointed state)."""
    import redis as redis_lib

    try:
        if batch_df.rdd.isEmpty():
            return

        # Aggregate this batch per zone
        agg_df = (
            batch_df
            .groupBy("zone_id", "city")
            .agg(
                F.count("event_id").alias("demand_1min"),
                F.avg("available_drivers").cast(IntegerType()).alias("available_drivers"),
                F.avg("weather_severity").cast(IntegerType()).alias("weather_severity"),
            )
        )

        rows = agg_df.collect()
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
            demand = row.demand_1min or 0
            drivers = max(row.available_drivers or 1, 1)
            rule_multiplier = calculate_rule_based_surge(demand, drivers)

            key = f"surge:{row.zone_id}"
            value = json.dumps({
                "zone_id": row.zone_id,
                "city": row.city,
                "demand_1min": demand,
                "demand_5min": demand * 5,
                "demand_15min": demand * 15,
                "available_drivers": drivers,
                "supply_ratio": demand / drivers,
                "final_multiplier": rule_multiplier,
                "rule_multiplier": rule_multiplier,
                "ml_multiplier": rule_multiplier,
                "ml_confidence": 0.0,
                "weather_severity": row.weather_severity or 0,
                "is_special_event": False,
                "updated_at": datetime.now().isoformat(),
            })
            pipeline.setex(key, settings.redis.ttl_seconds, value)

        pipeline.execute()
        logger.info(f"Batch {batch_id}: updated {len(rows)} zones in Redis")

    except Exception as e:
        logger.error(f"Batch {batch_id} failed: {e}")


def run_streaming_job() -> None:
    logger.info("Starting Surge Pricing Streaming Job")
    spark = create_spark_session()
    events_df = read_from_kafka(spark)

    query = (
        events_df
        .writeStream
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .foreachBatch(process_batch)
        .option("checkpointLocation", "file:///C:/spark_checkpoint/surge")
        .start()
    )

    logger.info("Streaming query started — surge data will appear in Redis every 5 seconds")
    query.awaitTermination()


if __name__ == "__main__":
    run_streaming_job()