"""
Spark Structured Streaming Job for Event Processing

This job:
1. Consumes events from Kafka
2. Parses and validates event schema
3. Performs aggregations and transformations
4. Writes to Parquet files partitioned by date
5. Maintains exactly-once semantics via checkpointing

Usage:
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
        event_processor.py
"""

import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, avg, 
    sum as spark_sum, min as spark_min, max as spark_max,
    expr, current_timestamp, date_format, lit,
    when, coalesce, struct, collect_list
)
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    IntegerType, FloatType, ArrayType, MapType
)


# Configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "user_events")
OUTPUT_PATH = os.getenv("OUTPUT_PATH", "/opt/spark-data/events")
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/opt/spark-data/checkpoints")
TRIGGER_INTERVAL = os.getenv("TRIGGER_INTERVAL", "30 seconds")


def create_spark_session() -> SparkSession:
    """Create and configure Spark session."""
    return (
        SparkSession.builder
        .appName("FeatureLaunchpad-EventProcessor")
        .config("spark.sql.streaming.checkpointLocation", CHECKPOINT_PATH)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.streaming.backpressure.enabled", "true")
        .config("spark.sql.adaptive.enabled", "true")
        .getOrCreate()
    )


def get_event_schema() -> StructType:
    """Define the event schema matching the backend Pydantic models."""
    
    context_schema = StructType([
        StructField("page_url", StringType(), True),
        StructField("page_path", StringType(), True),
        StructField("referrer", StringType(), True),
        StructField("user_agent", StringType(), True),
        StructField("device_type", StringType(), True),
        StructField("screen_width", IntegerType(), True),
        StructField("screen_height", IntegerType(), True),
        StructField("viewport_width", IntegerType(), True),
        StructField("viewport_height", IntegerType(), True),
        StructField("timezone", StringType(), True),
        StructField("locale", StringType(), True),
    ])
    
    properties_schema = StructType([
        StructField("question_id", IntegerType(), True),
        StructField("step_number", IntegerType(), True),
        StructField("total_steps", IntegerType(), True),
        StructField("slider_value", FloatType(), True),
        StructField("previous_value", FloatType(), True),
        StructField("option_id", StringType(), True),
        StructField("option_label", StringType(), True),
        StructField("selected_options", ArrayType(StringType()), True),
        StructField("recommendation_id", StringType(), True),
        StructField("recommendation_name", StringType(), True),
        StructField("match_score", FloatType(), True),
        StructField("time_on_step_ms", IntegerType(), True),
        StructField("total_time_ms", IntegerType(), True),
        StructField("share_platform", StringType(), True),
        StructField("cta_id", StringType(), True),
        StructField("cta_label", StringType(), True),
        StructField("extra", MapType(StringType(), StringType()), True),
    ])
    
    return StructType([
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("event_type", StringType(), False),
        StructField("event_properties", properties_schema, True),
        StructField("feature_name", StringType(), True),
        StructField("context", context_schema, True),
        StructField("timestamp", StringType(), False),
        StructField("received_at", StringType(), True),
    ])


def read_kafka_stream(spark: SparkSession):
    """Read events from Kafka as a streaming DataFrame."""
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .option("maxOffsetsPerTrigger", 10000)
        .load()
    )


def parse_events(kafka_df, schema: StructType):
    """Parse JSON events from Kafka messages."""
    return (
        kafka_df
        .selectExpr("CAST(value AS STRING) as json_value", "timestamp as kafka_timestamp")
        .select(
            from_json(col("json_value"), schema).alias("event"),
            col("kafka_timestamp")
        )
        .select(
            col("event.*"),
            col("kafka_timestamp"),
            to_timestamp(col("event.timestamp")).alias("event_timestamp")
        )
        .withColumn("processed_at", current_timestamp())
        .withColumn("event_date", date_format(col("event_timestamp"), "yyyy-MM-dd"))
        .withColumn("event_hour", date_format(col("event_timestamp"), "HH"))
    )


def write_raw_events(events_df, output_path: str, checkpoint_path: str):
    """Write raw events to Parquet, partitioned by date and hour."""
    return (
        events_df
        .writeStream
        .format("parquet")
        .option("path", f"{output_path}/raw")
        .option("checkpointLocation", f"{checkpoint_path}/raw")
        .partitionBy("event_date", "event_hour")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )


def compute_session_metrics(events_df, output_path: str, checkpoint_path: str):
    """
    Compute session-level metrics with watermarking for late data.
    
    Metrics computed:
    - Events per session
    - Session duration
    - Completion rate
    - Device distribution
    """
    session_metrics = (
        events_df
        .withWatermark("event_timestamp", "1 hour")
        .groupBy(
            window(col("event_timestamp"), "1 hour"),
            col("session_id"),
            col("user_id"),
            col("feature_name"),
            col("context.device_type").alias("device_type")
        )
        .agg(
            count("*").alias("event_count"),
            spark_min("event_timestamp").alias("session_start"),
            spark_max("event_timestamp").alias("session_end"),
            
            # Quiz metrics
            spark_sum(when(col("event_type") == "quiz_started", 1).otherwise(0)).alias("quizzes_started"),
            spark_sum(when(col("event_type") == "quiz_completed", 1).otherwise(0)).alias("quizzes_completed"),
            
            # Engagement metrics
            spark_sum(when(col("event_type") == "slider_adjusted", 1).otherwise(0)).alias("slider_interactions"),
            spark_sum(when(col("event_type") == "option_selected", 1).otherwise(0)).alias("option_selections"),
            
            # Time metrics
            spark_max(col("event_properties.total_time_ms")).alias("total_time_ms"),
            
            # Result metrics
            spark_max(col("event_properties.match_score")).alias("final_match_score"),
            spark_max(col("event_properties.recommendation_id")).alias("recommendation_id"),
            
            # Sharing
            spark_sum(when(col("event_type") == "result_shared", 1).otherwise(0)).alias("shares"),
        )
        .withColumn("session_duration_seconds", 
            (col("session_end").cast("long") - col("session_start").cast("long")))
        .withColumn("completion_rate",
            when(col("quizzes_started") > 0, 
                col("quizzes_completed") / col("quizzes_started")
            ).otherwise(0))
    )
    
    return (
        session_metrics
        .writeStream
        .format("parquet")
        .option("path", f"{output_path}/session_metrics")
        .option("checkpointLocation", f"{checkpoint_path}/session_metrics")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )


def compute_realtime_dashboard_metrics(events_df, output_path: str, checkpoint_path: str):
    """
    Compute real-time metrics for the dashboard.
    
    Updates every processing interval with:
    - Active users (last 5 minutes)
    - Events per minute
    - Completion funnel
    """
    realtime_metrics = (
        events_df
        .withWatermark("event_timestamp", "5 minutes")
        .groupBy(
            window(col("event_timestamp"), "1 minute"),
            col("feature_name"),
            col("event_type")
        )
        .agg(
            count("*").alias("event_count"),
            expr("count(distinct user_id)").alias("unique_users"),
            expr("count(distinct session_id)").alias("unique_sessions"),
            avg(col("event_properties.time_on_step_ms")).alias("avg_time_on_step_ms"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("window_end", col("window.end"))
        .drop("window")
    )
    
    return (
        realtime_metrics
        .writeStream
        .format("parquet")
        .option("path", f"{output_path}/realtime_metrics")
        .option("checkpointLocation", f"{checkpoint_path}/realtime_metrics")
        .trigger(processingTime="1 minute")
        .outputMode("append")
        .start()
    )


def compute_funnel_metrics(events_df, output_path: str, checkpoint_path: str):
    """
    Compute funnel analysis metrics.
    
    Tracks user progression through:
    quiz_started → question_viewed → quiz_step_completed → quiz_completed → result_shared
    """
    funnel_events = ["quiz_started", "question_viewed", "quiz_step_completed", 
                     "quiz_completed", "result_viewed", "result_shared"]
    
    funnel_metrics = (
        events_df
        .filter(col("event_type").isin(funnel_events))
        .withWatermark("event_timestamp", "1 hour")
        .groupBy(
            window(col("event_timestamp"), "1 hour"),
            col("feature_name"),
            col("event_type")
        )
        .agg(
            expr("count(distinct user_id)").alias("unique_users"),
            expr("count(distinct session_id)").alias("unique_sessions"),
            count("*").alias("total_events"),
        )
        .withColumn("window_start", col("window.start"))
        .withColumn("funnel_step", 
            when(col("event_type") == "quiz_started", 1)
            .when(col("event_type") == "question_viewed", 2)
            .when(col("event_type") == "quiz_step_completed", 3)
            .when(col("event_type") == "quiz_completed", 4)
            .when(col("event_type") == "result_viewed", 5)
            .when(col("event_type") == "result_shared", 6)
            .otherwise(0)
        )
        .drop("window")
    )
    
    return (
        funnel_metrics
        .writeStream
        .format("parquet")
        .option("path", f"{output_path}/funnel_metrics")
        .option("checkpointLocation", f"{checkpoint_path}/funnel_metrics")
        .trigger(processingTime=TRIGGER_INTERVAL)
        .outputMode("append")
        .start()
    )


def main():
    """Main entry point for the streaming job."""
    print("=" * 60)
    print("Feature Launchpad - Event Processor")
    print("=" * 60)
    print(f"Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"Topic: {KAFKA_TOPIC}")
    print(f"Output: {OUTPUT_PATH}")
    print("=" * 60)
    
    # Create Spark session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    
    # Get event schema
    schema = get_event_schema()
    
    # Read from Kafka
    kafka_stream = read_kafka_stream(spark)
    
    # Parse events
    events = parse_events(kafka_stream, schema)
    
    # Start all streaming queries
    queries = [
        write_raw_events(events, OUTPUT_PATH, CHECKPOINT_PATH),
        compute_session_metrics(events, OUTPUT_PATH, CHECKPOINT_PATH),
        compute_realtime_dashboard_metrics(events, OUTPUT_PATH, CHECKPOINT_PATH),
        compute_funnel_metrics(events, OUTPUT_PATH, CHECKPOINT_PATH),
    ]
    
    print(f"Started {len(queries)} streaming queries")
    
    # Wait for any query to terminate
    for query in queries:
        query.awaitTermination()


if __name__ == "__main__":
    main()
