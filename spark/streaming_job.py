"""Spark Structured Streaming - RapidGo Observabilidad (Variante 9).

Ejecucion:
  docker compose exec spark /opt/spark/bin/spark-submit \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
    --conf spark.jars.ivy=/tmp/.ivy \
    /opt/app/spark/streaming_job.py
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructField, StructType

import config

EVENT_SCHEMA = StructType(
    [
        StructField("event_id", StringType()),
        StructField("timestamp", StringType()),
        StructField("service", StringType()),
        StructField("endpoint", StringType()),
        StructField("http_method", StringType()),
        StructField("http_status", IntegerType()),
        StructField("latency_ms", IntegerType()),
        StructField("region", StringType()),
    ]
)


def read_raw(spark: SparkSession) -> DataFrame:
    """Lee Kafka y conserva el mensaje original con sus metadatos."""
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", config.KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", config.KAFKA_TOPIC)
        .option("startingOffsets", config.STARTING_OFFSETS)
        .option("failOnDataLoss", "false")
        .load()
    )
    return kafka_df.select(
        F.col("key").cast("string").alias("kafka_key"),
        F.col("value").cast("string").alias("raw_value"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
    )


def parse_and_enrich(raw_df: DataFrame) -> DataFrame:
    """Transformaciones 1, 2 y 3: parseo JSON, event time y bandera de error."""
    parsed = raw_df.withColumn("event", F.from_json("raw_value", config_schema()))
    flat = parsed.select(
        "kafka_key", "kafka_partition", "kafka_offset", "event.*"
    )
    return (
        flat.withColumn(  # T2: event time desde el timestamp del evento
            "event_time", F.to_timestamp("timestamp", config.TIMESTAMP_FORMAT)
        )
        .withColumn(  # T3: bandera de fallo (5xx)
            "is_error",
            (F.col("http_status") >= config.ERROR_STATUS_FROM).cast("int"),
        )
        .withColumn("is_valid", is_valid_expr())
    )


def config_schema() -> StructType:
    return EVENT_SCHEMA


def is_valid_expr():
    """Validacion segun docs/contrato.md. Null cuenta como invalido."""
    rules = (
        F.col("event_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("service").isin(config.SERVICES)
        & F.col("endpoint").startswith("/")
        & F.col("http_method").isin("GET", "POST", "PUT", "PATCH", "DELETE")
        & F.col("http_status").between(config.MIN_STATUS, config.MAX_STATUS)
        & F.col("latency_ms").between(config.MIN_LATENCY_MS, config.MAX_LATENCY_MS)
    )
    return F.coalesce(rules, F.lit(False))


def compute_kpis(valid_df: DataFrame) -> DataFrame:
    """Agregacion por ventana de 1 min y servicio, con KPIs y alertas."""
    kpis = (
        valid_df.withWatermark("event_time", config.WATERMARK)
        .groupBy(F.window("event_time", config.WINDOW_DURATION), "service")
        .agg(
            F.count("*").alias("total_events"),
            F.sum("is_error").alias("error_events"),
            F.percentile_approx("latency_ms", 0.95).alias("p95_latency_ms"),
        )
        .withColumn("error_rate", F.col("error_events") / F.col("total_events"))
        .withColumn("availability", 1 - F.col("error_rate"))
    )
    enough_sample = F.col("total_events") >= config.MIN_EVENTS_PER_WINDOW
    return kpis.withColumn(
        "alert_high_error_rate",
        enough_sample & (F.col("error_rate") > config.ERROR_RATE_THRESHOLD),
    ).withColumn(
        "alert_critical_latency",
        enough_sample & (F.col("p95_latency_ms") > config.P95_LATENCY_THRESHOLD_MS),
    ).select(
        F.col("window.start").alias("window_start"),
        F.col("window.end").alias("window_end"),
        "service",
        "total_events",
        "error_events",
        "error_rate",
        "availability",
        "p95_latency_ms",
        "alert_high_error_rate",
        "alert_critical_latency",
    )


def start_parquet_sink(df: DataFrame, name: str, path: str):
    """Sink Parquet en modo append, con checkpoint propio."""
    return (
        df.writeStream.format("parquet")
        .outputMode("append")
        .option("path", path)
        .option("checkpointLocation", f"{config.CHECKPOINT_PATH}/{name}")
        .trigger(processingTime=config.TRIGGER_INTERVAL)
        .queryName(name)
        .start()
    )


def main() -> None:
    spark = (
        SparkSession.builder.appName("rapidgo-observabilidad")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_df = read_raw(spark)
    enriched = parse_and_enrich(raw_df)
    valid = enriched.filter(F.col("is_valid")).drop("is_valid")

    queries = [
        # raw: mensaje original + particion/offset (evidencia para ADR-001)
        start_parquet_sink(raw_df, "raw", config.RAW_PATH),
        # processed: eventos validados y tipados
        start_parquet_sink(valid, "processed", config.PROCESSED_PATH),
        # curated: KPIs y alertas por ventana y servicio
        start_parquet_sink(compute_kpis(valid), "curated", config.CURATED_PATH),
    ]
    print(f"Queries activas: {[q.name for q in queries]}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
