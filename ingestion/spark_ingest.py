"""
spark_ingest.py
Query 1 — Kafka wiki-raw -> Iceberg local.raw.wiki_events_raw

Running from Spark container:
    spark-submit --master spark://spark-master:7077 \\
      --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0,\\
                 org.apache.hadoop:hadoop-aws:3.3.4,\\
                 software.amazon.awssdk:bundle:2.20.18 \\
      /app/spark_ingest.py
"""

import os

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import (
    BooleanType,
    IntegerType,
    LongType,
    StringType,
    StructType,
)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
CHECKPOINT_BASE = os.getenv("CHECKPOINT_BASE", "s3a://warehouse/checkpoints")

# Full Wikimedia RecentChange schema
WIKI_SCHEMA = (
    StructType()
    .add("id",            LongType(),    True)
    .add("type",          StringType(),  True)
    .add("namespace",     IntegerType(), True)
    .add("title",         StringType(),  True)
    .add("comment",       StringType(),  True)
    .add("parsedcomment", StringType(),  True)
    .add("timestamp",     LongType(),    True)
    .add("user",          StringType(),  True)
    .add("bot",           BooleanType(), False)
    .add("minor",         BooleanType(), False)
    .add("old_len",       IntegerType(), True)
    .add("new_len",       IntegerType(), True)
    .add("rev_id",        LongType(),    True)
    .add("wiki",          StringType(),  True)
    .add("server_name",   StringType(),  True)
)


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("wikit-ingest")
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.local",
                "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.local.type", "rest")
        .config("spark.sql.catalog.local.uri", "http://iceberg-rest:8181")
        .config("spark.sql.catalog.local.io-impl",
                "org.apache.iceberg.aws.s3.S3FileIO")
        .config("spark.sql.catalog.local.s3.endpoint", "http://minio:9000")
        .config("spark.sql.catalog.local.s3.path-style-access", "true")
        .config("spark.sql.catalog.local.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",
                "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


def ensure_table(spark: SparkSession) -> None:
    spark.sql("CREATE NAMESPACE IF NOT EXISTS local.raw")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS local.raw.wiki_events_raw (
            id             BIGINT,
            type           STRING,
            namespace      INT,
            title          STRING,
            comment        STRING,
            parsedcomment  STRING,
            event_ts       TIMESTAMP,
            user           STRING,
            bot            BOOLEAN,
            minor          BOOLEAN,
            old_len        INT,
            new_len        INT,
            rev_id         BIGINT,
            wiki           STRING,
            server_name    STRING,
            ingested_at    TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (days(event_ts), wiki)
        TBLPROPERTIES (
            'write.format.default'            = 'parquet',
            'write.parquet.compression-codec' = 'snappy',
            'write.target-file-size-bytes'    = '134217728'
        )
    """)


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")
    ensure_table(spark)

    raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", "wiki-raw")
        .option("startingOffsets", "latest")
        .option("maxOffsetsPerTrigger", 10_000)
        .option("kafka.group.id", "wikit-spark-ingest")
        .load()
    )

    parsed = (
        raw_df
        .selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), WIKI_SCHEMA).alias("e"))
        .select(
            col("e.id"),
            col("e.type"),
            col("e.namespace"),
            col("e.title"),
            col("e.comment"),
            col("e.parsedcomment"),
            col("e.timestamp").cast("timestamp").alias("event_ts"),
            col("e.user"),
            col("e.bot"),
            col("e.minor"),
            col("e.old_len"),
            col("e.new_len"),
            col("e.rev_id"),
            col("e.wiki"),
            col("e.server_name"),
            current_timestamp().alias("ingested_at"),
        )
        .filter(col("rev_id").isNotNull() & col("title").isNotNull())
    )

    (
        parsed.writeStream
        .format("iceberg")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("checkpointLocation", f"{CHECKPOINT_BASE}/raw-ingest")
        .toTable("local.raw.wiki_events_raw")
        .start()
        .awaitTermination()
    )


if __name__ == "__main__":
    main()