import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    StringType,
    LongType
)

spark = (
    SparkSession.builder
    .appName("CDCToSeaweedFS")
    .config("spark.hadoop.fs.s3a.endpoint", "http://seaweedfs:8333")
    .config(
        "spark.hadoop.fs.s3a.access.key",
        os.environ["AWS_ACCESS_KEY_ID"]
    )
    .config(
        "spark.hadoop.fs.s3a.secret.key",
        os.environ["AWS_SECRET_ACCESS_KEY"]
    )
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .getOrCreate()
)

customer_schema = StructType([
    StructField("id", IntegerType()),
    StructField("name", StringType()),
    StructField("email", StringType()),
    StructField("city", StringType())
])

cdc_schema = StructType([
    StructField("payload", StructType([
        StructField("before", customer_schema),
        StructField("after", customer_schema),
        StructField("op", StringType()),
        StructField("ts_ms", LongType())
    ]))
])

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "data-platform.public.customers")
    .option("startingOffsets", "latest")
    .load()
)

parsed_df = kafka_df.select(
    from_json(
        col("value").cast("string"),
        cdc_schema
    ).alias("event")
)

result_df = parsed_df.select(
    col("event.payload.before").alias("before"),
    col("event.payload.after").alias("after"),
    col("event.payload.op").alias("operation"),
    col("event.payload.ts_ms").alias("event_timestamp")
)

query = (
    result_df.writeStream
    .format("json")
    .outputMode("append")
    .option("path", "s3a://data-lake/cdc/customers_stream")
    .option(
        "checkpointLocation",
        "/opt/spark-apps/checkpoints/cdc_customers"
    )
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

print("Новые CDC-события обработаны и сохранены в SeaweedFS!")

spark.stop()