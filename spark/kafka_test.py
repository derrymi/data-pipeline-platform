from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("KafkaReadTest")
    .getOrCreate()
)

df = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka:9092")
    .option("subscribe", "data-platform.public.customers")
    .option("startingOffsets", "earliest")
    .load()
)

result = df.selectExpr(
    "CAST(key AS STRING) AS key",
    "CAST(value AS STRING) AS value",
    "topic",
    "partition",
    "offset"
)

result.show(10, truncate=False)

print("Kafka успешно прочитана через Spark!")

spark.stop()