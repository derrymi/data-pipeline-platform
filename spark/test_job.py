import os

from pyspark.sql import SparkSession


spark = (
    SparkSession.builder
    .appName("DataPlatformTest")
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

data = [
    (1, "Alice", 100),
    (2, "Bob", 200),
    (3, "Charlie", 150),
]

df = spark.createDataFrame(data, ["id", "name", "amount"])

df.show()

df.write.mode("overwrite").json("s3a://data-lake/test-output")

print("Data successfully written to SeaweedFS!")

spark.stop()