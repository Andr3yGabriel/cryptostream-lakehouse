from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import window, avg, max, min, col

KAFKA_PKG = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0" 
DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"
ALL_PKGS = f"{KAFKA_PKG},{DELTA_PKG}"

spark = SparkSession.builder.appName("CryptoStreamLakehouse") \
    .config("spark.jars.packages", ALL_PKGS) \
    .config("spark.driver.extraJavaOptions", 
            "--add-opens=java.base/java.lang=ALL-UNNAMED " +
            "--add-opens=java.base/java.util=ALL-UNNAMED " +
            "--add-opens=java.base/java.nio=ALL-UNNAMED " +
            "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

CHECKPOINT_PATH = "./storage/checkpoints/gold/crypto_metrics_1m"
GOLD_PATH = "./storage/gold/crypto_metrics_1m"

df = spark.readStream \
    .format("delta") \
    .load("./storage/silver/crypto_transformed")

# Using withWatermark to handle late data and avoid state buildup
df_with_watermark = df.withWatermark("api_timestamp", "30 seconds")

df_aggregated = df_with_watermark.groupBy(
    window(col("api_timestamp"), "1 minute"),
    col("coin_id")
).agg(
    avg("price").alias("avg_price"),
    max("price").alias("max_price"),
    min("price").alias("min_price")
)

query = df_aggregated.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(GOLD_PATH)

query.awaitTermination()