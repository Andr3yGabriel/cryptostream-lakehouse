from delta import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType

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

CHECKPOINT_PATH = "./storage/checkpoints/silver/crypto_transformed"
SILVER_PATH = "./storage/silver/crypto_transformed"

df = spark.readStream \
    .format("delta") \
    .load("./storage/bronze/crypto_raw")

schema = StructType([
    StructField("coin_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("timestamp", DoubleType(), True),
    StructField("volume_24h", DoubleType(), True)
])

df_expanded = df.withColumn("data", from_json(col("value"), schema))

df_parsed = df_expanded.select(
    col("data.coin_id").alias("coin_id"),
    col("data.price").alias("price"),
    col("data.volume_24h").alias("volume_24h"),
    col("data.timestamp").cast("timestamp").alias("api_timestamp"),
    col("timestamp").alias("ingestion_timestamp")
)

query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(SILVER_PATH)

query.awaitTermination()