from pyspark.sql import SparkSession
from delta import *
from pyspark.sql.functions import current_timestamp

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

CHECKPOINT_PATH = "./storage/checkpoints/bronze/crypto_raw"
BRONZE_PATH = "./storage/bronze/crypto_raw"

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:19092") \
    .option("subscribe", "crypto-raw") \
    .option("startingOffsets", "earliest") \
    .load()

df_parsed = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
              .withColumn("timestamp", current_timestamp())

query = df_parsed.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", CHECKPOINT_PATH) \
    .start(BRONZE_PATH)

query.awaitTermination()