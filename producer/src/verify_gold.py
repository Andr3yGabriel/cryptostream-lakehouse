from pyspark.sql import SparkSession
from pyspark.sql.functions import col

DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"

spark = SparkSession.builder.appName("VerifyGold") \
    .config("spark.jars.packages", DELTA_PKG) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

GOLD_PATH = "./storage/gold/crypto_metrics_1m"

df_gold = spark.read.format("delta").load(GOLD_PATH)

# A coluna 'window' gerada pelo Spark é um Struct com 'start' e 'end'.
# Vamos expandi-la para ficar mais fácil de ler no terminal.
df_gold_clean = df_gold.select(
    col("window.start").alias("window_start"),
    col("window.end").alias("window_end"),
    "coin_id",
    "avg_price",
    "min_price",
    "max_price"
).orderBy("window_start", "coin_id")

print("\n" + "="*70)
print("📊 MÉTRICAS GOLD (VELAS DE 1 MINUTO)")
print("="*70)
df_gold_clean.show(truncate=False)