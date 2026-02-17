from pyspark.sql import SparkSession

# Precisamos apenas do pacote Delta para ler a tabela
DELTA_PKG = "io.delta:delta-spark_2.12:3.1.0"

spark = SparkSession.builder.appName("VerifySilver") \
    .config("spark.jars.packages", DELTA_PKG) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Caminho da nossa tabela Silver
SILVER_PATH = "./storage/silver/crypto_transformed"

# Leitura em modo BATCH (spark.read em vez de spark.readStream)
df_silver = spark.read.format("delta").load(SILVER_PATH)

print("\n" + "="*50)
print("🔍 SCHEMA DA TABELA SILVER")
print("="*50)
df_silver.printSchema()

print("\n" + "="*50)
print("📊 AMOSTRA DOS DADOS LIMPOS")
print("="*50)
# Mostra os 20 primeiros registros sem cortar o texto
df_silver.show(truncate=False)