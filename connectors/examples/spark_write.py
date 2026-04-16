"""
PySpark — Write embeddings to GVDB.

Prerequisites:
  spark-submit --packages io.gvdb:gvdb-spark-connector_2.13:0.14.0 spark_write.py

Or add the shadow JAR directly:
  spark-submit --jars /path/to/gvdb-spark-connector-0.14.0-all.jar spark_write.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField, LongType, FloatType, ArrayType, StringType
)
import random

spark = SparkSession.builder \
    .master("local[*]") \
    .appName("gvdb-spark-write-example") \
    .getOrCreate()

# --- Generate sample data ---
DIMENSION = 128
NUM_VECTORS = 10_000

data = [
    (i, [random.gauss(0, 1) for _ in range(DIMENSION)], f"item_{i}", random.random())
    for i in range(NUM_VECTORS)
]

schema = StructType([
    StructField("id", LongType(), False),
    StructField("vector", ArrayType(FloatType()), False),
    StructField("name", StringType(), True),
    StructField("score", FloatType(), True),
])

df = spark.createDataFrame(data, schema)

# --- Write to GVDB ---
df.write.format("io.gvdb.spark") \
    .option("gvdb.target", "localhost:50051") \
    .option("gvdb.collection", "spark_embeddings") \
    .option("gvdb.dimension", str(DIMENSION)) \
    .option("gvdb.metric", "cosine") \
    .option("gvdb.index_type", "auto") \
    .option("gvdb.batch_size", "5000") \
    .mode("append") \
    .save()

print(f"Wrote {NUM_VECTORS} vectors to GVDB collection 'spark_embeddings'")

# --- Read back ---
df_read = spark.read.format("io.gvdb.spark") \
    .option("gvdb.target", "localhost:50051") \
    .option("gvdb.collection", "spark_embeddings") \
    .option("gvdb.include_metadata", "true") \
    .load()

print(f"Read back {df_read.count()} vectors")
df_read.show(5, truncate=True)

spark.stop()
