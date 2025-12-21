from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType

# Define Schema
schema = StructType([
    StructField("timestamp", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("page_url", StringType(), True),
    StructField("action", StringType(), True)
])

# Initialize Spark
spark = SparkSession.builder \
    .appName("ClickstreamRealTime") \
    .getOrCreate()

# Read from Kafka
# Inside the network, Spark uses the container name 'kafka' and port 29092
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:29092") \
    .option("subscribe", "clickstream") \
    .load()

# Parse JSON
parsed_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

# Write to HDFS (Parquet Sink)
query_hdfs = parsed_df.writeStream \
    .format("parquet") \
    .option("path", "hdfs://namenode:9000/user/clickstream/output") \
    .option("checkpointLocation", "hdfs://namenode:9000/user/clickstream/checkpoint") \
    .outputMode("append") \
    .start()

# Write to Console (For testing/debugging)
query_console = parsed_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()