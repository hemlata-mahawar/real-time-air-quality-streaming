from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from kafka import KafkaProducer
import json
import time

# -------------------------------
# 1. Start Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("AirQualityProducer") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

# -------------------------------
# 2. Read Data from GCS
# -------------------------------
df = spark.read.csv(
    "gs://ibd-oppe01/dataset/globalAirQuality.csv",
    header=True,
    inferSchema=True
)

# -------------------------------
# 3. Basic Cleaning
# -------------------------------

# Drop completely null rows
df = df.dropna(how="all")

# Drop rows missing critical fields
df = df.dropna(subset=["city", "timestamp"])

# Convert timestamp column
df = df.withColumn("timestamp", col("timestamp").cast("timestamp"))

# Sort ONLY by timestamp (important)
df = df.orderBy("timestamp")

# -------------------------------
# 4. Kafka Producer Setup
# -------------------------------
producer = KafkaProducer(
    bootstrap_servers="136.112.46.120:9092",
    value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
)

# -------------------------------
# 5. Send Data as Stream
# -------------------------------

# Use iterator (NOT collect) to avoid memory issues
for row in df.toLocalIterator():
    data = row.asDict()

    producer.send(
        "air-quality-stream",
        key=data["city"].encode("utf-8"),   # better partitioning
        value=data
    )

    # simulate real-time streaming
    time.sleep(0.05)

# Ensure all messages are sent
producer.flush()

print("Data successfully sent to Kafka!")

# -------------------------------
# 6. Stop Spark
# -------------------------------
spark.stop()
