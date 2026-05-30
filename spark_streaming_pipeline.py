from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# -------------------------------
# 1. Spark Session
# -------------------------------
spark = SparkSession.builder \
    .appName("AirQualityStreaming") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

spark.conf.set("spark.sql.shuffle.partitions", "10")

# -------------------------------
# 2. Schema
# -------------------------------
schema = StructType() \
    .add("timestamp", StringType()) \
    .add("country", StringType()) \
    .add("city", StringType()) \
    .add("latitude", DoubleType()) \
    .add("longitude", DoubleType()) \
    .add("pm25", DoubleType()) \
    .add("pm10", DoubleType()) \
    .add("no2", DoubleType()) \
    .add("so2", DoubleType()) \
    .add("o3", DoubleType()) \
    .add("co", DoubleType()) \
    .add("aqi", DoubleType()) \
    .add("temperature", DoubleType()) \
    .add("humidity", DoubleType()) \
    .add("wind_speed", DoubleType())

# -------------------------------
# 3. Read from Kafka
# -------------------------------
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "136.112.46.120:9092") \
    .option("subscribe", "air-quality-stream") \
    .option("startingOffsets", "latest") \
    .option("maxOffsetsPerTrigger", "500") \
    .load()

# -------------------------------
# 4. Parse JSON
# -------------------------------
df = df.selectExpr("CAST(value AS STRING)")

df = df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

# -------------------------------
# 5. Clean + timestamp
# -------------------------------
df = df.withColumn("event_time", to_timestamp("timestamp"))

df = df.dropna(subset=["city", "event_time", "aqi"])

# -------------------------------
# 6. Watermark
# -------------------------------
df = df.withWatermark("event_time", "5 minutes")

# -------------------------------
# 7. Compute V2
# -------------------------------
df = df.withColumn(
    "pollutant_max",
    greatest("pm25", "pm10", "no2", "so2", "o3", "co")
)

# -------------------------------
# 8. Window aggregation (FINAL)
# -------------------------------
agg = df.groupBy(
    window(col("event_time"), "8 hours", "1 hour"),
    col("city")
).agg(
    avg("aqi").alias("V1"),
    max("pollutant_max").alias("V2")
)

# -------------------------------
# 9. Ranking
# -------------------------------
def process_batch(batch_df, batch_id):
    if batch_df.rdd.isEmpty():
        return

    window_spec = Window.partitionBy("window").orderBy(
        col("V1").asc(),
        col("V2").asc()
    )

    ranked = batch_df.withColumn(
        "rank",
        row_number().over(window_spec)
    )

    ranked.select(
        col("window.start").alias("start"),
        col("window.end").alias("end"),
        col("city"),
        col("V1"),
        col("V2"),
        col("rank")
    ).orderBy("start", "rank").show(truncate=False)

# -------------------------------
# 10. Start Streaming
# -------------------------------
query = agg.writeStream \
    .outputMode("update") \
    .option("checkpointLocation", "/tmp/air-quality-checkpoint") \
    .foreachBatch(process_batch) \
    .start()

query.awaitTermination()
