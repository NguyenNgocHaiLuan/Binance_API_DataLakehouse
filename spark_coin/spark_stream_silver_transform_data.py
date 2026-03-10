import time
import os
from pyspark.sql import SparkSession
# Thêm các hàm cần thiết để xử lý Kafka và thời gian
from pyspark.sql.functions import col, from_json, to_timestamp, current_timestamp, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

def create_spark_session():
    return SparkSession.builder \
        .appName("BinanceSilverStreaming") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", "minio_admin") \
        .config("spark.hadoop.fs.s3a.secret.key", "minio_password") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    sc = spark.sparkContext
    sc.setLogLevel("WARN")
    
    print("Silver Streaming Job Started...")

    # 1. Định nghĩa Schema khớp với dữ liệu JSON bắn từ Producer
    kafka_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True), 
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingested_at", StringType(), True)
    ])

    # 2. ĐỌC TỪ KAFKA (Thay vì đọc file Parquet Bronze)
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "crypto_trade_price_1") \
        .option("startingOffsets", "earliest") \
        .option("failOnDataLoss", "true") \
        .load()

    # 3. Parse JSON và Chuyển đổi dữ liệu
    silver_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_string") \
        .select(from_json(col("json_string"), kafka_schema).alias("data")) \
        .select("data.*") \
        .withColumn("trade_timestamp", (col("trade_time") / 1000).cast("timestamp")) \
        .withColumn("ingest_timestamp", col("ingested_at").cast("timestamp")) \
        .withColumn("year", year(col("trade_timestamp"))) \
        .withColumn("month", month(col("trade_timestamp"))) \
        .withColumn("day", dayofmonth(col("trade_timestamp"))) \
        .drop("trade_time", "ingested_at", "json_string") \
        .withWatermark("trade_timestamp", "10 minutes") \
        .dropDuplicates(["symbol", "trade_timestamp"])
    
    print("Silver Schema Transformed:")
    silver_df.printSchema()

    # 4. GHI VÀO MINIO (SILVER LAYER)
    # Dữ liệu sẽ được lưu theo cấu trúc: silver/crypto_trades/year=2024/month=10/day=25/...
    query = silver_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://silver/crypto_trades/") \
        .option("checkpointLocation", "s3a://silver/_checkpoints/crypto_trades_kafka_v14/") \
        .partitionBy("year", "month", "day") \
        .trigger(processingTime="1 minute") \
        .start()
    
    print("Streaming is running and Writing to MinIO/silver...")
    query.awaitTermination()

if __name__ == "__main__":
    main()