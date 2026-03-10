from pyspark.sql.functions import col, window, max, min, first, last, sum, count, expr, current_timestamp, from_json
import logging
import time
from pyspark.sql import SparkSession
# THÊM IMPORT LongType để xử lý timestamp mili-giây từ Kafka
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType, LongType

# Cấu hình MinIO (Vẫn giữ để lưu Checkpoint)
MINIO_CONF = {
    "endpoint": "http://minio:9000",
    "access_key": "minio_admin",
    "secret_key": "minio_password"
}

DB_URL = "jdbc:postgresql://postgres:5432/warehouse_db"
DB_PROPERTIES = {
    "user": "admin",
    "password": "adminpassword",
    "driver": "org.postgresql.Driver"
}

def create_spark_session():
    return SparkSession.builder \
        .appName("BinanceGoldSpeedLayer") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def write_to_postgres(df, epoch_id):
    print(f"Writing batch {epoch_id} to PostgreSQL")
    try:
        df.write \
            .mode("append") \
            .format("jdbc") \
            .option("url", DB_URL) \
            .option("dbtable", "fact_market_candles") \
            .option("user", DB_PROPERTIES["user"]) \
            .option("password", DB_PROPERTIES["password"]) \
            .option("driver", DB_PROPERTIES["driver"]) \
            .save()
        print(f"Batch {epoch_id} written to PostgreSQL successfully")
    except Exception as e:
        print(f"Error writting batch {epoch_id}: {e}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Speed Layer Started (Kafka -> Postgres)")

    # 1. ĐỊNH NGHĨA SCHEMA JSON (Khớp với Producer gửi)
    kafka_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True),      # Quan trọng: LongType
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingested_at", StringType(), True)
    ])

    # 2. ĐỌC TỪ KAFKA (Thay vì Parquet)
    raw_kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", "crypto_trade_price_1") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "true") \
        .load()

    # 3. GIẢI MÃ JSON & CHUYỂN ĐỔI THỜI GIAN
    silver_df = raw_kafka_df.selectExpr("CAST(value AS STRING) as json_str") \
        .select(from_json(col("json_str"), kafka_schema).alias("data")) \
        .select("data.*") \
        .withColumn("trade_timestamp", (col("trade_time") / 1000).cast(TimestampType())) \
        .withColumn("ingest_timestamp", col("ingested_at").cast(TimestampType()))

    # 4. TÍNH TOÁN NẾN (Giữ nguyên logic cực xịn của bạn)
    gold_df = silver_df \
        .withWatermark("trade_timestamp", "10 minutes") \
        .groupBy(
            window(col("trade_timestamp"), "1 minute"),
            col("symbol")
        ) \
        .agg(
            first("price", ignorenulls=True).alias("open_price"),
            max("price").alias("high_price"),
            min("price").alias("low_price"),
            last("price", ignorenulls=True).alias("close_price"),
            sum("quantity").alias("total_volume"),
            count("*").alias("trade_count"),
            sum(expr("CASE WHEN is_buyer_maker = false THEN quantity ELSE 0 END")).alias("buy_volume_taker"),
            sum(expr("CASE WHEN is_buyer_maker = true THEN quantity ELSE 0 END")).alias("sell_volume_maker")
        ) \
        .select(
            col("window.start").alias("candle_start_time"),
            col("symbol"),
            col("open_price"), col("high_price"), col("low_price"), col("close_price"),
            col("total_volume"), col("buy_volume_taker"), col("sell_volume_maker"), col("trade_count"),
            current_timestamp().alias("ingested_at")
        )
    
    # 5. GHI VÀO POSTGRES
    # LƯU Ý: Đổi tên checkpoint để không bị lỗi xung đột với code cũ
    query = gold_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "s3a://silver/_checkpoints/fact_candles_realtime_v4/")        .trigger(processingTime="1 minute") \
        .start()
    
    print("Streaming to Gold layer is running...")
    query.awaitTermination()

if __name__ == "__main__":
    main()