from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, max, min, first, last, sum, count, expr, current_timestamp
from pyspark.sql.types import StructType, StructField, LongType, StringType, DoubleType, TimestampType, BooleanType

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
        .appName("BinanceGoldAggregation") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def write_to_postgres(df, epoch_id):
    print(f"Writing batch {epoch_id} to PostgreSQL...")
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
        print(f"Batch {epoch_id} written successfully")
    except Exception as e:
        print(f"Error writing batch {epoch_id}: {e}")

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Gold Aggregation Job Started...")

    silver_schema = StructType([
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_timestamp", TimestampType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingested_at",    StringType(),  True),
        StructField("year",           LongType(),    True),
        StructField("month",          LongType(),    True),
        StructField("day",            LongType(),    True),
        StructField("hour",           LongType(),    True),
    ])

    # Đọc từ Silver Parquet
    silver_df = spark.readStream \
        .format("parquet") \
        .schema(silver_schema) \
        .option("path", "s3a://silver/crypto_trades/") \
        .load()

    # Aggregate: tính nến 1 phút
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
    
    # syntax thiếu newline giữa .option() và .trigger()
    query = gold_df.writeStream \
        .outputMode("append") \
        .foreachBatch(write_to_postgres) \
        .option("checkpointLocation", "s3a://gold/_checkpoints/fact_candles/") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()
    print("Gold aggregation complete — data written to PostgreSQL")

if __name__ == "__main__":
    main()