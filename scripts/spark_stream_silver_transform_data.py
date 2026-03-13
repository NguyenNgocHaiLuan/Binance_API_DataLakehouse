from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, year, month, dayofmonth,
    when, round, log, lit, current_timestamp
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    DoubleType, LongType, BooleanType
)
import os
from dotenv import load_dotenv

load_dotenv()
MINIO_CONF = {
    "endpoint": os.getenv("MINIO_ENDPOINT"), 
    "access_key": os.getenv("MINIO_ACCESS_KEY"),
    "secret_key": os.getenv("MINIO_SECRET_KEY")
}

def create_spark_session():
    return SparkSession.builder \
        .appName("BinanceSilverTransform") \
        .config("spark.hadoop.fs.s3a.endpoint", MINIO_CONF["endpoint"]) \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_CONF["access_key"]) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_CONF["secret_key"]) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Silver Transform Job Started...")

    # Schema phải khớp với những gì Bronze ghi ra
    bronze_schema = StructType([
        StructField("source",         StringType(),  True),
        StructField("symbol",         StringType(),  True),
        StructField("price",          DoubleType(),  True),
        StructField("quantity",       DoubleType(),  True),
        StructField("trade_time",     LongType(),    True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingested_at",    StringType(),  True),
        StructField("year",           LongType(),    True),
        StructField("month",          LongType(),    True),
        StructField("day",            LongType(),    True),
        StructField("hour",           LongType(),    True),
    ])

    # 1. ĐỌC TỪ BRONZE — bắt buộc khai báo schema khi đọc streaming Parquet
    bronze_df = spark.readStream \
        .format("parquet") \
        .schema(bronze_schema) \
        .option("path", "s3a://bronze/crypto_trades/") \
        .load()

    # 2. CLEAN — Lọc bỏ data rác
    cleaned_df = bronze_df \
        .filter(col("price").isNotNull() & (col("price") > 0)) \
        .filter(col("quantity").isNotNull() & (col("quantity") > 0)) \
        .filter(col("symbol").isNotNull()) \
        .filter(col("trade_time").isNotNull()) \
        .filter(col("price") < 1_000_000)   # loại giá bất thường

    # 3. TRANSFORM — Chuẩn hóa kiểu dữ liệu
    transformed_df = cleaned_df \
        .withColumn("trade_timestamp", (col("trade_time") / 1000).cast("timestamp")) \
        .withColumn("ingest_timestamp", col("ingested_at").cast("timestamp")) \
        .withColumn("price",    round(col("price"), 8)) \
        .withColumn("quantity", round(col("quantity"), 8)) \
        .drop("trade_time", "ingested_at", "year", "month", "day", "hour")  # drop partition cũ, tính lại

    # 4. ENRICH — Tính thêm các trường phân tích
    enriched_df = transformed_df \
        .withColumn("trade_value", round(col("price") * col("quantity"), 8)) \
        .withColumn("trade_side",
            when(col("is_buyer_maker") == False, lit("BUY"))
            .otherwise(lit("SELL"))
        ) \
        .withColumn("price_magnitude", round(log(col("price")) / log(lit(10.0)), 2)) \
        .withColumn("is_large_trade",
            when(col("trade_value") > 10000, lit(True))
            .otherwise(lit(False))
        ) \
        .withColumn("processed_at", current_timestamp())

    # 5. DEDUPLICATE
    deduped_df = enriched_df \
        .withWatermark("trade_timestamp", "10 minutes") \
        .dropDuplicates(["symbol", "trade_timestamp", "price", "quantity"])

    # 6. Thêm lại partition columns từ trade_timestamp mới
    final_df = deduped_df \
        .withColumn("year",  year(col("trade_timestamp"))) \
        .withColumn("month", month(col("trade_timestamp"))) \
        .withColumn("day",   dayofmonth(col("trade_timestamp")))

    print("Silver Schema:")
    final_df.printSchema()

    # 7. GHI VÀO MINIO/SILVER
    query = final_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://silver/crypto_trades/") \
        .option("checkpointLocation", "s3a://silver/_checkpoints/crypto_trades_v4/") \
        .partitionBy("year", "month", "day") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()
    print("Silver transform complete — data written to MinIO/silver")

if __name__ == "__main__":
    main()