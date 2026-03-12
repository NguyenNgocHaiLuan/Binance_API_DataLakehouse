import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, year, month, dayofmonth, hour
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType

KAFKA_TOPIC = "crypto_trade_price_1"
MINIO_ACCESS_KEY = "minio_admin"
MINIO_SECRET_KEY = "minio_password"

def create_spark_session():
    return SparkSession.builder \
        .appName("CryptoBronzeIngestion") \
        .master("local[*]") \
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
        .config("spark.hadoop.fs.s3a.access.key", MINIO_ACCESS_KEY) \
        .config("spark.hadoop.fs.s3a.secret.key", MINIO_SECRET_KEY) \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
        .getOrCreate()

def main():
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")
    print("Spark Session created Successfully")

    schema = StructType([
        StructField("source", StringType(), True),
        StructField("symbol", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("quantity", DoubleType(), True),
        StructField("trade_time", LongType(), True),
        StructField("is_buyer_maker", BooleanType(), True),
        StructField("ingested_at", StringType(), True)
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "kafka:29092") \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    parsed_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data")
    ).select("data.*")

    final_df = parsed_df \
        .withColumn("timestamp", (col("trade_time") / 1000).cast("timestamp")) \
        .withColumn("year", year("timestamp")) \
        .withColumn("month", month("timestamp")) \
        .withColumn("day", dayofmonth("timestamp")) \
        .withColumn("hour", hour("timestamp"))

    # FIX 3: trigger(once=True) → xử lý batch hiện tại rồi tự thoát
    query = final_df.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", "s3a://bronze/crypto_trades/") \
        .option("checkpointLocation", "s3a://bronze/_checkpoints/crypto_trades/") \
        .partitionBy("year", "month", "day", "hour") \
        .trigger(once=True) \
        .start()

    query.awaitTermination()
    print("Bronze ingestion complete — data written to MinIO/bronze")

if __name__ == "__main__":
    main()