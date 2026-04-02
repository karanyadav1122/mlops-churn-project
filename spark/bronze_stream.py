from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("ChurnBronzeStream") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

df_kafka = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "churn_events") \
    .option("startingOffsets", "earliest") \
    .load()

df_raw = df_kafka.selectExpr("CAST(value AS STRING) as raw_json")

query = df_raw.writeStream \
    .format("json") \
    .outputMode("append") \
    .option("path", "data/bronze") \
    .option("checkpointLocation", "checkpoints/bronze") \
    .start()

query.awaitTermination()
