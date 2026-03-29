import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType


BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

bronze_path = os.path.join(BASE_DIR,"data","bronze_stream")
checkpoint_path = os.path.join(BASE_DIR,"checkpoints","bronze_kafka")

schema = StructType() \
         .add("gender",StringType()) \
         .add("location", StringType()) \
         .add("subscription_type", StringType()) \
         .add("tenure_months", IntegerType()) \
         .add("monthly_charges", DoubleType()) \
         .add("support_tickets", IntegerType()) \
         .add("late_payments", IntegerType()) \
         .add("tenure_bucket", StringType()) \
         .add("charge_bucket", StringType())
         
spark = SparkSession.builder \
        .appName("BronzekafkaStream") \
        .master("local[*]") \
        .config(
              "spark.jars.packages",
              "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
        )   \
        .getOrCreate()                             

spark.sparkContext.setLogLevel("WARN")


# reading stream from kafka
df_kafka = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "churn_input") \
            .option("startingOffsets", "latest")    \
            .load()


# now, converting kafka value from binary to string and parsing json

df_parsed = df_kafka.selectExpr("CAST(value AS STRING) as json_string")  \
            .select(from_json(col("json_string"), schema).alias("data")) \
            .select("data.*") \
            .withColumn("ingestion_time", current_timestamp())  
            
            
# writing parsed stream to bronze layer as JSON files

query = df_parsed.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", bronze_path) \
        .option("checkpointLocation", checkpoint_path) \
        .start()
        
        
print("Bronze kafka streaming started..")
print(f"writing bronze data to : {bronze_path}")

query.awaitTermination()                                            