import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

bronze_path = os.path.join(BASE_DIR,"data","bronze_stream")
silver_path = os.path.join(BASE_DIR, "data", "silver_stream")
checkpoint_path = os.path.join(BASE_DIR,"checkpoints","silver_stream")

schema = StructType() \
    .add("gender", StringType()) \
    .add("location", StringType()) \
    .add("subscription_type", StringType()) \
    .add("tenure_months", IntegerType()) \
    .add("monthly_charges", DoubleType()) \
    .add("support_tickets", IntegerType()) \
    .add("late_payments", IntegerType()) \
    .add("tenure_bucket", StringType()) \
    .add("charge_bucket", StringType()) \
    .add("ingestion_time", TimestampType())
    
spark = SparkSession.builder \
        .appName("SilverStream") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions","2") \
        .getOrCreate()
        
spark.sparkContext.setLogLevel("WARN")  


# reading bronze data as a stream

df_bronze = spark.readStream \
            .schema(schema) \
            .json(bronze_path)
            
# silver layer

df_silver = df_bronze.select(
  col("gender"),
    col("location"),
    col("subscription_type"),
    col("tenure_months"),
    col("monthly_charges"),
    col("support_tickets"),
    col("late_payments"),
    col("tenure_bucket"),
    col("charge_bucket"),
    col("ingestion_time")
)        


# writing silver stream
query = df_silver.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path", silver_path) \
        .option("checkpointLocation", checkpoint_path) \
        .start()
        
print("silver streaming started..") 
print(f"Reading from bronze path : {bronze_path}")
print(f"Writing Silver data to: {silver_path}")

query.awaitTermination()                                    