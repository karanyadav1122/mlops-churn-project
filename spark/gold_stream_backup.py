import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

silver_path = os.path.join(BASE_DIR, "data","silver_stream")
gold_path = os.path.join(BASE_DIR, "data","gold_stream")
checkpoint_path = os.path.join(BASE_DIR,"checkpoints","gold_stream")

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
        .appName("GoldStream") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions","2") \
        .getOrCreate()
        
spark.sparkContext.setLogLevel("WARN") 

# reading silver data

df_silver = spark.readStream \
            .schema(schema) \
            .json(silver_path)
            
            
df_gold = df_silver \
          .withColumn(
            "is_payment_risky",
            when(col("late_payments") >= 3 ,1).otherwise(0)
          ) \
          .withColumn(
            "is_high_support",
            when(col("support_tickets") >= 7, 1).otherwise(0)
          ) \
          .withColumn(
            "tenure_bucket",
            when(col("tenure_months") < 6, "new")
            .when(col("tenure_months") < 24, "mid")
            .otherwise("loyal")
          )  \
          .withColumn(
            "charge_bucket",
            when(col("monthly_charges") < 50 ,"low")
            .when(col("monthly_charges") < 100, "medium")
            .otherwise("high")
          )  \
          .withColumn(
            "engagement_risk_score",
            col("late_payments") + col("support_tickets")
          )                         

query = df_gold.writeStream \
         .format("json") \
         .outputMode("append") \
         .option("path",gold_path) \
         .option("checkpointLocation", checkpoint_path) \
         .start()
         
print("Gold streaming started..") 
print(f"Reading from silver path : {silver_path}")
print(f"writing Gold data to: {gold_path}")

query.awaitTermination()                  