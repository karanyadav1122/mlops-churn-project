from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField ,StringType, IntegerType, DoubleType
from pyspark.sql.functions import from_json, col

spark = SparkSession.builder \
    .appName("ChurnSilverTransform") \
    .master("local[*]") \
    .config("spark.sql.shuffle.partitions","2") \
    .getOrCreate()
    
spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("age", IntegerType(), True),
    StructField("gender", StringType(), True),
    StructField("location", StringType(), True),
    StructField("subscription_type", StringType(), True),
    StructField("monthly_charges", DoubleType(), True),
    StructField("tenure_months", IntegerType(), True),
    StructField("support_tickets", IntegerType(), True),
    StructField("late_payments", IntegerType(), True),
    StructField("timestamp", StringType(), True)
])

df_bronze = spark.readStream \
          .format('json') \
          .schema(StructType([StructField("raw_json",StringType(),True)])) \
          .load("data/bronze")
          
df_silver = df_bronze.select(
    from_json(col("raw_json"), schema).alias("data")
).select("data.*")

query = df_silver.writeStream \
        .format("json") \
        .outputMode("append") \
        .option("path","data/silver") \
        .option("checkpointLocation","data/checkpoints/silver") \
        .start()
        
query.awaitTermination()                          