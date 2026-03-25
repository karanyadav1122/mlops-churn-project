from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

spark = SparkSession.builder \
        .appName("ChurnGoldFeatures") \
        .master("local[*]") \
        .config("spark.sql.shuffle.partitions","2") \
        .getOrCreate()
        
spark.sparkContext.setLogLevel("WARN")

df_silver = spark.read.json("data/silver")
            
            
df_gold = df_silver \
          .withColumn(
            "is_payment_risky",
            when(col("late_payments") >= 3,1).otherwise(0)
          ) \
          .withColumn(
            "is_high_support",
            when(col("support_tickets") >= 7,1).otherwise(0)
          )  \
          .withColumn(
            "tenure_bucket",
            when(col("tenure_months") < 6,"new")
            .when(col("tenure_months") < 24,'mid')
            .otherwise('loyal')
          )  \
          .withColumn(
            "charge_bucket",
            when(col("monthly_charges") < 50, "low")
            .when(col("monthly_charges") < 100, "medium")
            .otherwise("high")
          )  \
          .withColumn(
            "engagement_risk_score",
            col("late_payments") + col("support_tickets")
          )  

df_gold.write.mode("overwrite").json("data/gold")