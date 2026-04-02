from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Analysis") \
    .master("local[*]")  \
    .getOrCreate()

df = spark.read.json("data/silver")

print("Schema:")
df.printSchema()

print("Sample Data:")
df.show(10)
