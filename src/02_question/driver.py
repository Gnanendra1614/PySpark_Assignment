import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from util import mask_card_udf

# Spark Session
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
spark = SparkSession.builder \
    .appName("PySpark Assignment Question 2") \
    .master("local[*]") \
    .getOrCreate()
# Card Number Data
card_data = [
    ("1234567891234567",),
    ("5678912345671234",),
    ("9123456712345678",),
    ("12345678123411122",),
    ("1234567812341342",)
]
# Custom Schema
card_schema = StructType([
    StructField("card_number", StringType(), True)
])
# Create DataFrame
credit_card_df = spark.createDataFrame(
    card_data,
    schema=card_schema
)
# Question 2.1
# Print DataFrame
print("Credit Card Data:")
credit_card_df.show(truncate=False)
# Question 2.2
# Print Original Number of Partitions
original_partitions = credit_card_df.rdd.getNumPartitions()

print(
    "Original number of partitions:",
    original_partitions
)
# Question 2.3
# Increase Partitions to 5
credit_card_df_5 = credit_card_df.repartition(5)

print(
    "Number of partitions after increasing to 5:",
    credit_card_df_5.rdd.getNumPartitions()
)
# Question 2.4
# Decrease Partitions Back to Original
credit_card_df_original = credit_card_df_5.coalesce(
    original_partitions
)

print(
    "Number of partitions after decreasing:",
    credit_card_df_original.rdd.getNumPartitions()
)
# Create masked_card_number column
result_df = credit_card_df.withColumn(
    "masked_card_number",
    mask_card_udf("card_number")
)
# Final Output
print("Final Output:")
result_df.select(
    "card_number",
    "masked_card_number"
).show(truncate=False)
# Stop Spark
spark.stop()