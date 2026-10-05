import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from util import (
    actions_last_7_days,
    add_login_date
)

# Spark Session
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder \
    .appName("PySpark Assignment Question 3") \
    .master("local[*]") \
    .getOrCreate()
# Data
data = [
    (1, 101, "login", "2023-09-05 08:30:00"),
    (2, 102, "click", "2023-09-06 12:45:00"),
    (3, 101, "click", "2023-09-07 14:15:00"),
    (4, 103, "login", "2023-09-08 09:00:00"),
    (5, 102, "logout", "2023-09-09 17:30:00"),
    (6, 101, "click", "2023-09-10 11:20:00"),
    (7, 103, "click", "2023-09-11 10:15:00"),
    (8, 102, "click", "2023-09-12 13:10:00")
]
# Custom Schema
schema = StructType([
    StructField("log_id", IntegerType(), True),
    StructField("user_id", IntegerType(), True),
    StructField("action", StringType(), True),
    StructField("time_stamp", StringType(), True)
])
# Create DataFrame
df = spark.createDataFrame(
    data,
    schema=schema
)
# Convert timestamp string to timestamp data type
from pyspark.sql.functions import to_timestamp

df = df.withColumn(
    "time_stamp",
    to_timestamp(
        col("time_stamp"),
        "yyyy-MM-dd HH:mm:ss"
    )
)

# Question 1
# Display DataFrame
print("Original DataFrame:")

df.show(truncate=False)


print("Original Schema:")

df.printSchema()
# Question 2
# Dynamic column names
column_names = [
    "log_id",
    "user_id",
    "user_activity",
    "time_stamp"
]

df = df.toDF(*column_names)
print("DataFrame after changing column names:")
df.show(truncate=False)
df.printSchema()
# Question 3
# Actions performed by each user
# in the last 7 days
print("Actions performed by each user in the last 7 days:")

last_7_days_df = actions_last_7_days(df)
last_7_days_df.show()
# Question 4
# Convert timestamp to login_date
print("DataFrame with login_date:")

login_date_df = add_login_date(df)

login_date_df.show(truncate=False)

login_date_df.printSchema()
# Question 5
# Write DataFrame as CSV
login_date_df.write \
    .mode("overwrite") \
    .option("header", True) \
    .option("delimiter", ",") \
    .option("quote", '"') \
    .option("escape", '"') \
    .csv("output/login_details_csv")
# Question 6
# Create managed table
# Database = user
# Table = login_details
spark.sql("""
CREATE DATABASE IF NOT EXISTS user
""")


login_date_df.write \
    .mode("overwrite") \
    .saveAsTable("user.login_details")


print("Managed table created successfully: user.login_details")
spark.stop()