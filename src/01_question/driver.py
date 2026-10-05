import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from util import (
    customers_only_iphone13,
    customers_upgraded_iphone13_to_iphone14,
    customers_bought_all_products
)
# Spark session
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
spark = SparkSession.builder \
    .appName("PySpark Assignment") \
    .master("local[*]") \
    .getOrCreate()
# Purchase Data
purchase = [
    (1, "iphone13"),
    (1, "dell i5 core"),
    (2, "iphone13"),
    (2, "dell i5 core"),
    (3, "iphone13"),
    (3, "dell i5 core"),
    (1, "dell i3 core"),
    (1, "hp i5 core"),
    (1, "iphone14"),
    (3, "iphone14"),
    (4, "iphone13"),
    (4, "iphone13")
]
# Purchase Data Schema
purchase_schema = StructType([
    StructField("customer", IntegerType(), True),
    StructField("product_model", StringType(), True)
])
# Create Purchase DataFrame
purchase_data_df = spark.createDataFrame(
    purchase,
    schema=purchase_schema
)
# Product Data
product = [
    ("iphone13",),
    ("dell i5 core",),
    ("dell i3 core",),
    ("hp i5 core",),
    ("iphone14",)
]
# Product Data Schema
product_schema = StructType([
    StructField("product_model", StringType(), True)
])
# Create Product DataFrame
product_data_df = spark.createDataFrame(
    product,
    schema=product_schema
)
# Display Data
print("Purchase Data:")
purchase_data_df.show()
print("Purchase Data Schema:")
purchase_data_df.printSchema()
print("Product Data:")
product_data_df.show()
print("Product Data Schema:")
product_data_df.printSchema()
# Question 1.2
# Customers who bought only iphone13
print("Customers who bought only iphone13:")

customers_only_iphone13(
    purchase_data_df
).show()
# Question 1.3
# Customers who upgraded from iphone13 to iphone14
print("Customers who upgraded from iphone13 to iphone14:")
customers_upgraded_iphone13_to_iphone14(
    purchase_data_df
).show()
# Question 1.4
# Customers who bought all products
print("Customers who bought all products:")
customers_bought_all_products(
    purchase_data_df,
    product_data_df
).show()
# Stop Spark
spark.stop()