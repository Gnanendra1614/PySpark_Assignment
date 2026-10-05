import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql.types import *

from util import (
    read_json,
    flatten_using_explode,
    flatten_using_explode_outer,
    flatten_using_posexplode,
    convert_columns_to_snake_case,
    add_load_date,
    add_date_columns
)
# Spark Session
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder \
    .appName("PySpark Assignment Question 4") \
    .master("local[*]") \
    .getOrCreate()
file_path = "data/employee.json"
employee_schema = StructType([
    StructField("id", StringType(), True),

    StructField(
        "employeeDetails",
        StructType([
            StructField("firstName", StringType(), True),
            StructField("lastName", StringType(), True),
            StructField("email", StringType(), True)
        ]),
        True
    ),

    StructField(
        "departments",
        ArrayType(
            StructType([
                StructField("departmentId", StringType(), True),
                StructField("departmentName", StringType(), True)
            ])
        ),
        True
    )
])

# Question 1
# Read JSON using dynamic function
employee_df = read_json(
    spark,
    file_path,
    employee_schema
)


print("Original DataFrame:")

employee_df.show(
    truncate=False
)


print("Original Schema:")

employee_df.printSchema()
# Question 2
# Flatten the DataFrame
flattened_df = employee_df.select(
    "id",
    "employeeDetails.*",
    "departments"
)


print("After Struct Flattening:")

flattened_df.show(
    truncate=False
)
# Question 3
# Record count before flattening
print(
    "Record count before flattening:",
    employee_df.count()
)
# Record count after explode
explode_df = flatten_using_explode(
    flattened_df,
    "departments"
)


print(
    "Record count after explode:",
    explode_df.count()
)
# Question 4
# Difference between explode,
# explode_outer and posexplode
print("Using explode:")

explode_df.show(
    truncate=False
)


print("Using explode_outer:")

explode_outer_df = flatten_using_explode_outer(
    flattened_df,
    "departments"
)

explode_outer_df.show(
    truncate=False
)


print("Using posexplode:")

posexplode_df = flattened_df.select(
    "id",
    "firstName",
    "lastName",
    posexplode(col("departments")).alias(
        "position",
        "department"
    )
)

posexplode_df.show(
    truncate=False
)
# Question 5
# Filter id = 0001
filtered_df = explode_df.filter(
    col("id") == "0001"
)
print("Records where id = 0001:")

filtered_df.show(
    truncate=False
)

# Convert camelCase to snake_case
snake_case_df = convert_columns_to_snake_case(
    explode_df
)


print("Columns after converting to snake_case:")

snake_case_df.show(
    truncate=False
)

snake_case_df.printSchema()
# Question 7
# Add load_date
load_date_df = add_load_date(
    snake_case_df
)
print("DataFrame with load_date:")

load_date_df.show(
    truncate=False
)
# Question 8
# Add year, month and day
final_df = add_date_columns(
    load_date_df
)
print("Final DataFrame:")

final_df.show(
    truncate=False
)
# Question 9
# Write as managed partitioned JSON table
spark.sql("""
CREATE DATABASE IF NOT EXISTS employee
""")


final_df.write \
    .mode("overwrite") \
    .format("json") \
    .partitionBy(
        "year",
        "month",
        "day"
    ) \
    .saveAsTable(
        "employee.employee_details"
    )


print(
    "Table created successfully: "
    "employee.employee_details"
)

spark.stop()