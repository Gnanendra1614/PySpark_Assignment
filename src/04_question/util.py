from pyspark.sql.functions import (
    col,
    explode,
    explode_outer,
    posexplode,
    current_date,
    year,
    month,
    dayofmonth,
    lit
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    ArrayType
)


# --------------------------------------------------
# 1. Read JSON dynamically
# --------------------------------------------------

def read_json(spark, file_path, schema):

    df = spark.read \
        .schema(schema) \
        .option("multiline", True) \
        .json(file_path)

    return df


# --------------------------------------------------
# 2. Flatten DataFrame using explode
# --------------------------------------------------

def flatten_using_explode(df, array_column):

    result_df = df.withColumn(
        array_column,
        explode(col(array_column))
    )

    return result_df


# --------------------------------------------------
# 3. Flatten DataFrame using explode_outer
# --------------------------------------------------

def flatten_using_explode_outer(df, array_column):

    result_df = df.withColumn(
        array_column,
        explode_outer(col(array_column))
    )

    return result_df


# --------------------------------------------------
# 4. Flatten DataFrame using posexplode
# --------------------------------------------------

def flatten_using_posexplode(df, array_column):

    result_df = df.withColumn(
        "position",
        posexplode(col(array_column)).pos
    )

    return result_df


# --------------------------------------------------
# 5. Convert camelCase to snake_case
# --------------------------------------------------

def camel_to_snake(name):

    result = ""

    for index, character in enumerate(name):

        if character.isupper() and index != 0:
            result += "_"

        result += character.lower()

    return result


# --------------------------------------------------
# Rename all columns dynamically
# --------------------------------------------------

def convert_columns_to_snake_case(df):

    for column_name in df.columns:

        new_column_name = camel_to_snake(column_name)

        df = df.withColumnRenamed(
            column_name,
            new_column_name
        )

    return df


# --------------------------------------------------
# 6. Add load_date
# --------------------------------------------------

def add_load_date(df):

    return df.withColumn(
        "load_date",
        current_date()
    )


# --------------------------------------------------
# 7. Add year, month and day
# --------------------------------------------------

def add_date_columns(df):

    return df \
        .withColumn("year", year(col("load_date"))) \
        .withColumn("month", month(col("load_date"))) \
        .withColumn("day", dayofmonth(col("load_date")))