import os
import sys
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *


# ----------------------------
# Add Question_4 to Python Path
# ----------------------------
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../src/Question_4"
        )
    )
)

from util import (
    flatten_using_explode,
    flatten_using_explode_outer,
    flatten_using_posexplode,
    camel_to_snake,
    convert_columns_to_snake_case,
    add_load_date,
    add_date_columns
)


# ----------------------------
# Fixture: Spark Session Setup
# ----------------------------
@pytest.fixture(scope="session")
def spark():

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder \
        .appName("Assignment Four Test") \
        .master("local[*]") \
        .getOrCreate()

    yield spark

    spark.stop()


# ----------------------------
# Fixture: Employee DataFrame
# ----------------------------
@pytest.fixture(scope="session")
def employee_df(spark):

    data = [
        (
            "0001",
            "James",
            ["sales", "finance"]
        ),
        (
            "0002",
            "Michel",
            ["sales"]
        ),
        (
            "0003",
            "Robert",
            []
        )
    ]

    schema = StructType([
        StructField("employeeId", StringType(), True),
        StructField("employeeName", StringType(), True),
        StructField(
            "departments",
            ArrayType(StringType()),
            True
        )
    ])

    return spark.createDataFrame(
        data,
        schema
    )


# ----------------------------
# Test: explode
# ----------------------------
def test_flatten_using_explode(
    employee_df
):

    result = flatten_using_explode(
        employee_df,
        "departments"
    )

    print("\nExplode:")
    result.show()

    assert result.count() == 3


# ----------------------------
# Test: explode_outer
# ----------------------------
def test_flatten_using_explode_outer(
    employee_df
):

    result = flatten_using_explode_outer(
        employee_df,
        "departments"
    )

    print("\nExplode Outer:")
    result.show()

    assert result.count() == 4


# ----------------------------
# Test: posexplode
# ----------------------------
def test_flatten_using_posexplode(
    employee_df
):

    result = flatten_using_posexplode(
        employee_df,
        "departments"
    )

    print("\nPosexplode:")
    result.show()

    assert result.count() == 3


# ----------------------------
# Test: Filter ID = 0001
# ----------------------------
def test_filter_id(
    employee_df
):

    result = employee_df.filter(
        f.col("employeeId") == "0001"
    )

    assert result.count() == 1


# ----------------------------
# Test: Camel case
# ----------------------------
def test_camel_to_snake():

    assert camel_to_snake(
        "employeeId"
    ) == "employee_id"

    assert camel_to_snake(
        "employeeName"
    ) == "employee_name"


# ----------------------------
# Test: Snake case columns
# ----------------------------
def test_convert_columns_to_snake_case(
    employee_df
):

    result = convert_columns_to_snake_case(
        employee_df
    )

    print("\nSnake case columns:")
    print(result.columns)

    assert result.columns == [
        "employee_id",
        "employee_name",
        "departments"
    ]


# ----------------------------
# Test: Load date
# ----------------------------
def test_add_load_date(
    employee_df
):

    result = add_load_date(
        employee_df
    )

    assert "load_date" in result.columns

    assert result.schema[
        "load_date"
    ].dataType == DateType()


# ----------------------------
# Test: Year Month Day
# ----------------------------
def test_add_date_columns(
    employee_df
):

    df = add_load_date(
        employee_df
    )

    result = add_date_columns(
        df
    )

    assert "year" in result.columns
    assert "month" in result.columns
    assert "day" in result.columns