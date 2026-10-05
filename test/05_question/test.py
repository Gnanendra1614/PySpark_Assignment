import os
import sys
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *


# ----------------------------
# Add Question_5 to Python Path
# ----------------------------
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../src/Question_5"
        )
    )
)

from util import (
    average_salary_by_department,
    employees_starting_with_m,
    add_bonus_column,
    reorder_employee_columns,
    perform_join,
    add_country_name,
    convert_columns_to_lowercase,
    add_load_date
)


# ----------------------------
# Fixture: Spark Session Setup
# ----------------------------
@pytest.fixture(scope="session")
def spark():

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder \
        .appName("Assignment Five Test") \
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
        (11, "james", "D101", "ny", 9000, 34),
        (12, "michel", "D101", "ny", 8900, 32),
        (13, "robert", "D102", "ca", 7900, 29),
        (14, "scott", "D103", "ca", 8000, 36),
        (15, "jen", "D102", "ny", 9500, 38),
        (16, "jeff", "D103", "uk", 9100, 35),
        (17, "maria", "D101", "ny", 7900, 40)
    ]

    schema = StructType([
        StructField("employee_id", IntegerType(), True),
        StructField("employee_name", StringType(), True),
        StructField("department", StringType(), True),
        StructField("State", StringType(), True),
        StructField("salary", IntegerType(), True),
        StructField("Age", IntegerType(), True)
    ])

    return spark.createDataFrame(
        data,
        schema
    )


# ----------------------------
# Fixture: Department DataFrame
# ----------------------------
@pytest.fixture(scope="session")
def department_df(spark):

    data = [
        ("D101", "sales"),
        ("D102", "finance"),
        ("D103", "marketing"),
        ("D104", "hr"),
        ("D105", "support")
    ]

    schema = StructType([
        StructField("dept_id", StringType(), True),
        StructField("dept_name", StringType(), True)
    ])

    return spark.createDataFrame(
        data,
        schema
    )


# ----------------------------
# Fixture: Country DataFrame
# ----------------------------
@pytest.fixture(scope="session")
def country_df(spark):

    data = [
        ("ny", "newyork"),
        ("ca", "California"),
        ("uk", "Russia")
    ]

    schema = StructType([
        StructField("country_code", StringType(), True),
        StructField("country_name", StringType(), True)
    ])

    return spark.createDataFrame(
        data,
        schema
    )


# ----------------------------
# Test: Average salary
# ----------------------------
def test_average_salary(
    employee_df,
    department_df
):

    result = average_salary_by_department(
        employee_df,
        department_df
    )

    print("\nAverage Salary:")
    result.show()

    assert result.count() == 3


# ----------------------------
# Test: Names starting with m
# ----------------------------
def test_employees_starting_with_m(
    employee_df,
    department_df
):

    result = employees_starting_with_m(
        employee_df,
        department_df
    )

    print("\nEmployees starting with m:")
    result.show()

    names = sorted([
        row["employee_name"]
        for row in result.collect()
    ])

    assert names == [
        "maria",
        "michel"
    ]


# ----------------------------
# Test: Bonus
# ----------------------------
def test_bonus(
    employee_df
):

    result = add_bonus_column(
        employee_df
    )

    print("\nBonus:")
    result.show()

    assert "bonus" in result.columns

    james = result.filter(
        result.employee_id == 11
    ).first()

    assert james["bonus"] == 18000


# ----------------------------
# Test: Reorder columns
# ----------------------------
def test_reorder_columns(
    employee_df
):

    result = reorder_employee_columns(
        employee_df
    )

    print("\nReordered columns:")
    print(result.columns)

    assert result.columns == [
        "employee_id",
        "employee_name",
        "salary",
        "State",
        "Age",
        "department"
    ]


# ----------------------------
# Test: Inner join
# ----------------------------
def test_inner_join(
    employee_df,
    department_df
):

    result = perform_join(
        employee_df,
        department_df,
        "inner"
    )

    print("\nInner Join:")
    result.show()

    assert result.count() == 7


# ----------------------------
# Test: Left join
# ----------------------------
def test_left_join(
    employee_df,
    department_df
):

    result = perform_join(
        employee_df,
        department_df,
        "left"
    )

    print("\nLeft Join:")
    result.show()

    assert result.count() == 7


# ----------------------------
# Test: Right join
# ----------------------------
def test_right_join(
    employee_df,
    department_df
):

    result = perform_join(
        employee_df,
        department_df,
        "right"
    )

    print("\nRight Join:")
    result.show()

    assert result.count() == 9


# ----------------------------
# Test: Country name
# ----------------------------
def test_country_name(
    employee_df,
    country_df
):

    result = add_country_name(
        employee_df,
        country_df
    )

    print("\nCountry Name:")
    result.show()

    assert "country_name" in result.columns


# ----------------------------
# Test: Lowercase columns
# ----------------------------
def test_lowercase_columns(
    employee_df,
    country_df
):

    df = add_country_name(
        employee_df,
        country_df
    )

    result = convert_columns_to_lowercase(
        df
    )

    print("\nLowercase columns:")
    print(result.columns)

    for column in result.columns:
        assert column == column.lower()


# ----------------------------
# Test: Load date
# ----------------------------
def test_load_date(
    employee_df
):

    result = add_load_date(
        employee_df
    )

    print("\nLoad Date:")
    result.show()

    assert "load_date" in result.columns

    assert result.schema[
        "load_date"
    ].dataType == DateType()