import os
import sys
import pytest

from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import *


# ----------------------------
# Add Question_3 to Python Path
# ----------------------------
sys.path.insert(
    0,
    os.path.abspath(
        os.path.join(
            os.path.dirname(__file__),
            "../../src/Question_3"
        )
    )
)

from util import (
    actions_last_7_days,
    add_login_date
)


# ----------------------------
# Fixture: Spark Session Setup
# ----------------------------
@pytest.fixture(scope="session")
def spark():

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = SparkSession.builder \
        .appName("Assignment Three Test") \
        .master("local[*]") \
        .getOrCreate()

    yield spark

    spark.stop()


# ----------------------------
# Fixture: Login DataFrame
# ----------------------------
@pytest.fixture(scope="session")
def login_data_df(spark):

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

    schema = StructType([
        StructField("log_id", IntegerType(), True),
        StructField("user_id", IntegerType(), True),
        StructField("action", StringType(), True),
        StructField("time_stamp", StringType(), True)
    ])

    df = spark.createDataFrame(
        data,
        schema
    )

    return df.withColumn(
        "time_stamp",
        f.to_timestamp(
            "time_stamp",
            "yyyy-MM-dd HH:mm:ss"
        )
    )


# ----------------------------
# Test: Dynamic column names
# ----------------------------
def test_dynamic_column_names(
    login_data_df
):

    columns = [
        "log_id",
        "user_id",
        "user_activity",
        "time_stamp"
    ]

    result = login_data_df.toDF(
        *columns
    )

    print("\nDynamic column names:")
    print(result.columns)

    assert result.columns == columns


# ----------------------------
# Test: Actions performed
# ----------------------------
def test_actions_last_7_days(
    login_data_df
):

    result = actions_last_7_days(
        login_data_df
    )

    print("\nActions performed:")
    result.show()

    assert result.count() > 0


# ----------------------------
# Test: Login date
# ----------------------------
def test_add_login_date(
    login_data_df
):

    result = add_login_date(
        login_data_df
    )

    print("\nLogin date:")
    result.show()

    assert "login_date" in result.columns

    assert result.schema[
        "login_date"
    ].dataType == DateType()