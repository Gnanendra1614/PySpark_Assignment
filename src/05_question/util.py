from pyspark.sql.functions import (
    avg,
    col,
    lit,
    lower,
    current_date
)


# --------------------------------------------------
# Question 2
# Find average salary of each department
# --------------------------------------------------

def average_salary_by_department(employee_df, department_df):

    result_df = (
        employee_df
        .join(
            department_df,
            employee_df.department == department_df.dept_id,
            "inner"
        )
        .groupBy("dept_id", "dept_name")
        .agg(
            avg("salary").alias("average_salary")
        )
    )

    return result_df


# --------------------------------------------------
# Question 3
# Find employee name and department name
# whose employee name starts with 'm'
# --------------------------------------------------

def employees_starting_with_m(employee_df, department_df):

    result_df = (
        employee_df
        .join(
            department_df,
            employee_df.department == department_df.dept_id,
            "inner"
        )
        .filter(
            lower(col("employee_name")).startswith("m")
        )
        .select(
            "employee_name",
            "dept_name"
        )
    )

    return result_df


# --------------------------------------------------
# Question 4
# Create bonus column
# bonus = salary * 2
# --------------------------------------------------

def add_bonus_column(employee_df):

    result_df = employee_df.withColumn(
        "bonus",
        col("salary") * 2
    )

    return result_df


# --------------------------------------------------
# Question 5
# Reorder employee columns
# --------------------------------------------------

def reorder_employee_columns(employee_df):

    result_df = employee_df.select(
        "employee_id",
        "employee_name",
        "salary",
        "State",
        "Age",
        "department"
    )

    return result_df


# --------------------------------------------------
# Question 6
# Dynamic joins
# --------------------------------------------------

def perform_join(
    employee_df,
    department_df,
    join_type
):

    result_df = employee_df.join(
        department_df,
        employee_df.department == department_df.dept_id,
        join_type
    )

    return result_df


# --------------------------------------------------
# Question 7
# Replace State with country_name
# --------------------------------------------------

def add_country_name(
    employee_df,
    country_df
):

    result_df = (
        employee_df
        .join(
            country_df,
            employee_df.State == country_df.country_code,
            "left"
        )
        .drop("State")
        .withColumnRenamed(
            "country_name",
            "country_name"
        )
    )

    return result_df


# --------------------------------------------------
# Question 8
# Convert all column names to lowercase
# dynamically
# --------------------------------------------------

def convert_columns_to_lowercase(df):

    for column_name in df.columns:

        df = df.withColumnRenamed(
            column_name,
            column_name.lower()
        )

    return df


# --------------------------------------------------
# Question 8
# Add load_date
# --------------------------------------------------

def add_load_date(df):

    result_df = df.withColumn(
        "load_date",
        current_date()
    )

    return result_df