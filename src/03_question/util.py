from pyspark.sql.functions import (
    col,
    count,
    current_timestamp,
    date_sub,
    to_date,
    date_format
)


# --------------------------------------------------
# Question 3
# Calculate number of actions performed
# by each user in the last 7 days
# --------------------------------------------------

def actions_last_7_days(df):

    result_df = (
        df
        .filter(
            col("time_stamp") >=
            date_sub(current_timestamp(), 7)
        )
        .groupBy("user_id")
        .agg(
            count("*").alias("action_count")
        )
    )

    return result_df


# --------------------------------------------------
# Question 4
# Convert timestamp to login_date
# --------------------------------------------------

def add_login_date(df):

    result_df = df.withColumn(
        "login_date",
        to_date(col("time_stamp"))
    )

    return result_df