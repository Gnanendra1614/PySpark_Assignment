from pyspark.sql.functions import (
    col,
    collect_set,
    size,
    array_contains,
    countDistinct
)


# 1. Find customers who bought only iphone13
def customers_only_iphone13(purchase_data_df):

    result_df = (
        purchase_data_df
        .groupBy("customer")
        .agg(
            collect_set("product_model").alias("products")
        )
        .filter(
            (size(col("products")) == 1) &
            array_contains(col("products"), "iphone13")
        )
        .select("customer")
    )

    return result_df


# 2. Find customers who upgraded from iphone13 to iphone14
def customers_upgraded_iphone13_to_iphone14(purchase_data_df):

    result_df = (
        purchase_data_df
        .groupBy("customer")
        .agg(
            collect_set("product_model").alias("products")
        )
        .filter(
            array_contains(col("products"), "iphone13") &
            array_contains(col("products"), "iphone14")
        )
        .select("customer")
    )

    return result_df


# 3. Find customers who bought all products
#    available in product_data_df
def customers_bought_all_products(
    purchase_data_df,
    product_data_df
):

    total_products = product_data_df.select(
        countDistinct("product_model")
    ).collect()[0][0]

    result_df = (
        purchase_data_df
        .join(
            product_data_df,
            on="product_model",
            how="inner"
        )
        .groupBy("customer")
        .agg(
            countDistinct("product_model").alias("product_count")
        )
        .filter(
            col("product_count") == total_products
        )
        .select("customer")
    )

    return result_df