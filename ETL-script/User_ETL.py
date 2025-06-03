from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, split, trim, explode, to_date, datediff,
    current_date, round
)
from functools import reduce


def load_user_data() -> DataFrame:
    return spark.read.table("aything.default.user_ext")

def filter_user_data(df: DataFrame) -> DataFrame:
    return df.filter((col("useful") > 5) & (col("review_count") > 10) & (col("fans") > 10))

def add_yelping_since_date(df: DataFrame) -> DataFrame:
    return df.withColumn("yelping_since_date", to_date(col("yelping_since")))

def calculate_total_compliments(df: DataFrame) -> DataFrame:
    compliment_cols = [c for c in df.columns if c.startswith("compliment_")]
    total_expr = reduce(lambda a, b: a + b, [col(c) for c in compliment_cols])
    return df.withColumn("total_compliments", total_expr)

def calculate_years_on_platform(df: DataFrame) -> DataFrame:
    return df.withColumn(
        "years_on_platform",
        round(datediff(current_date(), col("yelping_since_date")) / 365.0, 2)
    )

def drop_unused_columns(df: DataFrame) -> DataFrame:
    for c in df.columns:
        if c.startswith("compliment_"):
            df = df.drop(c)
    return df.drop("name", "yelping_since", "elite")

def explode_friends(df: DataFrame) -> DataFrame:
    df = df.withColumn("friend_array", split(col("friends"), ","))
    df = df.withColumn("friend_id", explode(col("friend_array")))
    df = df.withColumn("friend_id", trim(col("friend_id")))
    df = df.drop("friends", "friend_array")
    return df

# Removed drop_self_friendship based on instruction

def user_etl_pipeline() -> DataFrame:
    df = load_user_data()
    df = filter_user_data(df)
    df = add_yelping_since_date(df)
    df = calculate_total_compliments(df)
    df = calculate_years_on_platform(df)
    df = drop_unused_columns(df)
    df = explode_friends(df)
    return df

def write_to_s3(df: DataFrame, path: str, format: str = "parquet"):
    if format == "parquet":
        df.write.mode("overwrite").parquet(path)
    elif format == "csv":
        df.write.mode("overwrite").option("header", True).csv(path)
    else:
        raise ValueError("Unsupported format. Use 'parquet' or 'csv'.")

# Run pipeline and write to S3
user_df_cleaned = user_etl_pipeline()
write_to_s3(user_df_cleaned, "s3a://yelp-processed-data-storage/cleaned/user/")
