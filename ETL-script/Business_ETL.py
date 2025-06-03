from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, split, trim, explode, regexp_replace, lower, when,
    coalesce, lit, sum as spark_sum, to_date
)

def load_business_data() -> DataFrame:
    return spark.read.table("aything.default.business_ext")

def initial_cleaning(df: DataFrame) -> DataFrame:
    df = df.filter("is_open = 1")
    df = df.dropna(how='all', subset=["postal_code", "city"])
    df = df.dropna(how='all', subset=["state"])
    df = df.dropna(how='any', subset=["latitude", "longitude", "stars", "name", "business_id", "review_count", "categories"])
    df = df.filter(col("review_count") > 0)
    df = df.drop("address")
    return df

def extract_hours(df: DataFrame) -> DataFrame:
    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for d in days:
        df = df.withColumn(f"hours_{d}", col("hours").getField(d))
    df = df.drop("hours")
    for d in days:
        df = df.withColumn(f"hours_{d}", coalesce(col(f"hours_{d}"), lit("0:0-0:0")))
    return df

def calculate_weekly_open_hours(df: DataFrame) -> DataFrame:
    def to_minutes(time_str_col):
        parts = split(time_str_col, ":")
        return (parts.getItem(0).cast("int") * 60 + parts.getItem(1).cast("int")).cast("double")

    days = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    for d in days:
        df = df.withColumn(f"{d}_open", split(col(f"hours_{d}"), "-").getItem(0))
        df = df.withColumn(f"{d}_close", split(col(f"hours_{d}"), "-").getItem(1))
        df = df.withColumn(f"{d}_open_min", to_minutes(col(f"{d}_open")))
        df = df.withColumn(f"{d}_close_min", to_minutes(col(f"{d}_close")))
        df = df.withColumn(
            f"{d}_hours",
            when(col(f"{d}_close_min") >= col(f"{d}_open_min"),
                 (col(f"{d}_close_min") - col(f"{d}_open_min")) / 60.0)
            .otherwise((col(f"{d}_close_min") + 1440 - col(f"{d}_open_min")) / 60.0)
        )

    drop_cols = [f"{d}_{s}" for d in days for s in ["open", "close", "open_min", "close_min"]]
    df = df.drop(*drop_cols)
    df = df.dropna(how="all", subset=[f"{d}_hours" for d in days])
    df = df.filter(sum([col(f"{d}_hours") for d in days]) > 0)
    df = df.drop([f"hours_{d}" for d in days])
    df = df.withColumn("weekly_open_hours", sum([col(f"{d}_hours") for d in days]))
    return df

def extract_boolean_attributes(df: DataFrame) -> DataFrame:
    boolean_attributes = [
        "AcceptsInsurance", "BusinessAcceptsCreditCards", "BusinessAcceptsBitcoin", "ByAppointmentOnly",
        "BikeParking", "DogsAllowed", "Caters", "CoatCheck", "Corkage", "DriveThru", "GoodForDancing",
        "GoodForKids", "HappyHour", "HasTV", "Open24Hours", "OutdoorSeating", "RestaurantsDelivery",
        "RestaurantsGoodForGroups", "RestaurantsReservations", "RestaurantsTableService",
        "RestaurantsTakeOut", "WheelchairAccessible"
    ]
    for field in boolean_attributes:
        clean_field = field.lower() + "_flag"
        df = df.withColumn(clean_field, when(col(f"attributes.{field}") == "None", None)
                                        .otherwise(col(f"attributes.{field}").cast("boolean")))
    return df

def clean_string_attributes(df: DataFrame) -> DataFrame:
    mappings = {
        "wifi_clean": "attributes.WiFi",
        "noiselevel_clean": "attributes.NoiseLevel",
        "alcohol_clean": "attributes.Alcohol",
        "smoking_clean": "attributes.Smoking",
        "attire_clean": "attributes.RestaurantsAttire"
    }
    for new_col, old_col in mappings.items():
        df = df.withColumn(new_col, lower(trim(regexp_replace(col(old_col), r"^u?'|\'$", ""))))
    return df

def explode_categories(df: DataFrame) -> DataFrame:
    df = df.withColumn("categories_array", split(trim(col("categories")), ",\\s*"))
    df = df.withColumn("category", explode(col("categories_array")))
    df = df.withColumn("category", trim(col("category")))
    return df.drop("categories", "categories_array")

def normalize_string_columns(df: DataFrame) -> DataFrame:
    for field in df.schema.fields:
        if field.dataType.simpleString() == 'string':
            df = df.withColumn(field.name, lower(trim(col(field.name))))
    return df

def drop_redundant_columns(df: DataFrame) -> DataFrame:
    drop_cols = [
        "attributes", "categories", "categories_array", "is_open",
        "Monday_hours", "Tuesday_hours", "Wednesday_hours", "Thursday_hours",
        "Friday_hours", "Saturday_hours", "Sunday_hours"
    ]
    return df.drop(*[c for c in drop_cols if c in df.columns])

def business_etl_pipeline() -> DataFrame:
    df = load_business_data()
    df = initial_cleaning(df)
    df = extract_hours(df)
    df = calculate_weekly_open_hours(df)
    df = extract_boolean_attributes(df)
    df = clean_string_attributes(df)
    df = explode_categories(df)
    df = normalize_string_columns(df)
    df = drop_redundant_columns(df)
    return df

# Run pipeline and write
cleaned_business_df = business_etl_pipeline()
cleaned_business_df.write.mode("overwrite").parquet("s3a://yelp-processed-data-storage/cleaned/business/")
