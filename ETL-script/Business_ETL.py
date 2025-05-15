# business_etl_pipeline.py
# Databricks notebook to process Yelp Business data from raw S3 to processed S3

# ───────────────────────────────────────────────────────────────────────────────
# Cell 1: Widgets, Domain Derivation, Mount Buckets
# ───────────────────────────────────────────────────────────────────────────────
dbutils.widgets.removeAll()
dbutils.widgets.text(
    name="raw_key",
    defaultValue="Business/Business_batch_1.json",
    label="Raw S3 key to process (e.g. Business/…)"
)
raw_key = dbutils.widgets.get("raw_key")
domain = raw_key.split("/", 1)[0]

if domain != "Business":
    print(f"Skipping Business notebook because domain={domain!r}")
    dbutils.notebook.exit("SKIPPED")
print(f"▶ raw_key={raw_key!r}, domain={domain!r}")

def safe_mount(source, mount_point):
    try:
        dbutils.fs.mount(source=source, mount_point=mount_point)
        print(f"Mounted {source} → {mount_point}")
    except Exception:
        print(f"{mount_point} already mounted")

safe_mount("s3a://yelprawdata", "/mnt/yelprawdata")
safe_mount("s3a://yelpprocesseddata", "/mnt/yelpprocesseddata")
display(dbutils.fs.mounts())

# ───────────────────────────────────────────────────────────────────────────────
# Cell 2: Imports
# ───────────────────────────────────────────────────────────────────────────────
from pyspark.sql.functions import *
from pyspark.sql.types import *
from functools import reduce

# ───────────────────────────────────────────────────────────────────────────────
# Cell 3: Inspect & Read Raw Business Data
# ───────────────────────────────────────────────────────────────────────────────
raw_folder = f"/mnt/yelprawdata/{domain}"
print("▶ Listing folder:", raw_folder)
display(dbutils.fs.ls(raw_folder))

if raw_key.lower().endswith(".json") and "/" in raw_key:
    raw_path = f"{raw_folder}/{raw_key.split('/',1)[1]}"
    print("▶ Reading single file:", raw_path)
else:
    raw_path = f"{raw_folder}/*.json"
    print("▶ Reading all JSONs in folder:", raw_path)

df = spark.read.json(raw_path)
df.printSchema()
display(df.limit(5))

# ───────────────────────────────────────────────────────────────────────────────
# Cell 4: Clean Data & Extract Category Array
# ───────────────────────────────────────────────────────────────────────────────
df = (
    df.drop("address")
      .filter(col("categories").isNotNull())
      .filter(col("hours").isNotNull())
      .filter(col("is_open") == 1)
      .withColumn("categories_array", split(trim(col("categories")), ",\\s*"))
)
display(df.select("categories", "categories_array").limit(5))

# ───────────────────────────────────────────────────────────────────────────────
# Cell 5: Parse Nested Attributes (JSON Strings → Structs)
# ───────────────────────────────────────────────────────────────────────────────
ambience_schema = StructType([StructField(f, StringType(), True) for f in [
    "romantic","intimate","classy","hipster","divey","touristy","trendy","upscale","casual"
]])
parking_schema = StructType([StructField(f, StringType(), True) for f in [
    "garage","street","validated","lot","valet"
]])

df = (
    df.withColumn("Ambience_parsed", from_json(col("attributes.Ambience"), ambience_schema))
      .withColumn("BusinessParking_parsed", from_json(col("attributes.BusinessParking"), parking_schema))
      .withColumn("accepts_insurance", col("attributes.AcceptsInsurance").cast("boolean"))
      .withColumn("accepts_credit_cards", col("attributes.BusinessAcceptsCreditCards").cast("boolean"))
      .withColumn("bike_parking_flag", col("attributes.BikeParking").cast("boolean"))
      .withColumn("bitcoin_accepted_flag", col("attributes.BusinessAcceptsBitcoin").cast("boolean"))
      .withColumn("by_appointment_only_flag", col("attributes.ByAppointmentOnly").cast("boolean"))
      .withColumn("caters_flag", col("attributes.Caters").cast("boolean"))
      .withColumn("coat_check_flag", col("attributes.CoatCheck").cast("boolean"))
      .withColumn("corkage_flag", col("attributes.Corkage").cast("boolean"))
      .withColumn("drive_thru_flag", col("attributes.DriveThru").cast("boolean"))
      .withColumn("dogs_allowed_flag", col("attributes.DogsAllowed").cast("boolean"))
      .withColumn("good_for_dancing_flag", col("attributes.GoodForDancing").cast("boolean"))
      .withColumn("good_for_kids_flag", col("attributes.GoodForKids").cast("boolean"))
      .withColumn("happy_hour_flag", col("attributes.HappyHour").cast("boolean"))
      .withColumn("has_tv_flag", col("attributes.HasTV").cast("boolean"))
      .withColumn("noise_level", lower(regexp_extract(col("attributes.NoiseLevel"), r"'([^']+)'", 1)))
      .withColumn("wifi", col("attributes.WiFi"))
      .withColumn("price_range", col("attributes.RestaurantsPriceRange2").cast("int"))
      .drop("attributes.Music", "attributes.RestaurantsCounterService")
      .drop("attributes.Ambience", "attributes.BusinessParking")
)
display(df.select("accepts_credit_cards", "noise_level", "wifi", "price_range").limit(5))

# ───────────────────────────────────────────────────────────────────────────────
# Cell 6: Compute Weekly Hours & Days Open
# ───────────────────────────────────────────────────────────────────────────────
days = ["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]
for d in days:
    df = df.withColumn(f"hours_{d}", col("hours").getField(d))

def to_minutes(col_name):
    parts = split(col(col_name), ":")
    return (parts.getItem(0).cast("int")*60 + parts.getItem(1).cast("int")).cast("double")

for d in days:
    df = (
        df.withColumn(f"{d}_open_str", split(col(f"hours_{d}"), "-").getItem(0))
          .withColumn(f"{d}_close_str", split(col(f"hours_{d}"), "-").getItem(1))
          .withColumn(f"{d}_open_min", to_minutes(f"{d}_open_str"))
          .withColumn(f"{d}_close_min", to_minutes(f"{d}_close_str"))
          .withColumn(f"{d}_hrs", when(
              col(f"{d}_close_min") >= col(f"{d}_open_min"),
              (col(f"{d}_close_min") - col(f"{d}_open_min")) / 60.0
          ).otherwise(
              (col(f"{d}_close_min") + 1440 - col(f"{d}_open_min")) / 60.0
          ))
    )

hrs_cols = [f"{d}_hrs" for d in days]
for c in hrs_cols:
    df = df.withColumn(c, coalesce(col(c), lit(0.0)))

df = (
    df.withColumn("weekly_open_hours", reduce(lambda a, b: a + b, [col(c) for c in hrs_cols]))
      .withColumn("days_open", reduce(lambda a, b: a + when(col(b) > 0, 1).otherwise(0), hrs_cols))
      .drop(*([f"hours_{d}" for d in days] +
              [f"{d}_{s}" for d in days for s in ("open_str", "close_str", "open_min", "close_min")] +
              hrs_cols))
)

# ───────────────────────────────────────────────────────────────────────────────
# Cell 7: Final Cleanup & Explode Categories
# ───────────────────────────────────────────────────────────────────────────────
df = (
    df.withColumn("business_id", trim(col("business_id")))
      .withColumn("category", explode(col("categories_array")))
      .withColumn("category", trim(col("category")))
      .drop("categories", "categories_array", "BusinessParking_parsed", "Ambience_parsed",
            "corkage_flag", "is_open", "hours", "attributes")
)

display(df.limit(5))
df.printSchema()

# ───────────────────────────────────────────────────────────────────────────────
# Cell 8: Write to Processed S3
# ───────────────────────────────────────────────────────────────────────────────
output_path = "/mnt/yelpprocesseddata/Businessprocessed"
df.write.mode("append").parquet(output_path)
print("Wrote processed business data to:", output_path)

display(dbutils.fs.ls(output_path))
df_check = spark.read.parquet(output_path)
display(df_check.limit(20))

