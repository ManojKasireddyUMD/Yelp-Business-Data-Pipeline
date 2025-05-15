# user_etl_pipeline.py
# Databricks notebook script – transforms raw Yelp User JSON → processed Parquet

# ───────────────────────────────────────────────────────────────────────────────
# Cell 1  ·  Widgets, Domain Check, Bucket Mounts
# ───────────────────────────────────────────────────────────────────────────────
dbutils.widgets.removeAll()
dbutils.widgets.text(
    "raw_key",
    defaultValue="User/user_batch_1.json",        # manual-test default
    label="Raw S3 key to process"
)
raw_key = dbutils.widgets.get("raw_key") or "User/user_batch_1.json"
domain  = raw_key.split("/", 1)[0]

if domain != "User":
    print(f"Skipping User notebook because domain={domain!r}")
    dbutils.notebook.exit("SKIPPED")
print(f"▶ raw_key={raw_key!r}, domain={domain!r}")

def safe_mount(src: str, mp: str):
    """Idempotently mount S3 buckets."""
    try:
        dbutils.fs.mount(src, mp)
        print(f"Mounted {src} → {mp}")
    except Exception:
        print(f"{mp} already mounted or mount failed")

safe_mount("s3a://yelprawdata",       "/mnt/yelprawdata")
safe_mount("s3a://yelpprocesseddata", "/mnt/yelpprocesseddata")

# ───────────────────────────────────────────────────────────────────────────────
# Cell 2  ·  Imports & Setup
# ───────────────────────────────────────────────────────────────────────────────
%pip install --quiet vaderSentiment   # (needed only for Review notebook; harmless here)

from pyspark.sql.functions import (
    col, when, split, size, to_timestamp, current_timestamp, months_between,
    floor, explode, trim, sum as spark_sum
)
from pyspark.sql import SparkSession

# ───────────────────────────────────────────────────────────────────────────────
# Cell 3  ·  Load Raw Data & Inspect
# ───────────────────────────────────────────────────────────────────────────────
raw_folder = f"/mnt/yelprawdata/{domain}"
print("▶ Listing folder:", raw_folder)
display(dbutils.fs.ls(raw_folder))

df = spark.read.json(f"{raw_folder}/*.json")
df.printSchema()
display(df.limit(5))

# Null-count quick scan
display(
    df.select([
        spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ])
)

# ───────────────────────────────────────────────────────────────────────────────
# Cell 4  ·  Feature Engineering – Engagement, Elite Years, Account Age
# ───────────────────────────────────────────────────────────────────────────────
# 4.1  Engagement score (useful + funny + cool)
df = (
    df.withColumn("engagement_compliments", col("useful") + col("funny") + col("cool"))
      .drop("useful", "funny", "cool")
)

# 4.2  Drop seldom-used compliment_* columns, keep only a short whitelist
keep_cols = {"compliment_list", "compliment_writer", "compliment_note", "compliment_photos"}
drop_cols = [c for c in df.columns if c.startswith("compliment_") and c not in keep_cols]
df = df.drop(*drop_cols)

# 4.3  Number of elite years
df = (
    df.withColumn(
        "elite_years_count",
        when(col("elite") != "", size(split(col("elite"), ","))).otherwise(0)
    )
    .drop("elite")
)

# 4.4  Account age (whole years since yelping_since)
df = (
    df.withColumn("yelping_since_ts",
                  to_timestamp(col("yelping_since"), "yyyy-MM-dd HH:mm:ss"))
      .withColumn("account_age_years",
                  floor(months_between(current_timestamp(), col("yelping_since_ts")) / 12))
      .drop("yelping_since")
)

# ───────────────────────────────────────────────────────────────────────────────
# Cell 5  ·  Friends – Count & Explode
# ───────────────────────────────────────────────────────────────────────────────
df = (
    df.withColumn("friends_arr", split(col("friends"), ","))
      .withColumn("friend_count", size(col("friends_arr")))
      .withColumn("friend", explode(col("friends_arr")))
      .withColumn("friend", trim(col("friend")))
      .drop("friends_arr", "friends")
)

display(df.select("user_id", "friend", "friend_count").limit(5))

# ───────────────────────────────────────────────────────────────────────────────
# Cell 6  ·  Write to Processed Zone & Verify
# ───────────────────────────────────────────────────────────────────────────────
output_path = "/mnt/yelpprocesseddata/Userprocessed"

df.write.mode("append").parquet(output_path)
print(f"Wrote processed users to {output_path}")

display(dbutils.fs.ls(output_path))
display(spark.read.parquet(output_path).limit(20))

