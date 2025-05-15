# review_etl_pipeline.py
# Databricks notebook script to process Yelp review data from raw S3 to processed S3

# ───────────────────────────────────────────────────────────────────────────────
# Cell 1: Widgets & Domain Setup
# ───────────────────────────────────────────────────────────────────────────────
dbutils.widgets.removeAll()
dbutils.widgets.text(
    "raw_key",
    defaultValue="Review/review_batch_1.json",
    label="Raw S3 key to process"
)

raw_key = dbutils.widgets.get("raw_key") or "Review/review_batch_1.json"
domain = raw_key.split("/", 1)[0]

if domain != "Review":
    print(f"Skipping Review notebook because domain={domain!r}")
    dbutils.notebook.exit("SKIPPED")

print(f"▶ domain={domain!r}, raw_key={raw_key!r}")

# ───────────────────────────────────────────────────────────────────────────────
# Cell 2: Mount S3 Buckets (Idempotent)
# ───────────────────────────────────────────────────────────────────────────────
def safe_mount(src, mp):
    try:
        dbutils.fs.mount(source=src, mount_point=mp)
        print(f"Mounted {src} → {mp}")
    except Exception:
        print(f"{src} already mounted or failed")

safe_mount("s3a://yelprawdata", "/mnt/yelprawdata")
safe_mount("s3a://yelpprocesseddata", "/mnt/yelpprocesseddata")

display(dbutils.fs.ls(f"/mnt/yelprawdata/{domain}"))

# ───────────────────────────────────────────────────────────────────────────────
# Cell 3: Library Installation & Imports
# ───────────────────────────────────────────────────────────────────────────────
%pip install --quiet vaderSentiment

from pyspark.sql.functions import col, when, udf, to_timestamp, to_date, sum as _sum
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

# ───────────────────────────────────────────────────────────────────────────────
# Cell 4: Load, Clean & Transform Review Data
# ───────────────────────────────────────────────────────────────────────────────
# Load review data
df_review = spark.read.json(f"/mnt/yelprawdata/{domain}/*.json")

# Display schema and sample records
df_review.printSchema()
display(df_review.limit(5))

# Count null values in each column
display(
    df_review.select([
        _sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
        for c in df_review.columns
    ])
)

# Compute engagement_score
df_review = (
    df_review
    .withColumn("engagement_score", col("useful") + col("funny") + col("cool"))
    .drop("useful", "funny", "cool")
)

# Sentiment analysis using VADER
analyzer = SentimentIntensityAnalyzer()
vader_udf = udf(lambda t: float(analyzer.polarity_scores(t)["compound"]) if t else 0.0, FloatType())

df_review = (
    df_review
    .withColumn("sentiment_score", vader_udf(col("text")))
    .withColumn(
        "sentiment_label",
        when(col("sentiment_score") >= 0.05, "positive")
        .when(col("sentiment_score") <= -0.05, "negative")
        .otherwise("neutral")
    )
)

display(df_review.select("review_id", "sentiment_score", "sentiment_label").limit(5))

# Parse and format timestamp
df_review = (
    df_review
    .withColumn("ts", to_timestamp(col("date"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("date", to_date(col("ts")))
    .drop("ts", "text")
)

df_review.printSchema()
display(df_review.limit(5))

# ───────────────────────────────────────────────────────────────────────────────
# Cell 5: Write Processed Data to S3
# ───────────────────────────────────────────────────────────────────────────────
output_path = f"/mnt/yelpprocesseddata/{domain}processed"

df_review.write.mode("append").parquet(output_path)
print(f"Wrote processed reviews to {output_path}")

# Verify output
display(dbutils.fs.ls(output_path))
display(spark.read.parquet(output_path).limit(20))
