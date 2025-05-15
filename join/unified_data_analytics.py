# unified_analytics_join.py
# Joins Yelp Review, User, and Business datasets into a unified analytics table

from pyspark.sql.functions import col

# ───────────────────────────────────────────────────────────────────────────────
# Define Paths for Processed Data
# ───────────────────────────────────────────────────────────────────────────────
user_path     = "/mnt/yelpprocesseddata/Userprocessed"
review_path   = "/mnt/yelpprocesseddata/Reviewprocessed"
business_path = "/mnt/yelpprocesseddata/Businessprocessed"

# ───────────────────────────────────────────────────────────────────────────────
# Load Datasets
# ───────────────────────────────────────────────────────────────────────────────
df_user     = spark.read.parquet(user_path)
df_review   = spark.read.parquet(review_path)
df_business = spark.read.parquet(business_path)

# ───────────────────────────────────────────────────────────────────────────────
# Identify Join Keys & Handle Overlapping Columns
# ───────────────────────────────────────────────────────────────────────────────
join_keys = {"user_id", "business_id"}

user_cols     = set(df_user.columns)
review_cols   = set(df_review.columns)
business_cols = set(df_business.columns)

# Overlapping columns between review and user (excluding join keys)
overlap_review_user = (review_cols & user_cols) - join_keys

# Overlapping columns across review/user and business (excluding join keys)
overlap_reviewuser_business = ((review_cols | user_cols) & business_cols) - join_keys

# Rename overlapping columns in review
for col_name in overlap_review_user:
    df_review = df_review.withColumnRenamed(col_name, f"review_{col_name}")

# Rename overlapping columns in business
for col_name in overlap_reviewuser_business:
    df_business = df_business.withColumnRenamed(col_name, f"business_{col_name}")

# ───────────────────────────────────────────────────────────────────────────────
# Join Operations
# ───────────────────────────────────────────────────────────────────────────────
df_review_user = df_review.join(df_user, on="user_id", how="left")
df_unified     = df_review_user.join(df_business, on="business_id", how="left")

# ───────────────────────────────────────────────────────────────────────────────
# Output & Verify
# ───────────────────────────────────────────────────────────────────────────────
df_unified.show(20)
df_unified.printSchema()

unified_path = "/mnt/yelpprocesseddata/UnifiedAnalytics"
df_unified.write.mode("overwrite").parquet(unified_path)
print(f"Unified analytics table written to {unified_path}")

# Optional quick verification
display(df_unified.limit(20))

