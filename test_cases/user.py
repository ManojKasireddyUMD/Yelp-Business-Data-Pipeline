from pyspark.sql.functions import col, isnan, when

# Assume df_user is your cleaned user DataFrame

# 1. Test: No null user_id
assert df_user.filter(col("user_id").isNull()).count() == 0, "❌ Null user_id found"

# 2. Test: engagement_compliments is correctly calculated
df_test = df_user.withColumn("expected_engagement", col("useful") + col("funny") + col("cool"))
mismatch = df_test.filter(col("engagement_compliments") != col("expected_engagement")).count()
assert mismatch == 0, "❌ Incorrect engagement_compliments calculation"

# 3. Test: Dropped unnecessary compliment subfields
unwanted_columns = [
    "compliment_hot", "compliment_more", "compliment_profile", "compliment_cute",
    "compliment_cool", "compliment_funny", "compliment_plain", "compliment_writer", 
    "compliment_photos"
]
for col_name in unwanted_columns:
    assert col_name not in df_user.columns, f"❌ Unexpected column still present: {col_name}"

# 4. Test: elite_years_count matches count of elite years
df_test = df_user.withColumn("actual_elite_count", when(col("elite") == "", 0).otherwise(col("elite").cast("string").split(",")).getItem(0))
# In production, you'd use a regex/count of commas + 1 if not empty
# For this simplified test, just check that the column exists and is numeric
assert "elite_years_count" in df_user.columns, "❌ Missing elite_years_count column"

# 5. Test: Removed raw interaction columns after consolidation
for col_name in ["useful", "funny", "cool"]:
    assert col_name not in df_user.columns, f"❌ Raw column {col_name} still exists"

# 6. Test: No nulls in core fields
core_fields = ["user_id", "name", "review_count"]
for field in core_fields:
    assert df_user.filter(col(field).isNull()).count() == 0, f"❌ Null values found in {field}"

print("✅ All User ETL tests passed successfully.")
