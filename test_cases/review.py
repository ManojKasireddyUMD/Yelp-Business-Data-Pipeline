from pyspark.sql.functions import col, isnan, when

# Assume df_review is your cleaned review DataFrame

# 1. Test: No null review_id, user_id, or business_id
assert df_review.filter(col("review_id").isNull()).count() == 0, "❌ Null review_id found"
assert df_review.filter(col("user_id").isNull()).count() == 0, "❌ Null user_id found"
assert df_review.filter(col("business_id").isNull()).count() == 0, "❌ Null business_id found"

# 2. Test: engagement_score = useful + funny + cool
df_check = df_review.withColumn("expected_engagement", col("useful") + col("funny") + col("cool"))
mismatch = df_check.filter(col("engagement_score") != col("expected_engagement")).count()
assert mismatch == 0, "❌ Incorrect engagement_score calculation"

# 3. Test: Dropped raw columns after aggregation (optional depending on pipeline)
for col_name in ["useful", "funny", "cool"]:
    assert col_name not in df_review.columns, f"❌ Raw column '{col_name}' should have been dropped"

# 4. Test: Sentiment score or label column exists
assert "sentiment_score" in df_review.columns or "sentiment_label" in df_review.columns, "❌ Sentiment column missing"

# 5. Test: No nulls in core fields like text, stars, or date
for field in ["text", "stars", "date"]:
    assert df_review.filter(col(field).isNull()).count() == 0, f"❌ Null values found in '{field}'"

print("✅ All Review ETL tests passed successfully.")
