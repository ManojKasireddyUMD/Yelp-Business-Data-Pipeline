from pyspark.sql.functions import col, isnan, when, size

# Assume df_business is your cleaned business DataFrame

# 1. Test: No null business_id
assert df_business.filter(col("business_id").isNull()).count() == 0, "Null business_id found"

# 2. Test: Dropped businesses with missing categories
assert df_business.filter((col("categories").isNull()) | (col("categories") == "")).count() == 0, "Businesses without categories found"

# 3. Test: Dropped businesses with missing hours
assert df_business.filter(col("hours").isNull()).count() == 0, "Businesses with missing hours found"

# 4. Test: Removed address column
assert "address" not in df_business.columns, "address column still exists"

# 5. Test: Flattened schema (no complex struct/array columns)
complex_types = ["struct", "array"]
flat_schema = all(field.dataType.simpleString().split("<")[0] not in complex_types for field in df_business.schema.fields)
assert flat_schema, "Non-flat schema detected"

# 6. Test: Latitude and longitude are float type
assert df_business.schema["latitude"].dataType.simpleString() == "double", "latitude is not float"
assert df_business.schema["longitude"].dataType.simpleString() == "double", "longitude is not float"

# 7. Test: No entirely null rows
null_row_count = df_business.select([when(col(c).isNull() | isnan(c) | (col(c) == ""), 1).otherwise(0) for c in df_business.columns]).agg(
    sum(col_name) for col_name in df_business.columns
).filter(sum(col(c) for c in df_business.columns) == len(df_business.columns)).count()
assert null_row_count == 0, "Found rows where all columns are null"

print("✅ All business ETL tests passed.")

