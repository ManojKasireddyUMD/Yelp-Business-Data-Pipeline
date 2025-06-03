
%pip install vaderSentiment
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, trim, to_date, udf, when
)
from pyspark.sql.types import FloatType
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer

def read_review_table(table_name: str) -> DataFrame:
    return spark.read.table(table_name)

def add_sentiment_score(df: DataFrame) -> DataFrame:
    analyzer = SentimentIntensityAnalyzer()

    def get_vader_score(text):
        return float(analyzer.polarity_scores(text)["compound"]) if text else 0.0

    vader_udf = udf(get_vader_score, FloatType())
    df = df.withColumn("sentiment_score", vader_udf(col("text")))
    df = df.withColumn(
        "sentiment_label",
        when(col("sentiment_score") >= 0.05, "positive")
        .when(col("sentiment_score") <= -0.05, "negative")
        .otherwise("neutral")
    )
    return df

def clean_review_columns(df: DataFrame) -> DataFrame:
    # Trim all string columns
    trimmed_df = df.select([
        trim(col(c)).alias(c) if dict(df.dtypes)[c] == "string" else col(c)
        for c in df.columns
    ])
    return trimmed_df

def transform_review_date(df: DataFrame) -> DataFrame:
    return df.withColumn("date", to_date(col("date")))

def drop_text_column(df: DataFrame) -> DataFrame:
    return df.drop("text")

def write_review_to_s3(df: DataFrame, output_path: str):
    df.write.mode("overwrite").parquet(output_path)

# Execution
review_df = read_review_table("aything.default.review_ext")
review_df = add_sentiment_score(review_df)
review_df = drop_text_column(review_df)
review_df = transform_review_date(review_df)
review_df = clean_review_columns(review_df)
review_df.show()
