"""
clean_reviews.py
Cleaning pipeline for Goodreads reviews dataset (Silver Layer preparation)

Performs structural validation, null handling, and text normalization
before persisting the cleaned dataset back to Azure Data Lake (Silver).
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, length

def clean_reviews(spark: SparkSession, input_path: str, output_path: str):
    """
    Cleans raw Goodreads reviews data from the given Parquet source.

    Steps:
        1. Load raw Parquet from Silver (processed) layer.
        2. Drop rows with missing critical identifiers (review_id, book_id, user_id).
        3. Cast rating to integer and filter to valid range [1–5].
        4. Trim review_text, remove null/empty/short entries (<10 chars).
        5. Drop duplicate review_id entries.
        6. Persist cleaned DataFrame to Silver path (overwrite mode).

    Args:
        spark (SparkSession): Active Spark session.
        input_path (str): Path to raw Parquet (Silver layer).
        output_path (str): Destination for cleaned Parquet.

    Returns:
        DataFrame: Cleaned reviews DataFrame.
    """

    # 1. Load
    reviews = spark.read.parquet(input_path)
    print("Loaded raw dataset:")
    reviews.show(5, truncate=False)
    reviews.printSchema()

    # 2. Drop rows missing identifiers
    df = reviews.filter(
        col("review_id").isNotNull() &
        col("book_id").isNotNull() &
        col("user_id").isNotNull()
    )

    # 3. Cast and validate rating
    df = df.withColumn("rating_int", col("rating").cast("int"))
    df = df.filter(
        col("rating_int").isNotNull() &
        (col("rating_int") >= 1) &
        (col("rating_int") <= 5)
    )

    # 4. Trim and filter text
    df = df.withColumn("review_text", trim(col("review_text")))
    df = df.filter(
        col("review_text").isNotNull() &
        (length(col("review_text")) >= 10)
    )

    # 5. Drop duplicates
    df = df.dropDuplicates(["review_id"])

    # 6. Select final columns
    reviews_clean = df.select(
        "review_id",
        "book_id",
        "user_id",
        col("rating_int").alias("rating"),
        "review_text"
    )

    # 7. Write cleaned output
    reviews_clean.write.mode("overwrite").parquet(output_path)

    print("Cleaned dataset written to:", output_path)
    reviews_clean.show(5, truncate=False)
    return reviews_clean


if __name__ == "__main__":
    spark = SparkSession.builder.appName("CleanReviews").getOrCreate()

    input_path = "abfss://lakehouse@goodreadsreviews60XXXXXX.dfs.core.windows.net/processed/reviews/"
    output_path = "abfss://lakehouse@goodreadsreviews60XXXXXX.dfs.core.windows.net/processed/reviews_cleaned/"

    clean_reviews(spark, input_path, output_path)
    spark.stop()
