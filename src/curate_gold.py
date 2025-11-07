"""
curate_gold.py
Gold Layer Curation — Homework 1 (Databricks)

Joins cleaned reviews with books, authors, and bridge tables
to create the curated_reviews Delta table used for downstream analysis.

Steps:
    1. Load cleaned reviews (Silver).
    2. Load books, authors, and book_authors bridge tables.
    3. Join on book_id and author_id.
    4. Select required columns per lab instructions.
    5. Save curated dataset as Delta to Gold layer.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from src.config import REVIEWS_PATH, BOOKS_PATH, AUTHORS_PATH, CURATED_PATH

def curate_gold(spark: SparkSession):
    """
    Builds the Gold Layer curated_reviews Delta table.

    Args:
        spark (SparkSession): Active Spark session.

    Returns:
        DataFrame: Curated Gold-layer DataFrame.
    """

    # 1. Load cleaned datasets
    reviews = spark.read.parquet(REVIEWS_PATH)
    books = spark.read.parquet(BOOKS_PATH)
    authors = spark.read.parquet(AUTHORS_PATH)

    # 2. Optional: bridge (book_authors) if schema requires
    try:
        book_authors = spark.read.parquet(f"{BOOKS_PATH}/book_authors/")
        join_expr = (reviews.book_id == book_authors.book_id) & (book_authors.author_id == authors.author_id)
        joined = (reviews
                  .join(book_authors, "book_id", "inner")
                  .join(authors, "author_id", "inner")
                  .join(books, "book_id", "inner"))
    except Exception:
        # Direct join fallback (if authors and books already contain shared keys)
        joined = (reviews
                  .join(books, "book_id", "inner")
                  .join(authors, "author_id", "inner"))

    # 3. Select required columns
    curated = joined.select(
        col("review_id"),
        col("book_id"),
        col("title"),
        col("author_id"),
        col("name"),
        col("user_id"),
        col("rating"),
        col("review_text"),
        col("language"),
        col("n_votes"),
        col("date_added")
    )

    # 4. Persist to Gold layer (Delta)
    curated.write.mode("overwrite").format("delta").save(CURATED_PATH)

    # 5. Verification
    print("Curated dataset saved to:", CURATED_PATH)
    curated.show(5, truncate=False)
    curated.printSchema()
    return curated


if __name__ == "__main__":
    spark = SparkSession.builder.appName("CurateGold").getOrCreate()
    curate_gold(spark)
    spark.stop()