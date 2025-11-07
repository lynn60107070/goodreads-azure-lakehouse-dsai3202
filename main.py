"""
main.py
Entry point for Goodreads Azure Lakehouse (DSAI 3202 Lab 3)
Coordinates Spark session creation, data I/O, cleaning, and feature curation.

This script can be executed end-to-end in Databricks or locally.
"""

from src.spark_session import create_spark_session
from src.config import REVIEWS_PATH, CURATED_PATH, FEATURES_PATH
from src.io import read_data, write_data, inspect_data
from src.clean_reviews import clean_reviews
from src.curate_gold import curate_gold
from src.features import generate_features

def main():
    # Initialize Spark
    spark = create_spark_session("GoodreadsPipeline")

    # Step 1: Load uncleaned reviews (Silver layer)
    print("\n[1] Loading raw reviews from Silver layer...")
    reviews_df = read_data(spark, REVIEWS_PATH, fmt="parquet")
    inspect_data(reviews_df, n=3)

    # Step 2: Clean reviews
    print("\n[2] Cleaning dataset...")
    reviews_clean = clean_reviews(reviews_df)

    # Step 3: Curate Gold table (join with books/authors and filter relevant columns)
    print("\n[3] Curating Gold dataset...")
    curated_df = curate_gold(spark, reviews_clean)

    # Step 4: Generate derived features (aggregations, word count, etc.)
    print("\n[4] Generating enriched features...")
    features_df = generate_features(curated_df)

    # Step 5: Persist outputs
    print("\n[5] Writing cleaned curated dataset to Gold layer...")
    write_data(curated_df, CURATED_PATH, fmt="delta")

    print("\n[6] Writing feature-enriched dataset to Gold layer...")
    write_data(features_df, FEATURES_PATH, fmt="delta")

    print("\nPipeline execution completed successfully.")
    spark.stop()


if __name__ == "__main__":
    main()
