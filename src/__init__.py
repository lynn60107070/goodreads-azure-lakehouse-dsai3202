"""
src package initializer for Goodreads Azure Lakehouse (DSAI 3202 Lab 3)

This package contains all source code modules used for:
- Spark session creation and configuration
- Data I/O operations between Azure Data Lake layers
- Data cleaning and preprocessing logic
- Gold layer curation and feature engineering

Modules:
    config.py           → Environment variables and storage paths
    spark_session.py    → Spark session builder for Databricks/Fabric integration
    io.py               → Reusable read/write helpers for Parquet and Delta
    clean_reviews.py    → Cleaning transformations for reviews dataset
    curate_gold.py      → Joins for curated_reviews table (books + authors + reviews)
    features.py         → Feature aggregation (avg ratings, review counts, word stats)
"""

__all__ = ["config", "spark_session", "io", "clean_reviews", "curate_gold", "features"]
