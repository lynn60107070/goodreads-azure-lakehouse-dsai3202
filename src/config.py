"""
config.py
Central configuration file for Goodreads Azure Lakehouse (DSAI 3202 Lab 3)

Stores environment-level constants such as Azure storage paths,
account names, and default dataset locations for Bronze, Silver, and Gold layers.
This module ensures consistent path references across all scripts.
"""

import os

# ---------------------------------------------------------------------
# Azure Storage Configuration
# ---------------------------------------------------------------------

STORAGE_ACCOUNT = "goodreadsreviews60107070"
CONTAINER = "lakehouse"
BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ---------------------------------------------------------------------
# Lakehouse Layer Paths
# ---------------------------------------------------------------------

BRONZE_PATH = os.path.join(BASE_PATH, "raw/")            # raw ingestion layer
SILVER_PATH = os.path.join(BASE_PATH, "processed/")      # cleaned parquet layer
GOLD_PATH   = os.path.join(BASE_PATH, "gold/")           # curated + features layer

# ---------------------------------------------------------------------
# Dataset-Specific Paths
# ---------------------------------------------------------------------

BOOKS_PATH    = os.path.join(SILVER_PATH, "books/")
AUTHORS_PATH  = os.path.join(SILVER_PATH, "authors/")
REVIEWS_PATH  = os.path.join(SILVER_PATH, "reviews/")
CURATED_PATH  = os.path.join(GOLD_PATH, "curated_reviews/")
FEATURES_PATH = os.path.join(GOLD_PATH, "features_v1/")

# ---------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------

def print_config():
    """Print all key storage paths (for quick verification)."""
    print("Azure Lakehouse Configuration")
    print(f"Storage Account: {STORAGE_ACCOUNT}")
    print(f"Container:       {CONTAINER}")
    print(f"Base Path:       {BASE_PATH}")
    print(f"Bronze Path:     {BRONZE_PATH}")
    print(f"Silver Path:     {SILVER_PATH}")
    print(f"Gold Path:       {GOLD_PATH}")
    print(f"Curated Table:   {CURATED_PATH}")
    print(f"Features Table:  {FEATURES_PATH}")

if __name__ == "__main__":
    print_config()
