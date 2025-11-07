"""
Central configuration for Goodreads Azure Lakehouse (DSAI 3202 Lab 3)

All environment-specific values must come from environment variables or secrets.
Never hard-code storage accounts or keys in source control.
"""

import os

# ---------------------------------------------------------------------
# Azure Storage (parametric)
# ---------------------------------------------------------------------
STORAGE_ACCOUNT = os.getenv("AZURE_STORAGE_ACCOUNT", "<yourstorageaccount>")
CONTAINER = os.getenv("AZURE_STORAGE_CONTAINER", "lakehouse")

BASE_PATH = f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net"

# ---------------------------------------------------------------------
# Lakehouse Layers
# ---------------------------------------------------------------------
BRONZE_PATH = f"{BASE_PATH}/raw/"
SILVER_PATH = f"{BASE_PATH}/processed/"
GOLD_PATH   = f"{BASE_PATH}/gold/"

# ---------------------------------------------------------------------
# Dataset Paths
# ---------------------------------------------------------------------
BOOKS_PATH    = f"{SILVER_PATH}books/"
AUTHORS_PATH  = f"{SILVER_PATH}authors/"
REVIEWS_PATH  = f"{SILVER_PATH}reviews/"
CURATED_PATH  = f"{GOLD_PATH}curated_reviews/"
FEATURES_PATH = f"{GOLD_PATH}features_v1/"

def print_config():
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
