"""
main.py
Entry point for Goodreads Azure Lakehouse (DSAI 3202 Lab 3)
Runs: clean → curate → features with verification.
"""

from src.spark_session import create_spark_session
from src.config import REVIEWS_PATH, CURATED_PATH, FEATURES_PATH
from src.clean_reviews import clean_reviews
from src.curate_gold import curate_gold
from src.features import build_features

def main():
    spark = create_spark_session("GoodreadsPipeline")

    # 1) Clean Silver reviews in-place (per lab spec)
    print("\n[1] Cleaning Silver reviews …")
    clean_reviews(spark, input_path=REVIEWS_PATH, output_path=REVIEWS_PATH)

    # 2) Curate Gold table (Delta) and verify
    print("\n[2] Building curated_reviews …")
    curated_df = curate_gold(spark)
    print("curated_reviews rows =", curated_df.count())

    # 3) Build features_v1 (Delta) and verify
    print("\n[3] Building features_v1 …")
    features_df = build_features(spark)
    print("features_v1 rows =", features_df.count())

    print("\nPipeline completed.")
    spark.stop()

if __name__ == "__main__":
    main()
