"""
spark_session.py
Centralized SparkSession builder for the Goodreads Azure Lakehouse (DSAI 3202 Lab 3)

Ensures consistent Spark configuration across Databricks and local execution.
Handles connection to Azure Data Lake Storage Gen2 using ABFS.
"""

from pyspark.sql import SparkSession

def create_spark_session(app_name: str = "GoodreadsLakehouse") -> SparkSession:
    """
    Builds a SparkSession with optimized configuration for Azure Data Lake access.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured Spark session object.
    """

    spark = (
        SparkSession.builder
        .appName(app_name)
        # Enable Delta Lake support
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Optimize shuffle partitions for moderate dataset size
        .config("spark.sql.shuffle.partitions", "8")
        # Enable Arrow optimization for Pandas interoperability
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        # Enable autoBroadcastJoinThreshold for performance tuning
        .config("spark.sql.autoBroadcastJoinThreshold", "10MB")
        .getOrCreate()
    )

    print(f"SparkSession '{app_name}' initialized.")
    print("Spark version:", spark.version)
    return spark


if __name__ == "__main__":
    spark = create_spark_session()
    df = spark.createDataFrame([(1, "sample")], ["id", "text"])
    df.show()
    spark.stop()
