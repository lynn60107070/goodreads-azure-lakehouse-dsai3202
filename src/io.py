"""
io.py
Reusable I/O utilities for reading and writing Delta/Parquet datasets
across Bronze, Silver, and Gold layers of the Goodreads Azure Lakehouse.

Supports uniform access to Azure Data Lake Storage Gen2 paths using abfss:// schema.
"""

from pyspark.sql import SparkSession, DataFrame

def read_data(spark: SparkSession, path: str, fmt: str = "parquet") -> DataFrame:
    """
    Reads a dataset from the specified Azure path.

    Args:
        spark (SparkSession): Active Spark session.
        path (str): abfss:// path to dataset.
        fmt (str): 'parquet' or 'delta'.

    Returns:
        DataFrame: Loaded Spark DataFrame.
    """
    print(f"Reading from {path} (format={fmt})...")
    if fmt == "delta":
        return spark.read.format("delta").load(path)
    return spark.read.parquet(path)


def write_data(df: DataFrame, path: str, fmt: str = "parquet", mode: str = "overwrite") -> None:
    """
    Writes a DataFrame to the specified Azure path.

    Args:
        df (DataFrame): Spark DataFrame to write.
        path (str): abfss:// destination path.
        fmt (str): 'parquet' or 'delta'.
        mode (str): Save mode (default = overwrite).

    Returns:
        None
    """
    print(f"Writing to {path} (format={fmt}, mode={mode})...")
    df.write.mode(mode).format(fmt).save(path)
    print("Write complete.")


def inspect_data(df: DataFrame, n: int = 5) -> None:
    """
    Displays schema and sample rows for quick inspection.

    Args:
        df (DataFrame): Spark DataFrame.
        n (int): Number of rows to show (default = 5).

    Returns:
        None
    """
    df.printSchema()
    df.show(n, truncate=False)
    print(f"Total record count: {df.count()}")


if __name__ == "__main__":
    spark = SparkSession.builder.appName("IOUtils").getOrCreate()
    test_path = "abfss://lakehouse@goodreadsreviews60XXXXXX.dfs.core.windows.net/gold/curated_reviews/"
    df = read_data(spark, test_path, fmt="delta")
    inspect_data(df)
    spark.stop()
