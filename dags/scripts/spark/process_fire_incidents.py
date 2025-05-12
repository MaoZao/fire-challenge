import argparse

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date


def process_fire_incidents(
    spark: SparkSession, input_loc: str, output_loc: str
) -> None:
    """
    Reads fire incident data from a CSV file, processes it,
    and writes it as Parquet files to an output location.

    Args:
        spark: The SparkSession object.
        input_loc: The S3 path to the input CSV file
            (e.g., "s3a://fire-sf-bucket/raw/Fire_SF.csv").
        output_loc: The S3 path for the output Parquet files
                    (e.g., "s3a://fire-sf-bucket/processed/").
    """
    print(f"Reading CSV data from {input_loc}")
    # Read input CSV
    # Infer schema to detect data types, including for data_loaded_at
    # Ensure the CSV has a header
    df_raw = (
        spark.read.option("header", "true")
        .option("inferSchema", "true")
        .csv(input_loc)
    )

    print("Raw data schema:")
    df_raw.printSchema()

    # Ensure 'data_loaded_at' column exists
    if "data_loaded_at" not in df_raw.columns:
        raise ValueError(
            f"Column 'data_loaded_at' not found in the input CSV at "
            f"{input_loc}. Available columns: {df_raw.columns}"
        )

    # Cast 'data_loaded_at' to date type
    df_with_casted_date = df_raw.withColumn(
        "data_loaded_at_casted", to_date(col("data_loaded_at"))
    )

    df_filtered = df_with_casted_date.filter(
        col("data_loaded_at_casted").isNotNull()
    )

    # Check if the filtered DataFrame is empty
    columns_to_select = [
        c for c in df_raw.columns if c != "data_loaded_at"
    ] + [col("data_loaded_at_casted").alias("data_loaded_at")]
    df_to_write = df_filtered.select(*columns_to_select)

    print("Schema before writing:")
    df_to_write.printSchema()

    print(f"Writing data to {output_loc}")
    df_to_write.write.mode("overwrite").parquet(output_loc)
    print("Successfully wrote data.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input_path",
        type=str,
        help="S3 input path for Fire_SF.csv",
        default="s3a://fire-sf-bucket/raw/Fire_SF.csv",
    )
    parser.add_argument(
        "--output_path",
        type=str,
        help="S3 output path for processed and partitioned data",
        default="s3a://fire-sf-bucket/processed/",
    )
    args = parser.parse_args()

    print("Starting Spark application for processing fire incidents.")
    print(f"Input path: {args.input_path}")
    print(f"Output path: {args.output_path}")

    spark = (
        SparkSession.builder.appName("FireIncidentsProcessing")
        .config(
            "spark.jars.packages",
            (
                "io.delta:delta-core_2.12:2.3.0,"
                "org.apache.hadoop:hadoop-aws:3.3.2,"
                "org.postgresql:postgresql:42.7.3"
            ),
        )
        .config("spark.hadoop.fs.s3a.access.key", "minio")
        .config("spark.hadoop.fs.s3a.secret.key", "minio123")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config(
            "spark.hadoop.fs.s3a.impl",
            "org.apache.hadoop.fs.s3a.S3AFileSystem",
        )
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .getOrCreate()
    )

    try:
        process_fire_incidents(
            spark=spark, input_loc=args.input_path, output_loc=args.output_path
        )
    finally:
        spark.stop()
        print("Spark application finished.")
