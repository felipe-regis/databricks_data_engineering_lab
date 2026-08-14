# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

def read_transactions(spark, source_path, schema):
    """
    Reads the transactions data from the source path and returns a DataFrame.

    Args:
        spark: SparkSession object.
        source_path: Path to the source data.
        schema: Schema of the source data.

    Returns:
        DataFrame containing the transactions data.
    """
    return spark.read.csv(source_path, header=True, schema=schema)

def add_ingestion_metadata(df):
    """
    Adds ingestion metadata columns to the DataFrame.

    Args:
        df: Input DataFrame.

    Returns:
        DataFrame with added ingestion metadata columns.
    """
    return (df
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_source_file", F.col("_metadata.file_path"))
           )


if __name__ == "__main__":
    # Aqui é onde o Spark REAL é instanciado quando o pipeline roda na nuvem ou localmente
    spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

    schema = StructType([
    StructField("transaction_id", StringType(), True), 
    StructField("customer_id", StringType(), True),
    StructField("amount", StringType(), True),
    StructField("transaction_date", StringType(), True)
    ])

    source_path = "/Volumes/workspace/default/landing/"
    full_file_path = source_path + "transactions.csv"

    df = read_transactions(spark, full_file_path, schema)

    augmented_df = add_ingestion_metadata(df)

    augmented_df.write.format("delta").mode("append").saveAsTable("workspace.default.transactions_bronze")