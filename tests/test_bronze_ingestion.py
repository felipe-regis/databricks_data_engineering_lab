# tests/test_bronze_ingestion.py
from ingestion.bronze_ingestion import add_ingestion_metadata
import pytest

def test_add_ingestion_metadata_adds_columns(spark):
    # Create a sample DataFrame
    data = [("1", "CUST0001", "100.0", "2024-01-01")]
    columns = ["transaction_id", "customer_id", "amount", "transaction_date"]
    df = spark.createDataFrame(data, columns)

    # Add ingestion metadata
    result_df = add_ingestion_metadata(df)

    # Check if the new columns are added
    assert "_ingested_at" in result_df.columns
    assert "_source_file" in result_df.columns

    # Check if the new columns are not null
    assert result_df.select("_ingested_at").first()[0] is not None
    assert result_df.select("_source_file").first()[0] is not None