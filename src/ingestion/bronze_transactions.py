# Databricks notebook source
from venv import logger

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import logging

# Configuração básica do logger
logger = logging.getLogger("BronzeIngestion")
logger.setLevel(logging.INFO)

#  No lugar de print("Iniciando ingestção..."):
logger.info("Iniciando a ingestão de dados na camada Bronze.")


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

    try:
        # Aqui é onde o Spark REAL é instanciado quando o pipeline roda na nuvem ou localmente
        spark = SparkSession.builder.appName("BronzeIngestion").getOrCreate()

        dbutils.widgets.text("default_catalog", "workspace", "Catalog")
        dbutils.widgets.text("bronze_schema", "default", "Bronze Schema")
        dbutils.widgets.text("environment", "dev", "Environment")

        default_catalog = dbutils.widgets.get("default_catalog")
        bronze_schema = dbutils.widgets.get("bronze_schema")
        environment = dbutils.widgets.get("environment")

        schema = StructType([
            StructField("transaction_id", StringType(), True), 
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("transaction_date", DateType(), True)
        ])

        source_path = f"/Volumes/{default_catalog}/{bronze_schema}/landing/"
        csv_file_path = source_path + "transactions.csv"

        # Lendo o arquivo físico real
        df = read_transactions(spark, csv_file_path, schema)

        # Adicionando metadados de ingestão
        transactions_with_metadata_df = add_ingestion_metadata(df)

        # Criando a tabela Delta se ela não existir e limpando-a antes de escrever os novos dados
        table_name = "transactions"
        table_path = f"{default_catalog}.{bronze_schema}." + table_name
        spark.sql(f"TRUNCATE TABLE {table_path}")

        # Escrevendo os dados com metadados na tabela Delta
        transactions_with_metadata_df.write.format("delta").mode("overwrite").saveAsTable(table_path)

        # No lugar de print("Ingestão concluída com sucesso!"):
        logger.info("Ingestão concluída com sucesso!")
        pass

    except Exception as e:
        # Isso vai estourar o erro no cluster e acionar o email do YAML!
        logger.error(f"Falha crítica na ingestão: {e}")
        raise e