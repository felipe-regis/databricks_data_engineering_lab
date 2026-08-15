# Databricks notebook source
from venv import logger
from databricks.sdk.runtime import dbutils

from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import logging

# Configuração básica do logger
logger = logging.getLogger("BronzeIngestion")
logger.setLevel(logging.INFO)

#  No lugar de print("Iniciando ingestção..."):
logger.info("Iniciando a ingestão de dados na camada Bronze.")


def read_csv(spark, source_path, schema):
    """
    Reads the transactions data from the source path and returns a DataFrame.

    Args:
        spark: SparkSession object.
        source_path: Path to the source data.
        schema: Schema of the source data.

    Returns:
        DataFrame containing the transactions data.
    """
    ## FAILFAST para falhar rápido em caso de erro de leitura
    # return spark.read.option("mode","FAILFAST").csv(source_path, header=True, schema=schema) 

    ## SEM FAILFAST para não falhar rápido em caso de erro de leitura (Pode ser útil para lidar com dados sujos, mas pode mascarar problemas de qualidade de dados)    
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

def separate_quarantine(df):
    """
    Separa os registros válidos dos registros anômalos (quarentena) em memória.
    Regra: Transações onde o 'amount' não pôde ser convertido para Double (ficou nulo).
    """
    # Filtra apenas os dados onde a conversão para numérico teve sucesso
    valid_df = df.filter(F.col("amount").isNotNull())
    
    # Filtra os dados onde ocorreu falha na conversão (ex: "N/A" na origem)
    quarantine_df = df.filter(F.col("amount").isNull())
    
    return valid_df, quarantine_df


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

        # Contrato de dados reforçado e atualizado para DoubleType e DateType em contrato com a tabela Delta
        transactions_schema = StructType([
            StructField("transaction_id", StringType(), True), 
            StructField("customer_id", StringType(), True),
            StructField("amount", DoubleType(), True), # Schema reforçado e atualizado para DoubleType em contrato com a tabela Delta
            StructField("transaction_date", DateType(), True) # Schema reforçado e atualizado para DateType em contrato com a tabela Delta
        ])

        source_path = f"/Volumes/{default_catalog}/{bronze_schema}/landing/"
        csv_file_path = source_path + "transactions.csv"

        # Lendo o arquivo físico real
        df = read_csv(spark, csv_file_path, transactions_schema)

        # Adicionando metadados de ingestão
        df = add_ingestion_metadata(df)

        # Separando os dados válidos dos dados em quarentena
        valid_df, quarantine_df = separate_quarantine(df)

        # Criando a tabela Delta se ela não existir e limpando-a antes de escrever os novos dados
        table_name = "transactions"
        table_path = f"{default_catalog}.{bronze_schema}.{table_name}"
        spark.sql(f"TRUNCATE TABLE {table_path}") # Limpa a tabela antes de escrever os novos dados 

        # Escrevendo os dados com metadados na tabela Delta
        (valid_df.write
         .format("delta")
         .mode("Overwrite")
         .option("mergeSchema", "true")
         .saveAsTable(table_path)
         )
        logger.info(f"Dados válidos gravados com sucesso na tabela {table_path}.")

        quarantine_table_name = "transactions_quarantine"
        quarantine_table_path = f"{default_catalog}.{bronze_schema}.{quarantine_table_name}"

        # Escrevendo os dados com metadados na tabela Delta
        (quarantine_df.write
         .format("delta")
         .mode("append")
         .option("mergeSchema", "true")
         .saveAsTable(quarantine_table_path)
         )

        logger.info("Ingestão concluída com sucesso! Dados anômalos isolados na quarentena.") #[cite: 1]
        pass

    except Exception as e:
        # Isso vai estourar o erro no cluster e acionar o email do YAML!
        logger.error(f"Falha crítica na ingestão: {e}")
        raise e