# tests/unit/test_bronze_transactions.py
import pytest 
from pyspark.sql.types import StructType, StructField, StringType
from ingestion.bronze_transactions import add_ingestion_metadata

def test_add_ingestion_metadata_adds_columns(spark, tmp_path):
    # 1. PREPARAÇÃO: Criar um arquivo CSV temporário real na sua máquina local
    csv_file = tmp_path / "transactions_mock.csv"
    csv_data = "transaction_id,customer_id,amount,transaction_date\n1,CUST0001,100.0,2024-01-01"
    csv_file.write_text(csv_data)

    # 2. Leitura do arquivo usando o Spark (Isso força o Spark a criar a coluna oculta _metadata)
    schema = StructType([
        StructField("transaction_id", StringType(), True), 
        StructField("customer_id", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("transaction_date", StringType(), True)
    ])
    
    # Lendo o arquivo físico temporário criado acima
    df = spark.read.csv(str(csv_file), header=True, schema=schema)

    # 3. EXECUÇÃO: Adicionar os metadados de ingestão
    result_df = add_ingestion_metadata(df)

    # 4. VALIDAÇÃO: Checar se as novas colunas foram adicionadas
    assert "_ingested_at" in result_df.columns
    assert "_source_file" in result_df.columns

    # Checar se as novas colunas não são nulas e se o nome do arquivo consta no caminho
    assert result_df.select("_ingested_at").first()[0] is not None
    
    # O _source_file agora terá um caminho real, ex: 'file:///tmp/pytest-of.../transactions_mock.csv'
    source_file_value = result_df.select("_source_file").first()[0]
    assert source_file_value is not None
    assert "transactions_mock.csv" in source_file_value