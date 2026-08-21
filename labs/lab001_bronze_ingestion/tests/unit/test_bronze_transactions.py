# tests/unit/test_bronze_transactions.py
import pytest 
from datetime import date
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, DateType
from labs.lab001_bronze_ingestion.src.ingestion.bronze_transactions import add_ingestion_metadata, separate_quarantine

def test_add_ingestion_metadata_adds_columns(spark, tmp_path):
    # 1. PREPARAÇÃO: Criar um arquivo CSV temporário real na sua máquina local
    csv_file = tmp_path / "transactions_mock.csv"
    csv_data = "transaction_id,customer_id,amount,transaction_date\n1,CUST0001,100.0,2024-01-01"
    csv_file.write_text(csv_data)

    # 2. Leitura do arquivo usando o Spark (Isso força o Spark a criar a coluna oculta _metadata)
    schema = StructType([
        StructField("transaction_id", StringType(), True), 
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("transaction_date", DateType(), True)
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

def test_separate_quarantine_splits_valid_and_invalid_records(spark):
    """
    Testa se a função de quarentena divide corretamente os registros.
    Regra: Registros com 'amount' nulo vão para quarentena.
    """
    # 1. PREPARAÇÃO: Criando o schema exato do seu contrato de dados
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("amount", DoubleType(), True), 
        StructField("transaction_date", DateType(), True)
    ])

    # Simulando o comportamento do Spark lendo um CSV com erro de tipagem.
    # O valor 100.5 é válido. O valor None simula o "N/A" que o Spark converteu silenciosamente.
    data = [
        ("1", "CUST001", 100.5, date(2024, 1, 1)),
        ("2", "CUST002", None, date(2024, 1, 2)), # <- Deve ir para quarentena
        ("3", "CUST003", 250.0, date(2024, 1, 3))
    ]
    
    df_teste = spark.createDataFrame(data, schema)

    # 2. EXECUÇÃO: Chamando a sua nova função
    valid_df, quarantine_df = separate_quarantine(df_teste)

    # 3. VALIDAÇÃO: Verificando as contagens (2 válidos, 1 na quarentena)
    assert valid_df.count() == 2
    assert quarantine_df.count() == 1

    # Validando o conteúdo: Garantindo que não há nulos no DataFrame válido
    valid_amounts = [row.amount for row in valid_df.collect()]
    assert None not in valid_amounts

    # Validando o conteúdo: Garantindo que o DataFrame de quarentena só tem o registro falho
    quarantine_records = quarantine_df.collect()
    assert quarantine_records[0].amount is None
    assert quarantine_records[0].transaction_id == "2"