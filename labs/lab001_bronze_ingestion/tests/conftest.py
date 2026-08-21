import os
import sys
import pytest
from pyspark.sql import SparkSession

# Força o PySpark a usar o Python do seu ambiente virtual atual (corrige o erro no Windows)
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

@pytest.fixture(scope="session")
def spark():
    """
    Fixture que cria e gerencia uma SparkSession local para os testes unitários.
    O scope="session" garante que o Spark inicie apenas uma vez para toda a bateria de testes.
    """
    # Inicializa uma sessão Spark local
    spark_session = (
        SparkSession.builder
        .appName("meus-testes-locais")
        .master("local[*]") # Usa todos os núcleos da sua máquina local
        .config("spark.sql.shuffle.partitions", "2") # Otimiza a performance para testes pequenos
        .getOrCreate()
    )
    
    # Entrega a sessão para o teste que solicitou
    yield spark_session
    
    # Após a execução de todos os testes, desliga o motor do Spark para liberar memória
    spark_session.stop()