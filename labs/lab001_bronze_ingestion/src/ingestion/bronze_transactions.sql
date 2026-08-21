-- This SQL script creates the transactions_bronze table in the fully qualified schema passado como parâmetroif it does not already exist.
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
    /*Tabela de transações com dados bons, dentro do contrato de dados*/

    transaction_id STRING COMMENT 'Identificador único da transação',
    customer_id STRING COMMENT 'Identificador único do cliente',
    amount DOUBLE COMMENT 'Valor da transação',
    transaction_date DATE COMMENT 'Data da transação',
    _ingested_at TIMESTAMP NOT NULL COMMENT 'Timestamp de ingestão',
    _source_file STRING NOT NULL COMMENT 'Arquivo de origem da ingestão'
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze',
    'edl_datatype' = :edl_datatype_raw
);

CREATE TABLE IF NOT EXISTS IDENTIFIER(:quarantine_table_name) (
    
    /*Tabela de transações com dados em quarentena isolados, para análise e correção posterior da equipe de qualidade de dados (ou analistas de negócio)
    sem bloquear o fluxo dos dados bons*/

    transaction_id STRING COMMENT 'Identificador único da transação',
    customer_id STRING COMMENT 'Identificador único do cliente',
    amount STRING COMMENT 'Valor da transação',
    transaction_date STRING COMMENT 'Data da transação',
    _ingested_at TIMESTAMP COMMENT 'Timestamp de ingestão',
    _source_file STRING COMMENT 'Arquivo de origem da ingestão'
)
USING DELTA
TBLPROPERTIES (
    'delta.autoOptimize.optimizeWrite' = 'true',
    'delta.autoOptimize.autoCompact' = 'true',
    'quality' = 'bronze',
    'edl_datatype' = :edl_datatype_raw
);
