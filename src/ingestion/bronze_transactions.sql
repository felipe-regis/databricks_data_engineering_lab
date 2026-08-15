-- This SQL script creates the transactions_bronze table in the fully qualified schema passado como parâmetroif it does not already exist.
CREATE TABLE IF NOT EXISTS IDENTIFIER(:table_name) (
    transaction_id STRING NOT NULL COMMENT 'Identificador único da transação',
    customer_id STRING NOT NULL COMMENT 'Identificador único do cliente',
    amount DOUBLE COMMENT 'Valor da transação',
    transaction_date DATE NOT NULL COMMENT 'Data da transação',
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