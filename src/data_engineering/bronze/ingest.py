"""Ingestão da camada Bronze: leitura dos dados crus e gravação com metadados de auditoria."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_audit_columns(df: DataFrame, source_name: str) -> DataFrame:
    """Adiciona colunas de auditoria/rastreabilidade ao DataFrame ingerido."""
    return df.withColumn("_source", F.lit(source_name)).withColumn(
        "_ingested_at", F.current_timestamp()
    )


def ingest_raw_source(
    df: DataFrame,
    source_name: str,
) -> DataFrame:
    """Prepara um DataFrame de origem para a camada Bronze.

    Não faz nenhuma transformação de negócio — apenas adiciona metadados de
    auditoria. Regras de limpeza e tipagem ficam na camada Silver.
    """
    return add_audit_columns(df, source_name=source_name)


def write_bronze_table(df: DataFrame, catalog: str, schema: str, table: str) -> None:
    """Grava o DataFrame como tabela Delta na camada Bronze (append)."""
    full_table_name = f"{catalog}.{schema}.{table}"
    (
        df.write.format("delta")
        .mode("append")
        .option("mergeSchema", "true")
        .saveAsTable(full_table_name)
    )
