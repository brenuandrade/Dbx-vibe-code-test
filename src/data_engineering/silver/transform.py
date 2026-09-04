"""Transformações da camada Silver: limpeza, deduplicação e padronização."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def drop_duplicate_records(
    df: DataFrame, key_columns: list[str], order_by_column: str
) -> DataFrame:
    """Remove duplicatas mantendo o registro mais recente por chave.

    Args:
        df: DataFrame de entrada.
        key_columns: colunas que formam a chave de negócio.
        order_by_column: coluna usada para determinar o registro "mais recente"
            (ex.: timestamp de ingestão ou de última atualização).
    """
    window = Window.partitionBy(*key_columns).orderBy(F.col(order_by_column).desc())
    return (
        df.withColumn("_row_number", F.row_number().over(window))
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def trim_string_columns(df: DataFrame) -> DataFrame:
    """Remove espaços em branco nas extremidades de todas as colunas string."""
    string_columns = [f.name for f in df.schema.fields if f.dataType.typeName() == "string"]
    for column in string_columns:
        df = df.withColumn(column, F.trim(F.col(column)))
    return df


def standardize_nulls(df: DataFrame, columns: list[str]) -> DataFrame:
    """Converte strings vazias/placeholders comuns em nulos reais nas colunas informadas."""
    for column in columns:
        df = df.withColumn(
            column,
            F.when(F.trim(F.col(column)).isin("", "NULL", "N/A", "null"), None).otherwise(
                F.col(column)
            ),
        )
    return df


def clean_bronze_to_silver(
    df: DataFrame, key_columns: list[str], order_by_column: str
) -> DataFrame:
    """Pipeline de limpeza padrão aplicado ao promover dados de Bronze para Silver."""
    df = trim_string_columns(df)
    df = standardize_nulls(df, columns=key_columns)
    df = drop_duplicate_records(df, key_columns=key_columns, order_by_column=order_by_column)
    return df
