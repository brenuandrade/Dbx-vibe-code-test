"""Agregações da camada Gold: métricas de negócio prontas para consumo."""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def build_daily_summary(
    df: DataFrame,
    date_column: str,
    group_by_columns: list[str],
    value_column: str,
) -> DataFrame:
    """Cria uma agregação diária de exemplo (soma, média e contagem) por grupo.

    Serve como ponto de partida para métricas de negócio específicas do domínio.
    """
    return (
        df.withColumn("event_date", F.to_date(F.col(date_column)))
        .groupBy("event_date", *group_by_columns)
        .agg(
            F.sum(value_column).alias(f"{value_column}_total"),
            F.avg(value_column).alias(f"{value_column}_avg"),
            F.count(F.lit(1)).alias("record_count"),
        )
        .orderBy("event_date", *group_by_columns)
    )
