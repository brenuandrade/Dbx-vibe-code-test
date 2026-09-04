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


def build_near_time_sales_summary(
    orders_df: DataFrame,
    timestamp_column: str = "order_timestamp",
    window_duration: str = "15 minutes",
    group_by_columns: list[str] | None = None,
) -> DataFrame:
    """Agrega pedidos em janelas de tempo curtas, pensado para acompanhar o
    andamento de um evento como a Black Friday em quase tempo real
    (volume de pedidos, itens vendidos e receita bruta por janela).

    Espera um DataFrame no grão de item de pedido, com `timestamp_column` já
    convertida para um tipo timestamp, além das colunas `order_id`,
    `quantity` e `line_total`.
    """
    group_by_columns = group_by_columns or []
    return (
        orders_df.groupBy(F.window(F.col(timestamp_column), window_duration), *group_by_columns)
        .agg(
            F.countDistinct("order_id").alias("order_count"),
            F.sum("quantity").alias("items_sold"),
            F.sum("line_total").alias("gross_revenue"),
        )
        .select(
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            *group_by_columns,
            "order_count",
            "items_sold",
            "gross_revenue",
        )
        .orderBy("window_start", *group_by_columns)
    )
