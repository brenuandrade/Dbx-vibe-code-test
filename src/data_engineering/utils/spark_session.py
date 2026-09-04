"""Utilitário para obtenção de uma SparkSession, tanto em Databricks quanto localmente."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "data-engineering-pipeline") -> SparkSession:
    """Retorna a SparkSession ativa.

    Em um notebook/cluster Databricks, ``SparkSession.builder.getOrCreate()``
    já retorna a sessão gerenciada pelo runtime. Localmente (testes, CI),
    cria uma sessão standalone mínima.
    """
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
