"""Utilitário para obtenção de uma SparkSession, tanto em Databricks quanto localmente."""

from __future__ import annotations

from pyspark.sql import SparkSession


def get_spark_session(app_name: str = "data-engineering-pipeline") -> SparkSession:
    """Retorna a SparkSession ativa.

    Em um notebook Databricks — cluster clássico *ou* serverless — já existe
    uma sessão ativa gerenciada pelo runtime (no serverless, via Spark
    Connect); reaproveitá-la é obrigatório: chamar ``.master("local[*]")``
    nesse contexto falha com ``CANNOT_CONFIGURE_SPARK_CONNECT_MASTER``, pois
    master local e Spark Connect não podem ser configurados juntos.
    Localmente (testes, CI, script standalone), não há sessão ativa, então
    criamos uma standalone mínima.
    """
    active_session = SparkSession.getActiveSession()
    if active_session is not None:
        return active_session
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )
