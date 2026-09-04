"""Utilitário para garantir que catálogo/schema de destino existam antes de
gravar tabelas.

Databricks Asset Bundles não cria catálogos/schemas do Unity Catalog
automaticamente — apenas os recursos declarados em ``resources:`` (jobs,
pipelines etc.). Sem isso, a primeira escrita numa tabela falha com
``[SCHEMA_NOT_FOUND]``.
"""

from __future__ import annotations

from typing import Any, Protocol


class SqlExecutor(Protocol):
    def sql(self, query: str) -> Any: ...


def ensure_catalog_schema(spark: SqlExecutor, catalog: str, schema: str) -> None:
    """Cria o catálogo e o schema informados caso ainda não existam.

    Idempotente (``IF NOT EXISTS``) — seguro de chamar em toda execução do
    pipeline, não só na primeira.
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
