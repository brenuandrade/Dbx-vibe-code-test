# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Limpeza e padronização
# MAGIC Lê cada entidade da camada Bronze (produtos, lojas, consumidores,
# MAGIC endereços e pedidos), aplica limpeza/deduplicação e validações de
# MAGIC qualidade de dados, e grava na camada Silver.
# MAGIC
# MAGIC Esta etapa **não** trata a formatação "suja" de colunas monetárias
# MAGIC (`unit_price` em `products`/`orders`, gerada por `dirty_currency` na
# MAGIC Bronze) — use `data_engineering.bronze.noise.parse_dirty_price` como
# MAGIC ponto de partida para essa limpeza específica antes de promover para Gold.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema_bronze", "bronze")
dbutils.widgets.text("schema_silver", "silver")

catalog = dbutils.widgets.get("catalog")
schema_bronze = dbutils.widgets.get("schema_bronze")
schema_silver = dbutils.widgets.get("schema_silver")

# COMMAND ----------

import sys

sys.path.append("../src")

from data_engineering.silver.transform import clean_bronze_to_silver  # noqa: E402
from data_engineering.utils.data_quality import (  # noqa: E402
    check_no_duplicates,
    check_no_nulls,
    run_checks,
)
from data_engineering.utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session()

# COMMAND ----------

# Chave de negócio e coluna de "mais recente" por entidade. `orders` está no
# grão de item de pedido, por isso a chave combina order_id + product_id.
# Para `orders`, a coluna usada para desempate é `source_ingested_at`
# (quando o sistema de origem capturou o evento), não a `_ingested_at` de
# auditoria (quando este pipeline gravou a linha na Bronze) — as demais
# entidades não têm um timestamp de origem próprio, então usam a de auditoria.
TABLE_CONFIG = {
    "products": {"key_columns": ["product_id"], "order_by_column": "_ingested_at"},
    "stores": {"key_columns": ["store_id"], "order_by_column": "_ingested_at"},
    "customers": {"key_columns": ["customer_id"], "order_by_column": "_ingested_at"},
    "addresses": {"key_columns": ["address_id"], "order_by_column": "_ingested_at"},
    "orders": {
        "key_columns": ["order_id", "product_id"],
        "order_by_column": "source_ingested_at",
    },
}

# COMMAND ----------

for table_name, config in TABLE_CONFIG.items():
    bronze_table = f"{catalog}.{schema_bronze}.{table_name}"
    silver_table = f"{catalog}.{schema_silver}.{table_name}"
    key_columns = config["key_columns"]

    bronze_df = spark.table(bronze_table)
    silver_df = clean_bronze_to_silver(
        bronze_df, key_columns=key_columns, order_by_column=config["order_by_column"]
    )

    results = [
        check_no_nulls(silver_df, columns=key_columns),
        check_no_duplicates(silver_df, key_columns=key_columns),
    ]
    run_checks(results)

    (
        silver_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(silver_table)
    )

    print(f"Transformação concluída: {silver_table}")
