# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Acompanhamento near-time da Black Friday (dados sintéticos)
# MAGIC Gera e ingere na camada Bronze um dataset sintético de varejo — produtos,
# MAGIC lojas, consumidores, endereços e pedidos (grão de item de pedido) —
# MAGIC concentrado no dia da Black Friday, simulando a chegada near-time dos
# MAGIC eventos de checkout.
# MAGIC
# MAGIC Os dados já incluem ruído propositalmente realista (nulos, formatos
# MAGIC inconsistentes, duplicatas de reenvio, referências órfãs) — a limpeza
# MAGIC fica a cargo da camada Silver. Ver `src/data_engineering/bronze/` para os
# MAGIC geradores (`synthetic_data.py`) e os utilitários de ruído (`noise.py`).

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema", "bronze")
dbutils.widgets.text("black_friday_year", "")
dbutils.widgets.text("n_customers", "5000")
dbutils.widgets.text("n_products", "500")
dbutils.widgets.text("n_stores", "30")
dbutils.widgets.text("n_orders", "50000")
dbutils.widgets.text("seed", "42")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
black_friday_year = dbutils.widgets.get("black_friday_year")
n_customers = int(dbutils.widgets.get("n_customers"))
n_products = int(dbutils.widgets.get("n_products"))
n_stores = int(dbutils.widgets.get("n_stores"))
n_orders = int(dbutils.widgets.get("n_orders"))
seed = int(dbutils.widgets.get("seed"))

# COMMAND ----------

import sys

sys.path.append("../src")

from data_engineering.bronze.ingest import ingest_raw_source, write_bronze_table  # noqa: E402
from data_engineering.bronze.synthetic_data import generate_black_friday_dataset  # noqa: E402
from data_engineering.utils.catalog import ensure_catalog_schema  # noqa: E402
from data_engineering.utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session()

# COMMAND ----------

# Databricks Asset Bundles não cria catálogos/schemas do Unity Catalog
# automaticamente — só os recursos declarados em resources/ (jobs etc.).
# Garante que catalog.schema existam antes da primeira escrita.
ensure_catalog_schema(spark, catalog=catalog, schema=schema)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Geração do dataset sintético
# MAGIC Todas as entidades são geradas de forma determinística (mesma semente ⇒
# MAGIC mesmo dataset), o que facilita comparar execuções e depurar a camada Silver.

# COMMAND ----------

dataset = generate_black_friday_dataset(
    n_customers=n_customers,
    n_products=n_products,
    n_stores=n_stores,
    n_orders=n_orders,
    year=int(black_friday_year) if black_friday_year else None,
    seed=seed,
)

for table_name, records in dataset.items():
    print(f"{table_name}: {len(records)} registros gerados")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ingestão na camada Bronze
# MAGIC Cada entidade vira uma tabela Delta própria em `catalog.schema`, com
# MAGIC metadados de auditoria (`_source`, `_ingested_at`) adicionados pela
# MAGIC ingestão — sem nenhuma limpeza de negócio nesta etapa.

# COMMAND ----------

for table_name, records in dataset.items():
    raw_df = spark.createDataFrame(records)
    bronze_df = ingest_raw_source(raw_df, source_name=f"synthetic_black_friday_{table_name}")
    write_bronze_table(bronze_df, catalog=catalog, schema=schema, table=table_name)
    print(f"Ingestão concluída: {catalog}.{schema}.{table_name}")
