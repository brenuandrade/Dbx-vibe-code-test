# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze — Ingestão de dados crus
# MAGIC Lê os dados de origem e grava na camada Bronze sem transformação de negócio,
# MAGIC apenas com metadados de auditoria.

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema", "bronze")

catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

# COMMAND ----------

import sys

sys.path.append("../src")

from data_engineering.bronze.ingest import ingest_raw_source, write_bronze_table  # noqa: E402
from data_engineering.utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Exemplo: ingestão de um arquivo de origem
# MAGIC Ajuste `source_path` e `source_format` conforme a origem real dos dados
# MAGIC (arquivos em volume/UC, JDBC, Autoloader, etc).

# COMMAND ----------

source_path = "/Volumes/main/landing/raw_files/exemplo"
source_format = "csv"

raw_df = (
    spark.read.format(source_format)
    .option("header", "true")
    .option("inferSchema", "true")
    .load(source_path)
)

bronze_df = ingest_raw_source(raw_df, source_name="exemplo")

# COMMAND ----------

write_bronze_table(bronze_df, catalog=catalog, schema=schema, table="exemplo")

print(f"Ingestão concluída: {catalog}.{schema}.exemplo")
