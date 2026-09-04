# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Agregações de negócio
# MAGIC Lê os dados da camada Silver e cria métricas agregadas prontas para consumo
# MAGIC (dashboards, ML, relatórios).

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")

catalog = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold = dbutils.widgets.get("schema_gold")

# COMMAND ----------

import sys

sys.path.append("../src")

from data_engineering.gold.aggregate import build_daily_summary  # noqa: E402
from data_engineering.utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session()

# COMMAND ----------

silver_table = f"{catalog}.{schema_silver}.exemplo"
gold_table = f"{catalog}.{schema_gold}.exemplo_daily_summary"

silver_df = spark.table(silver_table)

gold_df = build_daily_summary(
    silver_df,
    date_column="_ingested_at",
    group_by_columns=["_source"],
    value_column="id",  # ajuste para a coluna de métrica real
)

# COMMAND ----------

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print(f"Agregação concluída: {gold_table}")
