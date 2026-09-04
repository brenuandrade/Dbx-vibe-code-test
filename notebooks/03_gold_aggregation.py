# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Acompanhamento near-time da Black Friday
# MAGIC Lê os pedidos da camada Silver e cria uma métrica agregada em janelas de
# MAGIC 15 minutos (volume de pedidos, itens vendidos e receita bruta por canal),
# MAGIC pronta para alimentar um dashboard de acompanhamento em quase tempo real.

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

from pyspark.sql import functions as F  # noqa: E402

from data_engineering.gold.aggregate import build_near_time_sales_summary  # noqa: E402
from data_engineering.utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session()

# COMMAND ----------

silver_table = f"{catalog}.{schema_silver}.orders"
gold_table = f"{catalog}.{schema_gold}.near_time_sales_by_channel"

orders_df = spark.table(silver_table).withColumn(
    "order_timestamp", F.to_timestamp("order_timestamp")
)

# COMMAND ----------

gold_df = build_near_time_sales_summary(
    orders_df,
    timestamp_column="order_timestamp",
    window_duration="15 minutes",
    group_by_columns=["channel"],
)

# COMMAND ----------

(
    gold_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_table)
)

print(f"Agregação concluída: {gold_table}")
