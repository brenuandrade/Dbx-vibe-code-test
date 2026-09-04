# Databricks notebook source
# MAGIC %md
# MAGIC # Silver — Limpeza e padronização
# MAGIC Lê os dados da camada Bronze, aplica limpeza/deduplicação/validações de
# MAGIC qualidade de dados e grava na camada Silver.

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

bronze_table = f"{catalog}.{schema_bronze}.exemplo"
silver_table = f"{catalog}.{schema_silver}.exemplo"

key_columns = ["id"]  # ajuste para a chave de negócio real
order_by_column = "_ingested_at"

bronze_df = spark.table(bronze_table)
silver_df = clean_bronze_to_silver(
    bronze_df, key_columns=key_columns, order_by_column=order_by_column
)

# COMMAND ----------

results = [
    check_no_nulls(silver_df, columns=key_columns),
    check_no_duplicates(silver_df, key_columns=key_columns),
]
run_checks(results)

# COMMAND ----------

(
    silver_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Transformação concluída: {silver_table}")
