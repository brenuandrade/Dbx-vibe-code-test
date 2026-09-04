# Databricks notebook source
# MAGIC %md
# MAGIC # Gold — Acompanhamento near-time da Black Friday
# MAGIC Lê os pedidos e consumidores da camada Silver e cria:
# MAGIC 1. `near_time_sales_by_channel` — volume de pedidos, itens vendidos e
# MAGIC    receita bruta em janelas de 15 minutos por canal.
# MAGIC 2. `customer_summary` — visão 360 do cliente (dados cadastrais + métricas
# MAGIC    de pedidos), com **mascaramento de PII** (nome, e-mail, CPF) aplicado
# MAGIC    via Unity Catalog Column Masks — só o grupo administrativo vê o dado
# MAGIC    real, os demais grupos com acesso à tabela veem o valor ofuscado.
# MAGIC
# MAGIC Opcionalmente (widget `run_stress_test`), também roda cenários de
# MAGIC **data skew** e **pico de ingestão** sobre `near_time_sales_by_channel`,
# MAGIC gravando o resultado em tabelas `*_stress_test` separadas — para validar
# MAGIC a resiliência do pipeline antes do dia real, sem afetar as métricas
# MAGIC "de verdade".

# COMMAND ----------

dbutils.widgets.text("catalog", "dev")
dbutils.widgets.text("schema_silver", "silver")
dbutils.widgets.text("schema_gold", "gold")
dbutils.widgets.text("admin_group", "admins")
dbutils.widgets.text("analyst_group", "analysts")
dbutils.widgets.text("run_stress_test", "false")

catalog = dbutils.widgets.get("catalog")
schema_silver = dbutils.widgets.get("schema_silver")
schema_gold = dbutils.widgets.get("schema_gold")
admin_group = dbutils.widgets.get("admin_group")
analyst_group = dbutils.widgets.get("analyst_group")
run_stress_test = dbutils.widgets.get("run_stress_test").strip().lower() == "true"

# COMMAND ----------

import sys

sys.path.append("../src")

from pyspark.sql import functions as F  # noqa: E402

from data_engineering.gold.aggregate import (  # noqa: E402
    build_customer_summary,
    build_near_time_sales_summary,
)
from data_engineering.gold.privacy import apply_privacy_layer  # noqa: E402
from data_engineering.gold.simulate import (  # noqa: E402
    simulate_data_skew,
    simulate_ingestion_spike,
)
from data_engineering.utils.spark_session import get_spark_session  # noqa: E402

spark = get_spark_session()

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Vendas near-time por canal

# COMMAND ----------

orders_table = f"{catalog}.{schema_silver}.orders"
sales_gold_table = f"{catalog}.{schema_gold}.near_time_sales_by_channel"

orders_df = spark.table(orders_table).withColumn(
    "order_timestamp", F.to_timestamp("order_timestamp")
)

sales_summary_df = build_near_time_sales_summary(
    orders_df,
    timestamp_column="order_timestamp",
    window_duration="15 minutes",
    group_by_columns=["channel"],
)

(
    sales_summary_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(sales_gold_table)
)

print(f"Agregação concluída: {sales_gold_table}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Visão 360 do cliente (com privacidade)
# MAGIC A tabela contém PII (`full_name`, `email`, `cpf`). Logo após gravá-la,
# MAGIC aplicamos as máscaras de coluna: usuários fora do grupo `admin_group`
# MAGIC continuam enxergando `total_orders`/`total_spent`/`loyalty_tier`
# MAGIC normalmente, mas veem o nome/e-mail/CPF ofuscados.

# COMMAND ----------

customers_table = f"{catalog}.{schema_silver}.customers"
customer_gold_table = f"{catalog}.{schema_gold}.customer_summary"

customers_df = spark.table(customers_table)
customer_summary_df = build_customer_summary(customers_df, orders_df)

(
    customer_summary_df.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(customer_gold_table)
)

apply_privacy_layer(
    spark,
    full_table_name=customer_gold_table,
    catalog=catalog,
    schema=schema_gold,
    table_columns=customer_summary_df.columns,
    admin_group=admin_group,
    read_groups=[analyst_group],
)

print(f"Agregação concluída: {customer_gold_table} (PII mascarado para fora de '{admin_group}')")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. (Opcional) Teste de estresse: data skew e pico de ingestão
# MAGIC Simula, sobre os mesmos pedidos, uma chave "quente" (produto viral) e um
# MAGIC pico de tráfego na abertura das ofertas — cenários comuns na Black
# MAGIC Friday. Só roda se `run_stress_test=true`; grava em tabelas separadas
# MAGIC para não sobrepor as métricas reais.

# COMMAND ----------

if run_stress_test:
    skewed_orders_df = simulate_data_skew(
        orders_df,
        key_column="product_id",
        hot_key_fraction=0.02,
        replication_factor=25,
    )

    black_friday_midnight = orders_df.agg(F.min("order_timestamp")).first()[0]
    spiked_orders_df = simulate_ingestion_spike(
        skewed_orders_df,
        timestamp_column="order_timestamp",
        spike_start=str(black_friday_midnight),
        spike_duration_minutes=5,
        spike_multiplier=15,
    )

    stress_summary_df = build_near_time_sales_summary(
        spiked_orders_df,
        timestamp_column="order_timestamp",
        window_duration="15 minutes",
        group_by_columns=["channel"],
    )

    stress_gold_table = f"{catalog}.{schema_gold}.near_time_sales_by_channel_stress_test"
    (
        stress_summary_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(stress_gold_table)
    )
    print(f"Teste de estresse concluído: {stress_gold_table}")
else:
    print("run_stress_test=false — pulando o cenário de skew/pico de ingestão")
