from pyspark.sql import functions as F

from data_engineering.gold.aggregate import (
    build_customer_summary,
    build_daily_summary,
    build_near_time_sales_summary,
)


def test_build_daily_summary(spark):
    df = spark.createDataFrame(
        [
            ("2024-01-01T10:00:00", "group_a", 10),
            ("2024-01-01T12:00:00", "group_a", 20),
            ("2024-01-02T08:00:00", "group_a", 5),
        ],
        ["event_ts", "group_col", "value"],
    )

    result = build_daily_summary(
        df, date_column="event_ts", group_by_columns=["group_col"], value_column="value"
    )

    rows = {(r["event_date"].isoformat(), r["group_col"]): r for r in result.collect()}
    day1 = rows[("2024-01-01", "group_a")]
    assert day1["value_total"] == 30
    assert day1["record_count"] == 2

    day2 = rows[("2024-01-02", "group_a")]
    assert day2["value_total"] == 5
    assert day2["record_count"] == 1


def test_build_near_time_sales_summary_aggregates_by_15_minute_window(spark):
    df = spark.createDataFrame(
        [
            ("ORD-1", "2026-11-27T00:01:00", "ecommerce", 1, 100.0),
            ("ORD-1", "2026-11-27T00:01:00", "ecommerce", 2, 50.0),  # 2ª linha do mesmo pedido
            ("ORD-2", "2026-11-27T00:05:00", "ecommerce", 1, 200.0),
            ("ORD-3", "2026-11-27T00:20:00", "loja_fisica", 3, 30.0),
        ],
        ["order_id", "order_timestamp", "channel", "quantity", "line_total"],
    ).withColumn("order_timestamp", F.to_timestamp("order_timestamp"))

    result = build_near_time_sales_summary(
        df,
        timestamp_column="order_timestamp",
        window_duration="15 minutes",
        group_by_columns=["channel"],
    )
    rows = result.collect()

    ecommerce_first_window = next(
        r for r in rows if r["channel"] == "ecommerce" and r["window_start"].minute == 0
    )
    assert ecommerce_first_window["order_count"] == 2  # ORD-1 e ORD-2, distintos
    assert ecommerce_first_window["items_sold"] == 4  # 1 + 2 + 1
    assert ecommerce_first_window["gross_revenue"] == 350.0

    loja_window = next(r for r in rows if r["channel"] == "loja_fisica")
    assert loja_window["order_count"] == 1
    assert loja_window["items_sold"] == 3
    assert loja_window["gross_revenue"] == 30.0


def test_build_customer_summary_aggregates_orders_per_customer(spark):
    customers_df = spark.createDataFrame(
        [
            ("CUST-1", "Ana Silva", "ana@example.com", "11111111111", "ouro"),
            ("CUST-2", "Bruno Costa", "bruno@example.com", "22222222222", "bronze"),
        ],
        ["customer_id", "full_name", "email", "cpf", "loyalty_tier"],
    )
    orders_df = spark.createDataFrame(
        [
            ("ORD-1", "CUST-1", "2026-11-27T00:01:00", 100.0),
            ("ORD-1", "CUST-1", "2026-11-27T00:01:00", 50.0),  # 2ª linha do mesmo pedido
            ("ORD-2", "CUST-1", "2026-11-27T05:00:00", 200.0),
        ],
        ["order_id", "customer_id", "order_timestamp", "line_total"],
    )

    result = build_customer_summary(customers_df, orders_df)
    rows = {r["customer_id"]: r for r in result.collect()}

    cust1 = rows["CUST-1"]
    assert cust1["total_orders"] == 2  # ORD-1 e ORD-2, distintos
    assert cust1["total_spent"] == 350.0
    assert cust1["full_name"] == "Ana Silva"
    assert cust1["email"] == "ana@example.com"
    assert cust1["cpf"] == "11111111111"

    # cliente sem pedidos: métricas ficam zeradas, não nulas
    cust2 = rows["CUST-2"]
    assert cust2["total_orders"] == 0
    assert cust2["total_spent"] == 0.0
