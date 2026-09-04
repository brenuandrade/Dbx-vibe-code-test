from pyspark.sql import functions as F

from data_engineering.gold.aggregate import build_daily_summary, build_near_time_sales_summary


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
