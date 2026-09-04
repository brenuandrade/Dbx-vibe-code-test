from data_engineering.gold.aggregate import build_daily_summary


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
