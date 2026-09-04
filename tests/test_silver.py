from data_engineering.silver.transform import (
    clean_bronze_to_silver,
    drop_duplicate_records,
    standardize_nulls,
    trim_string_columns,
)


def test_drop_duplicate_records_keeps_most_recent(spark):
    df = spark.createDataFrame(
        [
            (1, "old", "2024-01-01T00:00:00"),
            (1, "new", "2024-02-01T00:00:00"),
            (2, "only", "2024-01-15T00:00:00"),
        ],
        ["id", "value", "updated_at"],
    )

    result = drop_duplicate_records(df, key_columns=["id"], order_by_column="updated_at")

    rows = {r["id"]: r["value"] for r in result.collect()}
    assert rows == {1: "new", 2: "only"}
    assert result.count() == 2


def test_trim_string_columns(spark):
    df = spark.createDataFrame([("  hello  ", 1)], ["text", "num"])

    result = trim_string_columns(df)

    assert result.collect()[0]["text"] == "hello"


def test_standardize_nulls(spark):
    df = spark.createDataFrame([("N/A",), ("",), ("value",)], ["col"])

    result = standardize_nulls(df, columns=["col"])

    values = [r["col"] for r in result.collect()]
    assert values == [None, None, "value"]


def test_clean_bronze_to_silver_end_to_end(spark):
    df = spark.createDataFrame(
        [
            (1, "  a  ", "2024-01-01T00:00:00"),
            (1, "a-newer", "2024-02-01T00:00:00"),
        ],
        ["id", "value", "ts"],
    )

    result = clean_bronze_to_silver(df, key_columns=["id"], order_by_column="ts")

    assert result.count() == 1
    assert result.collect()[0]["value"] == "a-newer"
