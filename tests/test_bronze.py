from data_engineering.bronze.ingest import ingest_raw_source


def test_ingest_raw_source_adds_audit_columns(spark):
    df = spark.createDataFrame([(1, "a"), (2, "b")], ["id", "value"])

    result = ingest_raw_source(df, source_name="test_source")

    assert "_source" in result.columns
    assert "_ingested_at" in result.columns
    rows = result.select("_source").distinct().collect()
    assert len(rows) == 1
    assert rows[0]["_source"] == "test_source"
