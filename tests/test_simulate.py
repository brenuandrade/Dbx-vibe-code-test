from data_engineering.gold.simulate import simulate_data_skew, simulate_ingestion_spike


def test_simulate_data_skew_concentrates_volume_on_hot_keys(spark):
    # 100 chaves, 10 linhas cada — distribuição uniforme antes do skew
    rows = [(f"KEY-{k}", i) for k in range(100) for i in range(10)]
    df = spark.createDataFrame(rows, ["key", "value"])

    result = simulate_data_skew(
        df, key_column="key", hot_key_fraction=0.05, replication_factor=20, seed=1
    )

    counts = {r["key"]: r["count"] for r in result.groupBy("key").count().collect()}
    hot_counts = sorted(counts.values(), reverse=True)[:5]  # 5% de 100 chaves = 5
    cold_counts = sorted(counts.values())[:50]

    # linhas totais aumentaram (replicação das chaves quentes)
    assert result.count() > df.count()
    # as chaves quentes concentram muito mais volume que as demais
    assert min(hot_counts) > max(cold_counts)


def test_simulate_data_skew_is_deterministic_with_same_seed(spark):
    rows = [(f"KEY-{k}", i) for k in range(50) for i in range(5)]
    df = spark.createDataFrame(rows, ["key", "value"])

    result_a = simulate_data_skew(df, key_column="key", seed=7)
    result_b = simulate_data_skew(df, key_column="key", seed=7)

    assert result_a.count() == result_b.count()
    assert sorted(r["key"] for r in result_a.collect()) == sorted(
        r["key"] for r in result_b.collect()
    )


def test_simulate_data_skew_keeps_non_hot_rows_untouched(spark):
    rows = [(f"KEY-{k}", i) for k in range(20) for i in range(3)]
    df = spark.createDataFrame(rows, ["key", "value"])

    result = simulate_data_skew(df, key_column="key", hot_key_fraction=0.1, replication_factor=10)

    cold_key_total_before = df.filter(df["key"] == "KEY-19").count()  # pode ou não ser hot
    # todas as chaves continuam presentes (nenhuma linha é descartada, só replicada)
    assert set(r["key"] for r in df.collect()) == set(r["key"] for r in result.collect())
    assert cold_key_total_before > 0


def test_simulate_ingestion_spike_multiplies_rows_inside_window(spark):
    rows = [
        ("2026-11-27T00:02:00", "ORD-1"),
        ("2026-11-27T00:03:00", "ORD-2"),
        ("2026-11-27T10:00:00", "ORD-3"),
        ("2026-11-27T14:00:00", "ORD-4"),
    ]
    df = spark.createDataFrame(rows, ["order_timestamp", "order_id"])

    result = simulate_ingestion_spike(
        df,
        timestamp_column="order_timestamp",
        spike_start="2026-11-27T00:00:00",
        spike_duration_minutes=5,
        spike_multiplier=10,
    )

    # 2 linhas na janela (00:02 e 00:03) x10 + 2 linhas fora da janela
    assert result.count() == 2 * 10 + 2
    outside_window = result.filter(
        ~result["order_timestamp"].isin("2026-11-27T00:02:00", "2026-11-27T00:03:00")
    )
    assert outside_window.count() == 2


def test_simulate_ingestion_spike_adds_extra_lag_to_source_ingested_at(spark):
    rows = [
        ("2026-11-27T00:02:00", "2026-11-27T00:02:05", "ORD-1"),
        ("2026-11-27T10:00:00", "2026-11-27T10:00:03", "ORD-2"),
    ]
    df = spark.createDataFrame(rows, ["order_timestamp", "source_ingested_at", "order_id"])

    result = simulate_ingestion_spike(
        df,
        timestamp_column="order_timestamp",
        spike_start="2026-11-27T00:00:00",
        spike_duration_minutes=5,
        spike_multiplier=3,
        extra_lag_seconds=120,
    )

    spiked_rows = result.filter(result["order_id"] == "ORD-1").collect()
    assert len(spiked_rows) == 3
    assert all(r["source_ingested_at"] == "2026-11-27 00:04:05" for r in spiked_rows)

    untouched_row = result.filter(result["order_id"] == "ORD-2").collect()
    assert len(untouched_row) == 1
    assert untouched_row[0]["source_ingested_at"] == "2026-11-27T10:00:03"
