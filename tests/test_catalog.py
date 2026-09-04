from data_engineering.utils.catalog import ensure_catalog_schema


class FakeSpark:
    def __init__(self):
        self.executed: list[str] = []

    def sql(self, query: str):
        self.executed.append(query)
        return None


def test_ensure_catalog_schema_creates_catalog_and_schema():
    fake_spark = FakeSpark()

    ensure_catalog_schema(fake_spark, catalog="dev", schema="bronze")

    assert fake_spark.executed == [
        "CREATE CATALOG IF NOT EXISTS dev",
        "CREATE SCHEMA IF NOT EXISTS dev.bronze",
    ]


def test_ensure_catalog_schema_is_idempotent_call_shape():
    fake_spark = FakeSpark()

    ensure_catalog_schema(fake_spark, catalog="prod", schema="gold")
    ensure_catalog_schema(fake_spark, catalog="prod", schema="gold")

    assert fake_spark.executed.count("CREATE CATALOG IF NOT EXISTS prod") == 2
    assert fake_spark.executed.count("CREATE SCHEMA IF NOT EXISTS prod.gold") == 2
