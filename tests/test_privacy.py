from data_engineering.gold.privacy import (
    PII_MASKS,
    apply_privacy_layer,
    build_apply_mask_sql,
    build_grant_sql,
    build_mask_function_sql,
)


class FakeSpark:
    """Dublê de SparkSession: só registra as instruções SQL executadas."""

    def __init__(self):
        self.executed: list[str] = []

    def sql(self, query: str):
        self.executed.append(query)
        return None


class FailingGrantFakeSpark:
    """Dublê que simula um GRANT falhando para um grupo inexistente no
    Unity Catalog (PRINCIPAL_DOES_NOT_EXIST) — cenário real de workspace
    sem os grupos de governança provisionados.
    """

    def __init__(self, failing_group: str):
        self.executed: list[str] = []
        self.failing_group = failing_group

    def sql(self, query: str):
        self.executed.append(query)
        if f"TO `{self.failing_group}`" in query:
            raise Exception(
                f"[PRINCIPAL_DOES_NOT_EXIST] Could not find principal with name "
                f"{self.failing_group}."
            )
        return None


def test_build_mask_function_sql_creates_one_function_per_pii_column():
    functions = build_mask_function_sql(catalog="dev", schema="gold", admin_group="admins")
    assert set(functions.keys()) == set(PII_MASKS.values())


def test_build_mask_function_sql_references_admin_group_and_qualified_name():
    functions = build_mask_function_sql(catalog="dev", schema="gold", admin_group="admins")
    for name, ddl in functions.items():
        assert f"dev.gold.{name}" in ddl
        assert "is_account_group_member('admins')" in ddl
        assert "CREATE OR REPLACE FUNCTION" in ddl


def test_build_mask_function_sql_uses_custom_admin_group():
    functions = build_mask_function_sql(
        catalog="dev", schema="gold", admin_group="data-privacy-admins"
    )
    assert all(
        "is_account_group_member('data-privacy-admins')" in ddl for ddl in functions.values()
    )


def test_build_apply_mask_sql_only_targets_known_pii_columns():
    statements = build_apply_mask_sql(
        full_table_name="dev.gold.customer_summary",
        catalog="dev",
        schema="gold",
        table_columns=["customer_id", "full_name", "email", "cpf", "loyalty_tier", "total_orders"],
    )
    # apenas full_name, email e cpf têm máscara conhecida
    assert len(statements) == 3
    joined = "\n".join(statements)
    assert "ALTER COLUMN full_name SET MASK dev.gold.mask_full_name" in joined
    assert "ALTER COLUMN email SET MASK dev.gold.mask_email" in joined
    assert "ALTER COLUMN cpf SET MASK dev.gold.mask_cpf" in joined
    assert "customer_id" not in joined
    assert "loyalty_tier" not in joined


def test_build_apply_mask_sql_empty_when_no_pii_columns_present():
    statements = build_apply_mask_sql(
        full_table_name="dev.gold.near_time_sales_by_channel",
        catalog="dev",
        schema="gold",
        table_columns=["window_start", "window_end", "channel", "order_count"],
    )
    assert statements == []


def test_build_grant_sql():
    statements = build_grant_sql("dev.gold.customer_summary", groups=["admins", "analysts"])
    assert statements == [
        "GRANT SELECT ON TABLE dev.gold.customer_summary TO `admins`",
        "GRANT SELECT ON TABLE dev.gold.customer_summary TO `analysts`",
    ]


def test_apply_privacy_layer_creates_functions_masks_and_grants():
    fake_spark = FakeSpark()

    apply_privacy_layer(
        fake_spark,
        full_table_name="dev.gold.customer_summary",
        catalog="dev",
        schema="gold",
        table_columns=["customer_id", "full_name", "email", "cpf", "total_orders"],
        admin_group="admins",
        read_groups=["analysts"],
    )

    # 3 funções de máscara + 3 ALTER TABLE (full_name/email/cpf) + 2 GRANTs
    assert len(fake_spark.executed) == 3 + 3 + 2
    assert any("CREATE OR REPLACE FUNCTION dev.gold.mask_cpf" in s for s in fake_spark.executed)
    assert any("ALTER COLUMN email SET MASK" in s for s in fake_spark.executed)
    assert "GRANT SELECT ON TABLE dev.gold.customer_summary TO `analysts`" in fake_spark.executed
    assert "GRANT SELECT ON TABLE dev.gold.customer_summary TO `admins`" in fake_spark.executed


def test_apply_privacy_layer_always_includes_admin_group_in_grants_even_if_omitted():
    fake_spark = FakeSpark()

    apply_privacy_layer(
        fake_spark,
        full_table_name="dev.gold.customer_summary",
        catalog="dev",
        schema="gold",
        table_columns=["full_name"],
        admin_group="admins",
        read_groups=None,
    )

    grants = [s for s in fake_spark.executed if s.startswith("GRANT")]
    assert grants == ["GRANT SELECT ON TABLE dev.gold.customer_summary TO `admins`"]


def test_apply_privacy_layer_skips_alter_table_when_no_pii_columns():
    fake_spark = FakeSpark()

    apply_privacy_layer(
        fake_spark,
        full_table_name="dev.gold.near_time_sales_by_channel",
        catalog="dev",
        schema="gold",
        table_columns=["window_start", "channel", "order_count"],
        admin_group="admins",
        read_groups=["analysts"],
    )

    alter_statements = [s for s in fake_spark.executed if s.startswith("ALTER TABLE")]
    assert alter_statements == []
    # as 3 funções de máscara ainda são criadas (idempotente, custo desprezível)
    create_statements = [s for s in fake_spark.executed if "CREATE OR REPLACE FUNCTION" in s]
    assert len(create_statements) == 3


def test_apply_privacy_layer_tolerates_grant_to_nonexistent_group(capsys):
    """Um grupo (ex.: 'analysts') pode ainda não existir no Unity Catalog do
    workspace — isso não deve derrubar o pipeline: as máscaras já foram
    aplicadas antes do grant, que é best-effort.
    """
    fake_spark = FailingGrantFakeSpark(failing_group="analysts")

    apply_privacy_layer(
        fake_spark,
        full_table_name="dev.gold.customer_summary",
        catalog="dev",
        schema="gold",
        table_columns=["full_name", "email", "cpf"],
        admin_group="admins",
        read_groups=["analysts"],
    )

    # máscaras continuam sendo criadas/aplicadas normalmente
    assert any("CREATE OR REPLACE FUNCTION" in s for s in fake_spark.executed)
    assert any(s.startswith("ALTER TABLE") for s in fake_spark.executed)
    # o grant para o grupo seguinte (admins) ainda é tentado, apesar da falha
    assert "GRANT SELECT ON TABLE dev.gold.customer_summary TO `admins`" in fake_spark.executed

    captured = capsys.readouterr()
    assert "analysts" in captured.out
    assert "PRINCIPAL_DOES_NOT_EXIST" in captured.out
