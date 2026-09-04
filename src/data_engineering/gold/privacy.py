"""Camada de privacidade para tabelas Gold: mascaramento de colunas PII via
Unity Catalog Column Masks.

Colunas sensíveis (nome completo, e-mail, CPF) ficam ofuscadas por padrão;
apenas membros do grupo administrativo (checado via
``is_account_group_member`` do Unity Catalog) veem o valor real. O controle
é feito no nível da **coluna**, não da tabela: usuários comuns continuam
enxergando o restante da tabela Gold normalmente (métricas agregadas,
`loyalty_tier` etc.) — apenas os campos PII saem mascarados. Isso evita
bloquear o SELECT na tabela inteira, o que impediria o uso legítimo de
relatórios que não precisam do dado sensível.

As funções aqui são deliberadamente divididas em duas categorias:

- ``build_*``: montam e retornam a DDL como string, sem tocar em Spark —
  puras e testáveis sem cluster nem Unity Catalog.
- ``apply_privacy_layer``: executa a DDL via ``spark.sql(...)`` — depende de
  um workspace com Unity Catalog habilitado e dos grupos referenciados já
  existirem (grupos inexistentes apenas fazem ``is_account_group_member``
  retornar falso, ou seja, o comportamento falha "fechado": sem o grupo
  configurado, ninguém vê o dado real).
"""

from __future__ import annotations

from typing import Any, Protocol


class SqlExecutor(Protocol):
    """Qualquer objeto com um método ``sql(query)`` — tipicamente uma
    ``SparkSession``. Definido como Protocol para permitir testar
    ``apply_privacy_layer`` com um dublê simples, sem precisar de um cluster.
    """

    def sql(self, query: str) -> Any: ...


# Colunas PII conhecidas nas tabelas Gold e o nome da função de máscara
# aplicável a cada uma (criada por `build_mask_function_sql`).
PII_MASKS = {
    "full_name": "mask_full_name",
    "email": "mask_email",
    "cpf": "mask_cpf",
}


def build_mask_function_sql(catalog: str, schema: str, admin_group: str) -> dict[str, str]:
    """Monta a DDL das funções de máscara (uma por tipo de PII conhecido).

    Cada função libera o valor real apenas para membros de ``admin_group``;
    para os demais, aplica uma ofuscação parcial — mantém o suficiente para
    depuração/suporte de negócio sem expor o dado sensível por completo.

    Retorna ``{nome_da_funcao: DDL}``.
    """
    qualified = f"{catalog}.{schema}"
    return {
        "mask_full_name": f"""
CREATE OR REPLACE FUNCTION {qualified}.mask_full_name(full_name STRING)
RETURNS STRING
COMMENT 'Mascara nome completo para quem nao pertence ao grupo administrativo'
RETURN CASE
  WHEN is_account_group_member('{admin_group}') THEN full_name
  WHEN full_name IS NULL THEN NULL
  ELSE CONCAT(SPLIT_PART(full_name, ' ', 1), ' ***')
END
""".strip(),
        "mask_email": f"""
CREATE OR REPLACE FUNCTION {qualified}.mask_email(email STRING)
RETURNS STRING
COMMENT 'Mascara e-mail para quem nao pertence ao grupo administrativo'
RETURN CASE
  WHEN is_account_group_member('{admin_group}') THEN email
  WHEN email IS NULL THEN NULL
  ELSE CONCAT('***@', SPLIT_PART(email, '@', -1))
END
""".strip(),
        "mask_cpf": f"""
CREATE OR REPLACE FUNCTION {qualified}.mask_cpf(cpf STRING)
RETURNS STRING
COMMENT 'Mascara CPF para quem nao pertence ao grupo administrativo'
RETURN CASE
  WHEN is_account_group_member('{admin_group}') THEN cpf
  WHEN cpf IS NULL THEN NULL
  ELSE CONCAT('***.***.***-', RIGHT(cpf, 2))
END
""".strip(),
    }


def build_apply_mask_sql(
    full_table_name: str, catalog: str, schema: str, table_columns: list[str]
) -> list[str]:
    """Monta os ``ALTER TABLE ... ALTER COLUMN ... SET MASK`` para as
    colunas PII presentes em ``table_columns`` (apenas as que têm máscara
    conhecida em ``PII_MASKS`` — colunas sem PII são ignoradas).
    """
    statements = []
    for column in table_columns:
        mask_function = PII_MASKS.get(column)
        if mask_function:
            statements.append(
                f"ALTER TABLE {full_table_name} "
                f"ALTER COLUMN {column} SET MASK {catalog}.{schema}.{mask_function}"
            )
    return statements


def build_grant_sql(full_table_name: str, groups: list[str]) -> list[str]:
    """Monta os ``GRANT SELECT`` na tabela para os grupos informados.

    A diferenciação de acesso é feita pela máscara por coluna, não pelo
    GRANT: todo grupo com acesso legítimo à tabela recebe SELECT; a
    visibilidade do dado PII depende de o usuário pertencer (ou não) ao
    grupo administrativo usado nas funções de máscara.
    """
    return [f"GRANT SELECT ON TABLE {full_table_name} TO `{group}`" for group in groups]


def apply_privacy_layer(
    spark: SqlExecutor,
    full_table_name: str,
    catalog: str,
    schema: str,
    table_columns: list[str],
    admin_group: str = "admins",
    read_groups: list[str] | None = None,
) -> None:
    """Aplica a camada de privacidade completa a uma tabela Gold:

    1. Cria (ou substitui) as funções de máscara no catálogo/schema informados.
    2. Aplica a máscara a cada coluna PII presente em ``table_columns``.
    3. Concede SELECT na tabela a ``read_groups`` (o grupo administrativo é
       sempre incluído, para que ele também consiga consultar a tabela —
       vendo os dados sem máscara).

    Idempotente: usa ``CREATE OR REPLACE FUNCTION`` e ``SET MASK``, seguros
    de reexecutar a cada deploy/execução do pipeline.

    O ``GRANT`` a cada grupo em ``read_groups``/``admin_group`` é best-effort:
    se um grupo ainda não existir no Unity Catalog (``PRINCIPAL_DOES_NOT_EXIST``
    — comum enquanto os grupos de governança não foram provisionados no
    workspace), a falha é registrada e a execução segue para os próximos
    grants, em vez de derrubar o pipeline inteiro. As máscaras — a parte que
    de fato protege o PII — já foram criadas e aplicadas antes deste passo,
    então continuam valendo mesmo se algum grant falhar.
    """
    read_groups = list(read_groups or [])
    if admin_group not in read_groups:
        read_groups.append(admin_group)

    for ddl in build_mask_function_sql(catalog, schema, admin_group).values():
        spark.sql(ddl)

    for statement in build_apply_mask_sql(full_table_name, catalog, schema, table_columns):
        spark.sql(statement)

    for statement in build_grant_sql(full_table_name, read_groups):
        try:
            spark.sql(statement)
        except Exception as exc:  # noqa: BLE001 - best-effort, ver docstring
            print(f"[privacy] Aviso: falha ao conceder acesso ('{statement}'): {exc}")
