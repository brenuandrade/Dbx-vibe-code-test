"""Validações simples de qualidade de dados usadas entre as camadas Silver e Gold."""

from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import DataFrame


@dataclass
class DataQualityResult:
    check_name: str
    passed: bool
    details: str = ""


def check_no_nulls(df: DataFrame, columns: list[str]) -> DataQualityResult:
    """Falha se alguma das colunas informadas tiver valores nulos."""
    for column in columns:
        null_count = df.filter(df[column].isNull()).count()
        if null_count > 0:
            return DataQualityResult(
                check_name="check_no_nulls",
                passed=False,
                details=f"Coluna '{column}' possui {null_count} valores nulos.",
            )
    return DataQualityResult(check_name="check_no_nulls", passed=True)


def check_no_duplicates(df: DataFrame, key_columns: list[str]) -> DataQualityResult:
    """Falha se houver linhas duplicadas considerando as colunas-chave."""
    total = df.count()
    distinct = df.select(*key_columns).distinct().count()
    if total != distinct:
        return DataQualityResult(
            check_name="check_no_duplicates",
            passed=False,
            details=f"{total - distinct} linhas duplicadas encontradas para {key_columns}.",
        )
    return DataQualityResult(check_name="check_no_duplicates", passed=True)


def run_checks(results: list[DataQualityResult]) -> None:
    """Levanta uma exceção se qualquer verificação de qualidade tiver falhado."""
    failures = [r for r in results if not r.passed]
    if failures:
        messages = "\n".join(f"- {f.check_name}: {f.details}" for f in failures)
        raise ValueError(f"Falhas de qualidade de dados detectadas:\n{messages}")
