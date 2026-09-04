"""Utilitários para injetar ruído realista em dados sintéticos.

Simula os tipos de sujeira que aparecem em dados de produção — valores
ausentes, formatos inconsistentes, duplicatas por reenvio/retry e
inconsistências referenciais entre tabelas — comuns em integrações que
recebem picos de carga, como o cenário de acompanhamento near-time da
Black Friday. Cada função aqui é pequena e testável isoladamente; os
geradores em ``synthetic_data.py`` as combinam para montar os datasets
Bronze.

Convenção: as funções que recebem uma lista de registros mutam os dicts
*in place* e retornam a própria lista, exceto ``duplicate_records`` (que
necessariamente retorna uma lista maior).
"""

from __future__ import annotations

import random
import re
from typing import Any, Callable, Sequence

Record = dict[str, Any]


def chance(probability: float, rng: random.Random) -> bool:
    """Retorna True com a probabilidade informada (0.0 a 1.0)."""
    return rng.random() < probability


def messy_whitespace(value: Any, rng: random.Random) -> Any:
    """Adiciona espaços extras nas bordas, simulando digitação/import descuidado."""
    if not isinstance(value, str):
        return value
    pad_left = " " * rng.randint(0, 2)
    pad_right = " " * rng.randint(0, 2)
    return f"{pad_left}{value}{pad_right}"


def random_case(value: Any, rng: random.Random) -> Any:
    """Randomiza a caixa (maiúsculas/minúsculas/título), comum em integrações legadas."""
    if not isinstance(value, str):
        return value
    style = rng.choice(["upper", "lower", "title", "keep"])
    if style == "upper":
        return value.upper()
    if style == "lower":
        return value.lower()
    if style == "title":
        return value.title()
    return value


def noisy_enum(value: Any, rng: random.Random) -> Any:
    """Varia a formatação de um valor categórico (status, canal etc.)."""
    return random_case(messy_whitespace(value, rng), rng)


def corrupt_email(email: str, rng: random.Random) -> str:
    """Introduz erros comuns de digitação/importação em e-mails."""
    corruptions: list[Callable[[str], str]] = [
        lambda e: e.replace("@", " at "),
        lambda e: e.replace(".com", ".con"),
        lambda e: e.upper(),
        lambda e: e.replace("@", ""),
        lambda e: f" {e} ",
    ]
    return rng.choice(corruptions)(email)


def corrupt_phone(phone: str, rng: random.Random) -> str:
    """Gera variações de formatação de telefone (com/sem DDI, separadores, dígito faltando)."""
    digits = re.sub(r"\D", "", phone)
    formats: list[Callable[[str], str]] = [
        lambda d: d,
        lambda d: f"({d[:2]}) {d[2:7]}-{d[7:]}" if len(d) >= 11 else d,
        lambda d: f"+55{d}",
        lambda d: d[:-1],
    ]
    return rng.choice(formats)(digits)


def corrupt_cep(cep: str, rng: random.Random) -> str:
    """Gera variações/erros de CEP: sem hífen, dígito faltando, espaço sobrando."""
    digits = re.sub(r"\D", "", cep)
    formats: list[Callable[[str], str]] = [
        lambda d: f"{d[:5]}-{d[5:]}",
        lambda d: d,
        lambda d: d[:-1],
        lambda d: f"{d[:5]}-{d[5:]} ",
    ]
    return rng.choice(formats)(digits)


def dirty_currency(value: float, rng: random.Random) -> str:
    """Representa um valor monetário como string "crua", do jeito que chegaria
    de sistemas de origem heterogêneos: às vezes em formato pt-BR ("R$ 1.234,56"),
    às vezes plano ("1234.56"), ocasionalmente negativo por erro de digitação.

    Retorna sempre ``str`` (nunca ``float``) — de propósito: colunas monetárias
    vindas de fontes distintas raramente chegam já tipadas de forma consistente
    na Bronze; a conversão/validação para numérico é responsabilidade da Silver
    (ver ``parse_dirty_price``).
    """
    if chance(0.05, rng):
        value = -abs(value)
    if chance(0.5, rng):
        formatted = f"{value:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
        return f"R$ {formatted}"
    return f"{value:.2f}"


def parse_dirty_price(value: Any) -> float | None:
    """Converte um valor monetário "sujo" (string BRL, número puro, ou nulo) em ``float``.

    Espelha o tipo de parsing defensivo que a camada Silver precisa aplicar
    sobre uma coluna Bronze gerada por ``dirty_currency``. Retorna ``None``
    quando o valor não pode ser interpretado.
    """
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).replace("R$", "").strip()
    if not text:
        return None
    if "," in text:
        # formato brasileiro: milhar com "." e decimal com ","
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def duplicate_records(records: list[Record], rate: float, rng: random.Random) -> list[Record]:
    """Duplica uma fração dos registros, simulando reenvios/retries de sistemas
    upstream sob carga — cenário clássico de falha de idempotência em picos de
    tráfego (ex.: Black Friday). Retorna uma lista nova, maior que a original.
    """
    duplicates = [dict(record) for record in records if chance(rate, rng)]
    return records + duplicates


def inject_orphan_foreign_keys(
    records: list[Record],
    fk_field: str,
    rate: float,
    rng: random.Random,
    fake_id_factory: Callable[[], Any],
) -> list[Record]:
    """Substitui o valor de uma FK por um ID inexistente em parte dos registros.

    Simula inconsistência referencial entre sistemas de origem — por exemplo,
    um pedido referenciando um cliente que ainda não foi propagado (ou já foi
    removido) no sistema de cadastro.
    """
    for record in records:
        if chance(rate, rng):
            record[fk_field] = fake_id_factory()
    return records


def inject_missing_fields(
    records: list[Record], fields: Sequence[str], rate: float, rng: random.Random
) -> list[Record]:
    """Define como ``None`` campos específicos em uma fração dos registros."""
    for record in records:
        for field in fields:
            if chance(rate, rng):
                record[field] = None
    return records
