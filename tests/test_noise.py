import random

from data_engineering.bronze.noise import (
    chance,
    corrupt_cep,
    corrupt_email,
    corrupt_phone,
    dirty_currency,
    duplicate_records,
    inject_missing_fields,
    inject_orphan_foreign_keys,
    parse_dirty_price,
)


def test_chance_is_deterministic_with_seed():
    rng_a = random.Random(1)
    rng_b = random.Random(1)
    results_a = [chance(0.5, rng_a) for _ in range(20)]
    results_b = [chance(0.5, rng_b) for _ in range(20)]
    assert results_a == results_b


def test_corrupt_email_still_contains_original_local_part_or_variation():
    rng = random.Random(7)
    email = "joao.silva@example.com"
    corrupted = corrupt_email(email, rng)
    assert isinstance(corrupted, str)
    assert corrupted != ""


def test_corrupt_phone_strips_non_digits_before_reformatting():
    rng = random.Random(7)
    for _ in range(20):
        result = corrupt_phone("11987654321", rng)
        assert isinstance(result, str)


def test_corrupt_cep_variants():
    rng = random.Random(7)
    for _ in range(20):
        result = corrupt_cep("01310-100", rng)
        assert isinstance(result, str)


def test_dirty_currency_always_returns_string():
    rng = random.Random(3)
    for _ in range(50):
        result = dirty_currency(199.9, rng)
        assert isinstance(result, str)


def test_parse_dirty_price_handles_brl_format():
    assert parse_dirty_price("R$ 1.234,56") == 1234.56


def test_parse_dirty_price_handles_plain_string():
    assert parse_dirty_price("199.90") == 199.90


def test_parse_dirty_price_handles_numeric_input():
    assert parse_dirty_price(42) == 42.0
    assert parse_dirty_price(42.5) == 42.5


def test_parse_dirty_price_handles_none_and_invalid():
    assert parse_dirty_price(None) is None
    assert parse_dirty_price("não é preço") is None
    assert parse_dirty_price("") is None


def test_duplicate_records_only_grows_the_list():
    rng = random.Random(5)
    records = [{"id": i} for i in range(100)]
    result = duplicate_records(records, rate=0.3, rng=rng)
    assert len(result) >= len(records)
    # todo registro duplicado deve ser uma cópia de um registro original
    original_ids = {r["id"] for r in records}
    assert all(r["id"] in original_ids for r in result)


def test_duplicate_records_zero_rate_returns_same_length():
    rng = random.Random(5)
    records = [{"id": i} for i in range(50)]
    result = duplicate_records(records, rate=0.0, rng=rng)
    assert len(result) == len(records)


def test_inject_orphan_foreign_keys_replaces_some_values():
    rng = random.Random(9)
    records = [{"customer_id": f"CUST-{i}"} for i in range(200)]
    valid_ids = {r["customer_id"] for r in records}
    result = inject_orphan_foreign_keys(
        records, "customer_id", rate=0.5, rng=rng, fake_id_factory=lambda: "ORPHAN-ID"
    )
    orphan_count = sum(1 for r in result if r["customer_id"] not in valid_ids)
    assert orphan_count > 0


def test_inject_orphan_foreign_keys_zero_rate_changes_nothing():
    rng = random.Random(9)
    records = [{"customer_id": f"CUST-{i}"} for i in range(50)]
    valid_ids = {r["customer_id"] for r in records}
    result = inject_orphan_foreign_keys(
        records, "customer_id", rate=0.0, rng=rng, fake_id_factory=lambda: "ORPHAN-ID"
    )
    assert all(r["customer_id"] in valid_ids for r in result)


def test_inject_missing_fields_sets_none_for_some_records():
    rng = random.Random(11)
    records = [{"phone": "11999999999"} for _ in range(200)]
    result = inject_missing_fields(records, ["phone"], rate=0.5, rng=rng)
    none_count = sum(1 for r in result if r["phone"] is None)
    assert none_count > 0
    assert none_count < len(result)
