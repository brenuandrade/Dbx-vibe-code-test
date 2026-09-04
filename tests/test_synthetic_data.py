import datetime

from data_engineering.bronze.synthetic_data import (
    generate_addresses,
    generate_black_friday_dataset,
    generate_black_friday_orders,
    generate_customers,
    generate_products,
    generate_stores,
    get_black_friday_date,
)


def test_get_black_friday_date_is_last_friday_of_november():
    # Datas conhecidas publicamente (Black Friday = última sexta de novembro)
    assert get_black_friday_date(2023) == datetime.date(2023, 11, 24)
    assert get_black_friday_date(2024) == datetime.date(2024, 11, 29)
    assert get_black_friday_date(2026) == datetime.date(2026, 11, 27)


def test_get_black_friday_date_is_always_a_friday():
    for year in range(2020, 2031):
        assert get_black_friday_date(year).weekday() == 4


def test_generate_products_is_deterministic_with_same_seed():
    a = generate_products(50, seed=1)
    b = generate_products(50, seed=1)
    assert a == b


def test_generate_products_different_seed_differs():
    a = generate_products(50, seed=1)
    b = generate_products(50, seed=2)
    assert a != b


def test_generate_products_has_expected_fields_and_noise():
    products = generate_products(200, seed=1)
    assert len(products) >= 200  # duplicatas só aumentam a contagem
    sample = products[0]
    assert {"product_id", "sku", "product_name", "category", "unit_price"} <= sample.keys()
    # unit_price sempre string "crua" (ver dirty_currency)
    assert all(isinstance(p["unit_price"], str) for p in products)
    # deve haver algum brand nulo (ruído) numa amostra grande
    assert any(p["brand"] is None for p in products)


def test_generate_stores_basic_shape():
    stores = generate_stores(20, seed=1)
    assert len(stores) == 20
    assert {"store_id", "store_name", "city", "state"} <= stores[0].keys()


def test_generate_customers_has_duplicates_and_noise():
    customers = generate_customers(300, seed=1)
    assert len(customers) >= 300
    ids = [c["customer_id"] for c in customers]
    # duplicate_records deve gerar customer_id repetidos
    assert len(ids) != len(set(ids))
    assert any(c["phone"] is None or c["gender"] is None for c in customers)


def test_generate_addresses_reference_existing_and_orphan_customers():
    customers = generate_customers(100, seed=1)
    addresses = generate_addresses(customers, seed=2)
    valid_customer_ids = {c["customer_id"] for c in customers}
    assert len(addresses) >= len(customers)
    orphan_count = sum(1 for a in addresses if a["customer_id"] not in valid_customer_ids)
    assert orphan_count > 0  # ruído referencial esperado


def test_generate_black_friday_orders_grain_and_volume():
    customers = generate_customers(100, seed=1)
    products = generate_products(50, seed=2)
    stores = generate_stores(5, seed=3)
    black_friday = get_black_friday_date(2026)

    orders = generate_black_friday_orders(
        n_orders=500,
        customers=customers,
        products=products,
        stores=stores,
        black_friday=black_friday,
        seed=4,
    )

    assert len(orders) >= 500
    sample = orders[0]
    assert {
        "order_id",
        "customer_id",
        "store_id",
        "product_id",
        "quantity",
        "unit_price",
        "line_total",
        "order_timestamp",
        "source_ingested_at",
    } <= sample.keys()

    # a maioria dos pedidos deve cair no dia da Black Friday (ou nos vizinhos)
    order_dates = {datetime.date.fromisoformat(o["order_timestamp"][:10]) for o in orders}
    expected_dates = {
        black_friday - datetime.timedelta(days=1),
        black_friday,
        black_friday + datetime.timedelta(days=1),
    }
    assert order_dates <= expected_dates


def test_generate_black_friday_orders_ingested_at_is_never_before_order_timestamp():
    customers = generate_customers(50, seed=1)
    products = generate_products(20, seed=2)
    stores = generate_stores(3, seed=3)
    black_friday = get_black_friday_date(2026)

    orders = generate_black_friday_orders(
        n_orders=200,
        customers=customers,
        products=products,
        stores=stores,
        black_friday=black_friday,
        seed=4,
    )

    for order in orders:
        order_ts = datetime.datetime.fromisoformat(order["order_timestamp"])
        ingested_at = datetime.datetime.fromisoformat(order["source_ingested_at"])
        assert ingested_at >= order_ts


def test_generate_black_friday_dataset_returns_all_entities():
    dataset = generate_black_friday_dataset(
        n_customers=50, n_products=20, n_stores=5, n_orders=100, year=2026, seed=1
    )
    assert set(dataset.keys()) == {"products", "stores", "customers", "addresses", "orders"}
    assert all(len(records) > 0 for records in dataset.values())


def test_generate_black_friday_dataset_is_reproducible():
    a = generate_black_friday_dataset(
        n_customers=30, n_products=10, n_stores=3, n_orders=50, year=2026, seed=99
    )
    b = generate_black_friday_dataset(
        n_customers=30, n_products=10, n_stores=3, n_orders=50, year=2026, seed=99
    )
    assert a == b
