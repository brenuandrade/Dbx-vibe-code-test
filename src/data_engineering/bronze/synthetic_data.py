"""Geração de dados sintéticos para o cenário de acompanhamento near-time da Black Friday.

Simula o fluxo de dados de um varejo — produtos, lojas, consumidores,
endereços e pedidos — chegando na camada Bronze. Os geradores produzem
listas de ``dict`` (sem dependência de Spark, para serem testados sem
cluster) e já incluem ruído realista via ``data_engineering.bronze.noise``,
refletindo o tipo de sujeira observada em integrações de produção sob alta
carga: valores ausentes, formatos inconsistentes, duplicatas de reenvio e
inconsistências referenciais entre tabelas.

A entidade de pedidos (``orders``) é gerada no grão de item de pedido —
o formato típico de um tópico de eventos de checkout — com timestamps
concentrados no dia da Black Friday e um pequeno atraso de ingestão
simulando a chegada near-time dos dados na Bronze.
"""

from __future__ import annotations

import datetime
import random
from typing import Any

from data_engineering.bronze.noise import (
    chance,
    corrupt_cep,
    corrupt_email,
    corrupt_phone,
    dirty_currency,
    duplicate_records,
    inject_missing_fields,
    inject_orphan_foreign_keys,
    messy_whitespace,
    noisy_enum,
    parse_dirty_price,
)

Record = dict[str, Any]

# --- Dados de referência (curados, pt-BR) -----------------------------------

FIRST_NAMES = [
    "Ana",
    "Bruno",
    "Carlos",
    "Daniela",
    "Eduardo",
    "Fernanda",
    "Gabriel",
    "Helena",
    "Igor",
    "Juliana",
    "Lucas",
    "Mariana",
    "Nicolas",
    "Otavio",
    "Patricia",
    "Rafael",
    "Sabrina",
    "Thiago",
    "Vanessa",
    "William",
]
LAST_NAMES = [
    "Silva",
    "Santos",
    "Oliveira",
    "Souza",
    "Rodrigues",
    "Ferreira",
    "Alves",
    "Pereira",
    "Lima",
    "Gomes",
    "Costa",
    "Ribeiro",
    "Martins",
    "Carvalho",
    "Almeida",
    "Barbosa",
    "Araujo",
    "Nascimento",
    "Correia",
    "Teixeira",
]

STATES = [
    ("SP", "São Paulo"),
    ("RJ", "Rio de Janeiro"),
    ("MG", "Minas Gerais"),
    ("RS", "Rio Grande do Sul"),
    ("PR", "Paraná"),
    ("SC", "Santa Catarina"),
    ("BA", "Bahia"),
    ("PE", "Pernambuco"),
    ("CE", "Ceará"),
    ("DF", "Distrito Federal"),
]
CITIES_BY_STATE = {
    "SP": ["São Paulo", "Campinas", "Santos", "Sorocaba"],
    "RJ": ["Rio de Janeiro", "Niterói", "Duque de Caxias"],
    "MG": ["Belo Horizonte", "Uberlândia", "Contagem"],
    "RS": ["Porto Alegre", "Caxias do Sul", "Pelotas"],
    "PR": ["Curitiba", "Londrina", "Maringá"],
    "SC": ["Florianópolis", "Joinville", "Blumenau"],
    "BA": ["Salvador", "Feira de Santana"],
    "PE": ["Recife", "Olinda"],
    "CE": ["Fortaleza", "Juazeiro do Norte"],
    "DF": ["Brasília"],
}
STREET_TYPES = ["Rua", "Avenida", "Alameda", "Travessa"]
STREET_NAMES = [
    "das Flores",
    "Sete de Setembro",
    "Brasil",
    "Paulista",
    "das Palmeiras",
    "Rio Branco",
    "Getúlio Vargas",
    "Amazonas",
    "Independência",
    "das Acácias",
]
NEIGHBORHOODS = ["Centro", "Jardim das Américas", "Boa Vista", "Vila Nova", "Santa Cruz"]

# (categoria, subcategoria, marcas, nomes de produto)
PRODUCT_CATALOG = [
    (
        "Eletrônicos",
        "Smartphones",
        ["Samsung", "Apple", "Motorola", "Xiaomi"],
        ["Galaxy S24", "iPhone 15", "Moto G84", "Redmi Note 13"],
    ),
    (
        "Eletrônicos",
        "TVs",
        ["Samsung", "LG", "Philco", "AOC"],
        ['TV 50" 4K', 'TV 55" QLED', 'TV 43" Smart', 'TV 65" 4K'],
    ),
    (
        "Eletrodomésticos",
        "Linha Branca",
        ["Brastemp", "Electrolux", "Consul"],
        ["Geladeira Frost Free", "Máquina de Lavar 12kg", "Micro-ondas 30L"],
    ),
    (
        "Moda",
        "Calçados",
        ["Nike", "Adidas", "Vans", "Puma"],
        ["Tênis Running", "Tênis Casual", "Chinelo Slide"],
    ),
    (
        "Moda",
        "Vestuário",
        ["Hering", "Renner", "C&A"],
        ["Camiseta Básica", "Calça Jeans", "Jaqueta Corta-Vento"],
    ),
    (
        "Casa",
        "Móveis",
        ["Tok&Stok", "Madesa", "Ripardo"],
        ["Sofá 3 Lugares", "Mesa de Jantar", "Guarda-Roupa 6 Portas"],
    ),
    (
        "Beleza",
        "Cuidados Pessoais",
        ["Natura", "O Boticário", "Avon"],
        ["Perfume 100ml", "Kit Skincare", "Hidratante Corporal"],
    ),
    (
        "Brinquedos",
        "Infantil",
        ["Estrela", "Hasbro", "Mattel"],
        ["Boneca", "Carrinho de Controle Remoto", "Jogo de Tabuleiro"],
    ),
]

PAYMENT_METHODS = ["cartao_credito", "cartao_debito", "pix", "boleto"]
ORDER_STATUSES = ["confirmado", "pago", "enviado", "entregue", "cancelado"]
CHANNELS = ["ecommerce", "loja_fisica", "marketplace", "app"]


def get_black_friday_date(year: int) -> datetime.date:
    """Retorna a data da Black Friday no Brasil: a última sexta-feira de novembro."""
    last_day = datetime.date(year, 11, 30)
    offset = (last_day.weekday() - 4) % 7  # 4 = sexta-feira
    return last_day - datetime.timedelta(days=offset)


def _full_name(rng: random.Random) -> str:
    return f"{rng.choice(FIRST_NAMES)} {rng.choice(LAST_NAMES)}"


def _cpf_like(rng: random.Random) -> str:
    """Gera uma sequência numérica no formato de CPF (não é um CPF válido)."""
    return "".join(rng.choice("0123456789") for _ in range(11))


def _random_hex_id(rng: random.Random, length: int = 10) -> str:
    """Gera um identificador hexadecimal usando o RNG seedado (reprodutível),
    ao contrário de ``uuid.uuid4()``, que não é determinístico por semente.
    """
    return "".join(rng.choice("0123456789abcdef") for _ in range(length))


# --- Geradores por entidade --------------------------------------------


def generate_products(n: int, seed: int = 42, noise_rate: float = 0.08) -> list[Record]:
    """Gera o catálogo de produtos. ``unit_price`` é sempre uma string "crua"
    (ver ``dirty_currency``), refletindo formatos heterogêneos de origem.
    """
    rng = random.Random(seed)
    products: list[Record] = []
    for i in range(n):
        category, subcategory, brands, names = rng.choice(PRODUCT_CATALOG)
        base_price = round(rng.uniform(19.9, 4999.9), 2)
        products.append(
            {
                "product_id": f"PROD-{i + 1:06d}",
                "sku": f"SKU{rng.randint(100000, 999999)}",
                "product_name": rng.choice(names),
                "category": category,
                "subcategory": subcategory,
                "brand": rng.choice(brands),
                "unit_price": dirty_currency(base_price, rng),
                "currency": "BRL",
                "is_active": rng.random() > 0.05,
            }
        )

    products = inject_missing_fields(products, ["brand"], rate=noise_rate, rng=rng)
    for product in products:
        if chance(noise_rate, rng):
            product["category"] = noisy_enum(product["category"], rng)
    # Reprocessamento do catálogo às vezes gera SKUs duplicados
    products = duplicate_records(products, rate=noise_rate / 3, rng=rng)
    return products


def generate_stores(n: int, seed: int = 43, noise_rate: float = 0.08) -> list[Record]:
    """Gera a rede de lojas (físicas e digitais)."""
    rng = random.Random(seed)
    stores: list[Record] = []
    for i in range(n):
        uf, state_name = rng.choice(STATES)
        city = rng.choice(CITIES_BY_STATE[uf])
        stores.append(
            {
                "store_id": f"STORE-{i + 1:04d}",
                "store_name": f"Loja {city} {i + 1}",
                "store_type": rng.choice(["loja_fisica", "ecommerce", "quiosque"]),
                "city": city,
                "state": uf,
                "region": state_name,
                "opened_at": (
                    datetime.date(2015, 1, 1) + datetime.timedelta(days=rng.randint(0, 3650))
                ).isoformat(),
                "is_active": rng.random() > 0.03,
            }
        )

    for store in stores:
        if chance(noise_rate, rng):
            store["state"] = store["state"].lower()  # inconsistência de caixa na UF
        if chance(noise_rate / 2, rng):
            store["city"] = None
    return stores


def generate_customers(n: int, seed: int = 44, noise_rate: float = 0.1) -> list[Record]:
    """Gera a base de consumidores."""
    rng = random.Random(seed)
    email_domains = ["gmail.com", "hotmail.com", "yahoo.com.br", "outlook.com"]
    customers: list[Record] = []
    for i in range(n):
        name = _full_name(rng)
        email_local = name.lower().replace(" ", ".")
        customers.append(
            {
                "customer_id": f"CUST-{i + 1:07d}",
                "full_name": name,
                "cpf": _cpf_like(rng),
                "email": f"{email_local}{rng.randint(1, 999)}@{rng.choice(email_domains)}",
                "phone": f"{rng.randint(11, 99)}9{rng.randint(10000000, 99999999)}",
                "birth_date": (
                    datetime.date(1955, 1, 1) + datetime.timedelta(days=rng.randint(0, 365 * 55))
                ).isoformat(),
                "gender": rng.choice(["F", "M", "outro", None]),
                "signup_date": (
                    datetime.date(2018, 1, 1) + datetime.timedelta(days=rng.randint(0, 365 * 7))
                ).isoformat(),
                "loyalty_tier": rng.choice(["bronze", "prata", "ouro", "platina"]),
            }
        )

    for customer in customers:
        if chance(noise_rate, rng):
            customer["email"] = corrupt_email(customer["email"], rng)
        if chance(noise_rate, rng):
            customer["phone"] = corrupt_phone(customer["phone"], rng)
    customers = inject_missing_fields(customers, ["phone", "gender"], rate=noise_rate / 2, rng=rng)
    # Mesma pessoa cadastrada mais de uma vez (cadastro duplicado no app/site)
    customers = duplicate_records(customers, rate=noise_rate / 4, rng=rng)
    return customers


def generate_addresses(
    customers: list[Record], seed: int = 45, noise_rate: float = 0.1
) -> list[Record]:
    """Gera endereços de entrega para os consumidores (1 a 2 por cliente)."""
    rng = random.Random(seed)
    addresses: list[Record] = []
    for customer in customers:
        n_addresses = 2 if rng.random() < 0.15 else 1
        for j in range(n_addresses):
            uf, _ = rng.choice(STATES)
            city = rng.choice(CITIES_BY_STATE[uf])
            addresses.append(
                {
                    "address_id": f"ADDR-{_random_hex_id(rng)}",
                    "customer_id": customer["customer_id"],
                    "street": f"{rng.choice(STREET_TYPES)} {rng.choice(STREET_NAMES)}",
                    "number": str(rng.randint(1, 3000)),
                    "complement": rng.choice([None, "Apto 12", "Casa 2", "Bloco B"]),
                    "neighborhood": rng.choice(NEIGHBORHOODS),
                    "city": city,
                    "state": uf,
                    "zip_code": f"{rng.randint(1000, 99999):05d}{rng.randint(100, 999):03d}",
                    "country": "Brasil",
                    "is_primary": j == 0,
                }
            )

    for address in addresses:
        if chance(noise_rate, rng):
            address["zip_code"] = corrupt_cep(address["zip_code"], rng)
        if chance(noise_rate / 2, rng):
            address["city"] = messy_whitespace(address["city"], rng)
    # Endereços "órfãos": cliente ainda não propagado (ou já removido) no cadastro
    addresses = inject_orphan_foreign_keys(
        addresses,
        fk_field="customer_id",
        rate=noise_rate / 5,
        rng=rng,
        fake_id_factory=lambda: f"CUST-{rng.randint(9000000, 9999999)}",
    )
    return addresses


def _simulate_arrival_timestamps(
    n: int, black_friday: datetime.date, rng: random.Random
) -> list[datetime.datetime]:
    """Gera timestamps de pedidos concentrados no dia da Black Friday.

    Reproduz o padrão real de tráfego de e-commerce: pico logo à meia-noite
    (abertura das ofertas), um vale ao longo da manhã, e um segundo pico no
    fim da tarde/início da noite. Uma pequena fração dos pedidos "vaza" para
    os dois dias vizinhos, como acontece em campanhas que antecipam ou
    estendem as ofertas.
    """
    hourly_weights = [
        9,
        5,
        3,
        2,
        2,
        2,
        3,
        4,
        5,
        6,
        7,
        7,
        6,
        6,
        6,
        7,
        8,
        9,
        10,
        9,
        8,
        7,
        6,
        8,
    ]
    timestamps = []
    for _ in range(n):
        day_offset = rng.choices([-1, 0, 1], weights=[5, 90, 5])[0]
        day = black_friday + datetime.timedelta(days=day_offset)
        hour = rng.choices(range(24), weights=hourly_weights)[0]
        timestamps.append(
            datetime.datetime.combine(
                day, datetime.time(hour, rng.randint(0, 59), rng.randint(0, 59))
            )
        )
    return sorted(timestamps)


def generate_black_friday_orders(
    n_orders: int,
    customers: list[Record],
    products: list[Record],
    stores: list[Record],
    black_friday: datetime.date,
    seed: int = 46,
    noise_rate: float = 0.12,
) -> list[Record]:
    """Gera eventos de pedido (grão: item do pedido) concentrados na Black Friday.

    Cada linha representa a confirmação de um item de pedido chegando quase em
    tempo real na Bronze — o formato típico de um tópico de eventos de
    checkout — com ``source_ingested_at`` refletindo a latência de ingestão
    near-time do sistema de origem (a maioria em segundos; uma cauda longa
    chega minutos depois, simulando filas congestionadas sob pico de carga).

    Note que ``source_ingested_at`` é distinto da coluna de auditoria
    ``_ingested_at`` adicionada por ``bronze.ingest.ingest_raw_source``: a
    primeira representa quando o *sistema de origem* capturou o evento; a
    segunda, quando *este pipeline* gravou a linha na Bronze.

    O ruído aqui é propositalmente mais intenso que nas demais entidades,
    refletindo o estresse de sistemas upstream durante a Black Friday.
    """
    rng = random.Random(seed)
    customer_ids = [c["customer_id"] for c in customers]
    store_ids = [s["store_id"] for s in stores]

    # Preço de referência numérico por produto (uso interno do "sistema de
    # checkout" para calcular o total da linha). Alguns produtos só têm um
    # preço "sujo" disponível (ex.: negativo por erro de digitação no
    # catálogo) — nesse caso, um parsing defensivo cai para um preço padrão,
    # do mesmo jeito que um sistema real precisaria se proteger.
    product_catalog: list[tuple[str, float]] = []
    for product in products:
        price = parse_dirty_price(product["unit_price"])
        if price is None or price <= 0:
            price = round(rng.uniform(19.9, 999.9), 2)
        product_catalog.append((product["product_id"], price))

    timestamps = _simulate_arrival_timestamps(n_orders, black_friday, rng)

    orders: list[Record] = []
    for i, order_ts in enumerate(timestamps):
        product_id, unit_price = rng.choice(product_catalog)
        quantity = rng.choices([1, 2, 3, 4, 5], weights=[55, 25, 10, 6, 4])[0]
        discount_pct = rng.choices([0, 10, 20, 30, 40, 50], weights=[20, 25, 25, 15, 10, 5])[0]
        line_total = round(quantity * unit_price * (1 - discount_pct / 100), 2)

        # Latência de ingestão near-time: cauda longa via distribuição
        # semi-normal, mais uma pequena chance de atraso grande (retry/fila).
        ingestion_lag_seconds = int(abs(rng.gauss(mu=8, sigma=20)))
        if chance(0.02, rng):
            ingestion_lag_seconds += rng.randint(300, 3600)

        orders.append(
            {
                "order_id": f"ORD-{black_friday.isoformat()}-{i + 1:07d}",
                "customer_id": rng.choice(customer_ids),
                "store_id": rng.choice(store_ids),
                "channel": rng.choices(CHANNELS, weights=[55, 15, 20, 10])[0],
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": dirty_currency(unit_price, rng),
                "discount_pct": discount_pct,
                "line_total": line_total,
                "currency": "BRL",
                "payment_method": rng.choice(PAYMENT_METHODS),
                "status": rng.choices(ORDER_STATUSES, weights=[35, 30, 15, 15, 5])[0],
                "order_timestamp": order_ts.isoformat(),
                "source_ingested_at": (
                    order_ts + datetime.timedelta(seconds=ingestion_lag_seconds)
                ).isoformat(),
            }
        )

    # --- ruído: mais intenso aqui, simulando sistemas sob estresse de pico ---
    orders = inject_orphan_foreign_keys(
        orders,
        "customer_id",
        rate=noise_rate / 4,
        rng=rng,
        fake_id_factory=lambda: f"CUST-{rng.randint(9000000, 9999999)}",
    )
    orders = inject_orphan_foreign_keys(
        orders,
        "product_id",
        rate=noise_rate / 6,
        rng=rng,
        fake_id_factory=lambda: f"PROD-{rng.randint(900000, 999999)}",
    )
    orders = inject_missing_fields(orders, ["payment_method"], rate=noise_rate / 3, rng=rng)

    for order in orders:
        if chance(noise_rate / 5, rng):
            order["quantity"] = -order["quantity"]  # erro de leitor/scanner
        if chance(noise_rate / 10, rng):
            order["line_total"] = round(order["line_total"] * rng.uniform(50, 200), 2)  # fat-finger
        if chance(noise_rate / 2, rng):
            order["status"] = noisy_enum(order["status"], rng)

    # Reenvios/retries são mais comuns durante o pico de carga da Black Friday
    orders = duplicate_records(orders, rate=noise_rate / 2, rng=rng)
    rng.shuffle(orders)  # eventos near-time não chegam perfeitamente ordenados
    return orders


def generate_black_friday_dataset(
    n_customers: int = 5000,
    n_products: int = 500,
    n_stores: int = 30,
    n_orders: int = 50000,
    year: int | None = None,
    seed: int = 42,
) -> dict[str, list[Record]]:
    """Gera o conjunto completo de dados sintéticos Bronze para o cenário de
    acompanhamento near-time da Black Friday: produtos, lojas, consumidores,
    endereços e pedidos (grão de item de pedido).

    Args:
        year: ano de referência da Black Friday. Se omitido, usa o ano atual.
        seed: semente base — a mesma semente sempre reproduz o mesmo dataset.
    """
    year = year or datetime.date.today().year
    black_friday = get_black_friday_date(year)

    products = generate_products(n_products, seed=seed + 1)
    stores = generate_stores(n_stores, seed=seed + 2)
    customers = generate_customers(n_customers, seed=seed + 3)
    addresses = generate_addresses(customers, seed=seed + 4)
    orders = generate_black_friday_orders(
        n_orders, customers, products, stores, black_friday, seed=seed + 5
    )

    return {
        "products": products,
        "stores": stores,
        "customers": customers,
        "addresses": addresses,
        "orders": orders,
    }
