import csv
import random
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import SupportsFloat, cast


def generate_ecommerce_dataset(raw_dir: Path, *, days: int = 10, seed: int = 42) -> None:
    random.seed(seed)
    batch_dir = raw_dir / f"batch_date={datetime.now(UTC).date().isoformat()}"
    batch_dir.mkdir(parents=True, exist_ok=True)

    start_ts = datetime.now(UTC).replace(hour=8, minute=0, second=0, microsecond=0) - timedelta(days=days)
    customers = _customers(start_ts)
    products = _products(start_ts)
    orders = _orders(start_ts, customers, products, days)
    transactions = _transactions(orders)
    events = _events(start_ts, customers, products, days)

    _write_csv(batch_dir / "customers.csv", customers)
    _write_csv(batch_dir / "products.csv", products)
    _write_csv(batch_dir / "orders.csv", orders)
    _write_csv(batch_dir / "transactions.csv", transactions)
    _write_csv(batch_dir / "events.csv", events)


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).replace(tzinfo=None).isoformat(timespec="seconds")


def _customers(start_ts: datetime) -> list[dict[str, object]]:
    cities = [
        ("Sao Paulo", "SP"),
        ("Rio de Janeiro", "RJ"),
        ("Curitiba", "PR"),
        ("Belo Horizonte", "MG"),
    ]
    rows: list[dict[str, object]] = []
    for index in range(1, 41):
        city, state = cities[index % len(cities)]
        created = start_ts - timedelta(days=random.randint(10, 90))
        rows.append(
            {
                "customer_id": f"C{index:04d}",
                "email": f"customer{index}@example.com",
                "full_name": f"Customer {index}",
                "city": city,
                "state": state,
                "created_at": _iso(created),
                "updated_at": _iso(start_ts + timedelta(hours=index % 24)),
            }
        )
    rows.append({**rows[0], "updated_at": _iso(start_ts + timedelta(days=1))})
    return rows


def _products(start_ts: datetime) -> list[dict[str, object]]:
    catalog = [
        ("P0001", "SKU-COFFEE", "Specialty Coffee", "grocery", 42.90),
        ("P0002", "SKU-HEADPHONES", "Wireless Headphones", "electronics", 299.90),
        ("P0003", "SKU-SNEAKERS", "Running Sneakers", "fashion", 389.00),
        ("P0004", "SKU-BACKPACK", "Commuter Backpack", "fashion", 189.50),
        ("P0005", "SKU-BOOK", "Data Engineering Book", "books", 119.90),
        ("P0006", "SKU-MONITOR", "27in Monitor", "electronics", 1399.00),
    ]
    return [
        {
            "product_id": product_id,
            "sku": sku,
            "product_name": name,
            "category": category,
            "unit_price": price,
            "is_active": True,
            "updated_at": _iso(start_ts + timedelta(hours=index)),
        }
        for index, (product_id, sku, name, category, price) in enumerate(catalog)
    ]


def _orders(
    start_ts: datetime,
    customers: list[dict[str, object]],
    products: list[dict[str, object]],
    days: int,
) -> list[dict[str, object]]:
    rows = []
    statuses = ["created", "paid", "shipped", "delivered", "cancelled"]
    for index in range(1, 181):
        product = random.choice(products)
        ordered_at = start_ts + timedelta(hours=random.randint(0, days * 24))
        rows.append(
            {
                "order_id": f"O{index:06d}",
                "customer_id": random.choice(customers)["customer_id"],
                "product_id": product["product_id"],
                "order_ts": _iso(ordered_at),
                "quantity": random.randint(1, 4),
                "unit_price": product["unit_price"],
                "discount_amount": round(random.choice([0, 0, 0, 5, 10, 25]), 2),
                "order_status": random.choice(statuses),
                "updated_at": _iso(ordered_at + timedelta(minutes=random.randint(1, 360))),
            }
        )
    rows.append({**rows[5], "updated_at": _iso(start_ts + timedelta(days=days, hours=2))})
    rows.append({**rows[8], "order_id": "O_BAD_NEGATIVE", "quantity": -2})
    rows.append({**rows[9], "order_id": "O_BAD_NULL_CUSTOMER", "customer_id": ""})
    return rows


def _transactions(orders: list[dict[str, object]]) -> list[dict[str, object]]:
    rows = []
    methods = ["credit_card", "pix", "bank_slip", "wallet"]
    for index, order in enumerate(orders[:160], start=1):
        amount = float(cast(SupportsFloat, order["quantity"])) * float(
            cast(SupportsFloat, order["unit_price"])
        ) - float(cast(SupportsFloat, order["discount_amount"]))
        transaction_ts = datetime.fromisoformat(str(order["order_ts"])) + timedelta(minutes=random.randint(1, 30))
        rows.append(
            {
                "transaction_id": f"T{index:06d}",
                "order_id": order["order_id"],
                "transaction_ts": _iso(transaction_ts.replace(tzinfo=UTC)),
                "payment_method": random.choice(methods),
                "payment_status": random.choice(["authorized", "captured", "failed", "refunded"]),
                "amount": round(amount, 2),
                "currency": "BRL",
                "updated_at": _iso(transaction_ts.replace(tzinfo=UTC) + timedelta(minutes=2)),
            }
        )
    rows.append({**rows[0], "transaction_id": "T_BAD_AMOUNT", "amount": -10.00})
    return rows


def _events(
    start_ts: datetime,
    customers: list[dict[str, object]],
    products: list[dict[str, object]],
    days: int,
) -> list[dict[str, object]]:
    rows = []
    event_types = ["page_view", "product_view", "add_to_cart", "checkout_started", "purchase"]
    for index in range(1, 501):
        event_ts = start_ts + timedelta(minutes=random.randint(0, days * 24 * 60))
        product_id = random.choice(products)["product_id"] if random.random() > 0.25 else ""
        customer_id = random.choice(customers)["customer_id"] if random.random() > 0.08 else ""
        rows.append(
            {
                "event_id": f"E{index:06d}",
                "customer_id": customer_id,
                "session_id": f"S{random.randint(1, 130):05d}",
                "event_ts": _iso(event_ts),
                "event_type": random.choice(event_types),
                "product_id": product_id,
                "device_type": random.choice(["mobile", "desktop", "tablet"]),
                "updated_at": _iso(event_ts + timedelta(minutes=1)),
            }
        )
    rows.append({**rows[3], "event_ts": "not-a-timestamp", "event_id": "E_BAD_TS"})
    return rows


def _write_csv(path: Path, rows: list[dict[str, object]]) -> None:
    if not rows:
        return
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
