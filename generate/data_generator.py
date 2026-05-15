"""
Generate mock Stripe charges, Stripe refunds, and Shopify orders for the Recon prototype.

Approximately 2,000 charge/order pairs are created across five merchants, then
``inject_dirty_cases`` applies merchant-specific and global data-quality defects.
Output JSON is written under ``data/generated/``.
"""

from __future__ import annotations

import json
import random
import string
from copy import deepcopy
from datetime import datetime, timedelta, timezone
from pathlib import Path

from faker import Faker

from generate.constants import MERCHANTS, NO_TAX_STATES, TAXABLE_STATES

fake = Faker()
Faker.seed(42)
random.seed(42)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
GENERATED_DIR = PROJECT_ROOT / "data" / "generated"

_CHARGES_PER_MERCHANT = 400
_FAILED_CHARGE_RATE = 0.02
_PARTIALLY_REFUNDED_ORDER_RATE = 0.05


def _random_charge_id() -> str:
    suffix = "".join(random.choices(string.ascii_lowercase + string.digits, k=24))
    return f"ch_{suffix}"


def _pick_billing_state(merchant_currency: str) -> tuple[str | None, str, str]:
    """Return (state_or_none_for_intl, postal_code, country_code)."""
    if merchant_currency == "eur":
        # EU-style billing for EUR merchants (province/state optional in source data)
        country = random.choice(["FR", "DE", "ES", "NL"])
        region = fake.bothify(text="??").upper()
        return (region, fake.postcode(), country)
    if random.random() < 0.82:
        st = random.choice(TAXABLE_STATES)
        return st, fake.zipcode(), "US"
    st = random.choice(NO_TAX_STATES)
    return st, fake.zipcode(), "US"


def _amount_cents() -> int:
    return int(random.randint(499, 499_99))  # $4.99–$499.99


def _tax_cents(amount_cents: int, state: str | None, country: str, status: str) -> int:
    if status != "succeeded":
        return 0
    if country != "US" or not state:
        return 0
    if state in NO_TAX_STATES:
        return 0
    if state in TAXABLE_STATES:
        rate = random.uniform(0.0475, 0.103)
        return int(round(amount_cents * rate))
    return 0


def _iso_from_unix(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S%z")


def _fmt_money(cents: int) -> str:
    return f"{cents / 100:.2f}"


def _bool_note_attr(value: bool) -> str:
    return "true" if value else "false"


def generate_clean_records() -> tuple[list[dict], list[dict]]:
    """Build ~2,000 succeeded/failed Stripe charges and matching Shopify orders (clean)."""
    charges: list[dict] = []
    orders: list[dict] = []
    order_id_seq = 500_000

    for merchant in MERCHANTS:
        mid = merchant["id"]
        default_currency = merchant["currency"]
        stores = merchant["shopify_stores"]
        b2b_ratio = merchant["b2b_ratio"]

        for _ in range(_CHARGES_PER_MERCHANT):
            order_id_seq += 1
            shopify_order_id = str(order_id_seq)
            source_store = (
                random.choices(
                    stores,
                    weights=[0.72, 0.28] if len(stores) == 2 else None,
                    k=1,
                )[0]
                if len(stores) == 2
                else stores[0]
            )

            is_b2b = random.random() < b2b_ratio
            exemption_cert_id = f"CERT-{fake.lexify('????').upper()}-{fake.random_int(1000, 9999)}" if is_b2b else None

            amount = _amount_cents()
            status = "failed" if random.random() < _FAILED_CHARGE_RATE else "succeeded"

            state, postal, country = _pick_billing_state(default_currency)
            tax_amount = _tax_cents(amount, state, country, status)

            created_ts = int(
                (datetime.now(timezone.utc) - timedelta(days=random.randint(0, 400), seconds=random.randint(0, 86400))).timestamp()
            )

            ch_id = _random_charge_id()
            charge_currency = default_currency

            billing_state = state
            billing_postal = postal
            billing_country = country

            charge = {
                "id": ch_id,
                "object": "charge",
                "merchant_id": mid,
                "amount": amount,
                "currency": charge_currency,
                "status": status,
                "created": created_ts,
                "billing_details": {
                    "email": fake.email(),
                    "name": fake.name(),
                    "address": {
                        "city": fake.city(),
                        "country": billing_country,
                        "line1": fake.street_address(),
                        "postal_code": billing_postal,
                        "state": billing_state,
                    },
                },
                "metadata": {
                    "shopify_order_id": shopify_order_id,
                    "is_b2b": is_b2b,
                    "exemption_cert_id": exemption_cert_id,
                },
                "tax_amount_collected": tax_amount,
                "source_store": source_store,
            }

            fin = "paid" if status == "succeeded" else "pending"
            if status == "succeeded" and random.random() < _PARTIALLY_REFUNDED_ORDER_RATE:
                fin = "partially_refunded"

            subtotal_cents = max(amount - tax_amount, 0)
            order = {
                "id": order_id_seq,
                "name": f"#{order_id_seq % 100000}",
                "merchant_id": mid,
                "source_store": source_store,
                "created_at": _iso_from_unix(created_ts),
                "currency": default_currency,
                "total_price": _fmt_money(amount),
                "subtotal_price": _fmt_money(subtotal_cents),
                "total_tax": _fmt_money(tax_amount),
                "taxes_included": False,
                "financial_status": fin,
                "billing_address": {
                    "address1": fake.street_address(),
                    "city": fake.city(),
                    "province_code": billing_state,
                    "country_code": billing_country,
                    "zip": billing_postal,
                },
                "stripe_charge_id": ch_id,
                "note_attributes": [
                    {"name": "is_b2b", "value": _bool_note_attr(is_b2b)},
                    {
                        "name": "exemption_cert_id",
                        "value": exemption_cert_id or "",
                    },
                ],
            }

            charges.append(charge)
            orders.append(order)

    return charges, orders


def inject_dirty_cases(charges: list[dict], orders: list[dict], refunds: list[dict]) -> None:
    """
    Apply merchant-specific dirty profiles and global anomalies.

    Mutates ``charges`` and ``orders`` in place and appends to ``refunds``.
    """
    taxable_set = set(TAXABLE_STATES)

    # --- Acme Foods: missing billing zip/state on 15% of charges + matching orders ---
    acme_id = "merch_acme_foods"
    acme_charges = [c for c in charges if c["merchant_id"] == acme_id]
    k_acme = max(1, int(round(len(acme_charges) * 0.15)))
    for ch in random.sample(acme_charges, k=min(k_acme, len(acme_charges))):
        ch["billing_details"]["address"]["postal_code"] = None
        ch["billing_details"]["address"]["state"] = None
        oid = ch["metadata"]["shopify_order_id"]
        for o in orders:
            if o["merchant_id"] == acme_id and str(o["id"]) == oid:
                o["billing_address"]["zip"] = ""
                o["billing_address"]["province_code"] = None
                break

    # --- Ridge Gear: clone 8% of ridge-gear-us charges to ridge-gear-ca ---
    ridge_id = "merch_ridge_gear"
    us_charges = [c for c in charges if c["merchant_id"] == ridge_id and c["source_store"] == "ridge-gear-us"]
    k_ridge = max(1, int(round(len(us_charges) * 0.08)))
    for ch in random.sample(us_charges, k=min(k_ridge, len(us_charges))):
        clone = deepcopy(ch)
        clone["id"] = _random_charge_id()
        clone["source_store"] = "ridge-gear-ca"
        # same shopify_order_id in metadata, same amount/currency/etc.
        charges.append(clone)

    # --- Nova Health: 60% of B2B charges lose exemption cert ---
    nova_id = "merch_nova_health"
    nova_b2b = [c for c in charges if c["merchant_id"] == nova_id and c["metadata"].get("is_b2b") is True]
    k_nova = max(1, int(round(len(nova_b2b) * 0.60)))
    for ch in random.sample(nova_b2b, k=min(k_nova, len(nova_b2b))):
        ch["metadata"]["exemption_cert_id"] = None
        oid = ch["metadata"]["shopify_order_id"]
        for o in orders:
            if o["merchant_id"] == nova_id and str(o["id"]) == oid:
                new_attrs = []
                for na in o["note_attributes"]:
                    if na["name"] == "exemption_cert_id":
                        new_attrs.append({"name": "exemption_cert_id", "value": ""})
                    else:
                        new_attrs.append(na)
                o["note_attributes"] = new_attrs
                break

    # --- Surf Co: 30% of charges forced to USD (merchant default EUR) ---
    surf_id = "merch_surf_co"
    surf_charges = [c for c in charges if c["merchant_id"] == surf_id]
    k_surf = max(1, int(round(len(surf_charges) * 0.30)))
    for ch in random.sample(surf_charges, k=min(k_surf, len(surf_charges))):
        ch["currency"] = "usd"

    # --- Graze Box: 40 orphan refunds pointing at non-existent charges ---
    graze_id = "merch_graze_box"
    now_ts = int(datetime.now(timezone.utc).timestamp())
    for i in range(1, 41):
        refunds.append(
            {
                "id": f"re_orphan_{i:03d}",
                "object": "refund",
                "merchant_id": graze_id,
                "amount": random.randint(100, 15_000),
                "currency": "usd",
                "status": "succeeded",
                "created": now_ts - random.randint(0, 7 * 86400),
                "charge_id": f"ch_FAKE_{i:03d}",
            }
        )

    # --- Global: 3% succeeded charges in taxable states with zero tax collected ---
    candidates = [
        c
        for c in charges
        if c["status"] == "succeeded"
        and (c["billing_details"]["address"].get("state") or "") in taxable_set
    ]
    k_g = max(1, int(round(len(candidates) * 0.03)))
    for ch in random.sample(candidates, k=min(k_g, len(candidates))):
        ch["tax_amount_collected"] = 0
        oid = ch["metadata"]["shopify_order_id"]
        mid = ch["merchant_id"]
        for o in orders:
            if o["merchant_id"] == mid and str(o["id"]) == oid:
                o["total_tax"] = "0.00"
                break


def write_generated_json(charges: list[dict], orders: list[dict], refunds: list[dict]) -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)
    (GENERATED_DIR / "stripe_charges.json").write_text(json.dumps(charges, indent=2), encoding="utf-8")
    (GENERATED_DIR / "stripe_refunds.json").write_text(json.dumps(refunds, indent=2), encoding="utf-8")
    (GENERATED_DIR / "shopify_orders.json").write_text(json.dumps(orders, indent=2), encoding="utf-8")


def run_generation() -> None:
    """Generate mock JSON datasets under ``data/generated/``."""
    charges, orders = generate_clean_records()
    refunds: list[dict] = []
    inject_dirty_cases(charges, orders, refunds)
    write_generated_json(charges, orders, refunds)
    print(
        f"Wrote {len(charges)} charges, {len(orders)} orders, {len(refunds)} refunds to {GENERATED_DIR}"
    )


if __name__ == "__main__":
    run_generation()
