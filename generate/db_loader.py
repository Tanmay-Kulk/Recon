"""
In a production Numeral-style pipeline, this ingestion step would be handled by Fivetran connectors
or custom API integrations writing Parquet files to S3. The dbt pipeline would then read from an
external stage or cloud warehouse like Snowflake or BigQuery. Here we write directly to DuckDB to keep
the prototype self-contained and runnable without any cloud credentials.
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATA_DIR = PROJECT_ROOT / "data"
DUCKDB_PATH = DATA_DIR / "recon.duckdb"
GENERATED_DIR = DATA_DIR / "generated"


def _read_json_array(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Expected JSON at {path}. Run data generation first.")
    return pd.read_json(path, orient="records")


def _flatten_stripe_charges(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    bd = pd.json_normalize(df["billing_details"])
    bd = bd.rename(
        columns={
            "address.state": "billing_state",
            "address.postal_code": "billing_zip",
            "address.country": "billing_country",
        }
    )

    meta = pd.json_normalize(df["metadata"])

    base = df.drop(columns=["billing_details", "metadata"]).reset_index(drop=True)

    bd_cols = [c for c in ["billing_state", "billing_zip", "billing_country"] if c in bd.columns]
    meta_cols = [c for c in ["is_b2b", "exemption_cert_id", "shopify_order_id"] if c in meta.columns]

    out = pd.concat(
        [base, bd[bd_cols].reset_index(drop=True), meta[meta_cols].reset_index(drop=True)],
        axis=1,
    )
    return out


def _flatten_shopify_orders(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df

    addr = pd.json_normalize(df["billing_address"])
    addr = addr.rename(
        columns={
            "province_code": "billing_state",
            "zip": "billing_zip",
            "country_code": "billing_country",
        }
    )

    base = df.drop(columns=["billing_address"]).reset_index(drop=True)
    base = base.drop(columns=["note_attributes"], errors="ignore")
    addr_cols = [c for c in ["billing_state", "billing_zip", "billing_country"] if c in addr.columns]
    return pd.concat([base, addr[addr_cols].reset_index(drop=True)], axis=1)


def _ensure_refund_columns(df: pd.DataFrame) -> pd.DataFrame:
    expected = ["id", "object", "merchant_id", "amount", "currency", "status", "created", "charge_id"]
    if df.empty:
        return pd.DataFrame(columns=expected)
    for col in expected:
        if col not in df.columns:
            df[col] = pd.NA
    return df[expected]


def load_raw_tables() -> None:
    """Read generated JSON, flatten nested fields, and replace raw tables in DuckDB."""
    charges_path = GENERATED_DIR / "stripe_charges.json"
    refunds_path = GENERATED_DIR / "stripe_refunds.json"
    orders_path = GENERATED_DIR / "shopify_orders.json"

    df_charges = _flatten_stripe_charges(_read_json_array(charges_path))
    df_orders = _flatten_shopify_orders(_read_json_array(orders_path))
    df_refunds = _ensure_refund_columns(_read_json_array(refunds_path))

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DUCKDB_PATH))

    con.register("_raw_stripe_charges", df_charges)
    con.execute("CREATE OR REPLACE TABLE raw_stripe_charges AS SELECT * FROM _raw_stripe_charges")
    con.unregister("_raw_stripe_charges")

    con.register("_raw_stripe_refunds", df_refunds)
    con.execute("CREATE OR REPLACE TABLE raw_stripe_refunds AS SELECT * FROM _raw_stripe_refunds")
    con.unregister("_raw_stripe_refunds")

    con.register("_raw_shopify_orders", df_orders)
    con.execute("CREATE OR REPLACE TABLE raw_shopify_orders AS SELECT * FROM _raw_shopify_orders")
    con.unregister("_raw_shopify_orders")

    con.close()


if __name__ == "__main__":
    load_raw_tables()
    print(f"Loaded raw tables into {DUCKDB_PATH}")
