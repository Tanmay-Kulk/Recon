# Recon

Sales tax onboarding often fails when transaction data from **Stripe** and **Shopify** is incomplete or inconsistent before the first filing. Missing billing addresses, orphan refunds, B2B orders without exemption certificates, and incorrect tax in taxable states all create compliance risk. **Recon** is a prototype diagnostic tool that runs a realistic data pipeline (mock ingestion → dbt on DuckDB → JSON export) and surfaces merchant health, data quality issues, and economic nexus exposure in a single browser dashboard—no web server, Streamlit, or frontend framework required for the UI itself.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  Data sources (prototype: generated JSON mimicking APIs)        │
│  Stripe charges/refunds  ·  Shopify orders                      │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Pipeline (Python + dbt on DuckDB)                              │
│  generate/ → data/generated/*.json → db_loader → recon.duckdb   │
│  dbt: staging (views) → intermediate (tables) → marts (tables)  │
└───────────────────────────────┬─────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────┐
│  Dashboard                                                      │
│  dashboard.html  ←  fetch(data/results.json)                    │
└─────────────────────────────────────────────────────────────────┘
```

| Layer | What it does |
|-------|----------------|
| **Generate** | `generate/data_generator.py` builds ~2,000 charge/order pairs across five merchants, then injects known dirty cases. |
| **Load** | `generate/db_loader.py` flattens nested JSON fields into `raw_*` DuckDB tables. |
| **Transform** | `recon_dbt/` runs dbt models, seeds nexus thresholds, and singular tests that *fail on purpose* when dirty data is present. |
| **Present** | `pipeline.py` exports `data/results.json`; `dashboard.html` renders the diagnostic UI. |

## Quickstart

```bash
pip install -r requirements.txt
python pipeline.py
```

Then open the dashboard. Because the UI loads JSON via `fetch`, use a local static server (recommended):

```bash
python -m http.server 8000
```

Open [http://localhost:8000/dashboard.html](http://localhost:8000/dashboard.html).

> Opening `dashboard.html` directly as `file://` often blocks `fetch` in modern browsers; the server step avoids that.

**Interview walkthrough:** see [DEMO_SCRIPT.md](DEMO_SCRIPT.md) for a timed talk track, live commands, merchant switch order, and Q&A.

## Project layout

```
recon/
├── generate/
│   ├── constants.py          # Merchants, taxable / no-tax states
│   ├── data_generator.py     # Mock Stripe + Shopify JSON
│   └── db_loader.py          # JSON → DuckDB raw tables
├── data/
│   ├── generated/            # stripe_charges.json, shopify_orders.json, …
│   ├── recon.duckdb          # Created by pipeline
│   └── results.json          # Dashboard input (created by pipeline)
├── recon_dbt/
│   ├── models/staging|intermediate|marts/
│   ├── seeds/state_nexus_thresholds.csv
│   └── tests/assert_*.sql
├── pipeline.py
├── dashboard.html
└── requirements.txt
```

## dbt model architecture

**Staging** (`models/staging/`) sits directly on raw sources. It renames and casts columns, derives row-level flags (missing ZIP, zero tax in taxable state, orphan refund, etc.), and does not join across systems.

**Intermediate** (`models/intermediate/`) reconciles Stripe and Shopify into one canonical transaction stream (`int_unified_transactions`), deduplicating on Stripe charge ID with Stripe as the source of truth.

**Marts** (`models/marts/`) produce dashboard-ready aggregates: `fct_data_issues` (onboarding issue counts by merchant) and `fct_nexus_exposure` (trailing 12-month sales/transactions vs. state thresholds, with CA/TX sales-only, NY **AND** logic, and default **OR** logic elsewhere).

Run dbt manually from `recon_dbt/` if needed:

```bash
cd recon_dbt
dbt deps --profiles-dir .
dbt seed --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## Five demo merchants & dirty profiles

| Merchant | Dirty profile |
|----------|----------------|
| Acme Foods Co | Missing billing ZIP/state |
| Ridge Gear Inc | Duplicate cross-store (same Shopify order, two stores) |
| Nova Health Supplies | B2B missing exemption certificates |
| Surf Co Collective | Charge currency USD vs. merchant default EUR |
| Graze Box Snacks | Orphan refunds (`ch_FAKE_*`) |

dbt singular tests in `recon_dbt/tests/` are **expected to fail** after `python pipeline.py` because the generator intentionally seeds these defects.

## How this maps to production at scale

| Prototype | Production at scale |
|-----------|---------------------|
| DuckDB file (`data/recon.duckdb`) | Snowflake, BigQuery, or Redshift |
| Python generator + `db_loader.py` | Fivetran (or custom API connectors) writing Parquet to S3 |
| `python pipeline.py` | Scheduled **dbt Cloud**, Airflow, or Dagster job |
| `dashboard.html` + static JSON | BI tool, internal React app, or API-backed dashboard |

The layering (raw → staging → intermediate → marts) stays the same; only the execution environment and ingestion path change.

## Development notes: issues encountered & fixes

Brief log of problems hit while building the prototype and how they were resolved:

1. **Empty Git-tracked folders** — Git does not track empty directories. **Fix:** added `.gitkeep` files under `generate/`, `data/generated/`, `data/notices/`, and `recon_dbt/models/*` until real files existed.

2. **Incomplete US state coverage in `TAXABLE_STATES`** — The first constants list omitted many states with sales tax (e.g. MA, CT, MD). **Fix:** expanded `TAXABLE_STATES` to 45 codes plus `NO_TAX_STATES` for OR, MT, NH, DE, AK; aligned staging/intermediate SQL `IN (...)` lists and the nexus seed (50 states + DC).

3. **Shopify `note_attributes` left nested in DuckDB** — Staging could not read B2B flags from a JSON blob. **Fix:** in `db_loader.py`, drop `note_attributes` after flattening billing address (`base.drop(columns=["note_attributes"], errors="ignore")`); B2B logic remains on Stripe charges via `metadata`.

4. **dbt profiles location** — `dbt` looks in `~/.dbt/profiles.yml` by default. **Fix:** `pipeline.py` and docs use `--profiles-dir .` from `recon_dbt/` so `recon_dbt/profiles.yml` points at `../data/recon.duckdb`.

5. **Nexus threshold accuracy (CA, TX, NY)** — California and Texas use **$500k sales only** (no transaction count); New York uses **$500k AND 100 transactions**. **Fix:** encoded explicitly in `seeds/state_nexus_thresholds.csv` and in `fct_nexus_exposure.sql` `CASE` logic (not generalized to one rule for all states).

6. **Dashboard `fetch` on `file://`** — Browsers block loading `data/results.json` when the HTML is opened from disk. **Fix:** README and `dashboard.html` error text recommend `python -m http.server` and opening `http://localhost:8000/dashboard.html`.

7. **Invalid HTML tags in `dashboard.html`** — A typo introduced `<motion.div>` elements (invalid). **Fix:** replaced with standard `<div>` tags so the layout renders correctly.

8. **Expected dbt test failures** — Singular tests return rows when dirty data exists; dbt reports failure. **Fix:** by design for the demo; `pipeline.py` still exports pass/fail into `results.json` and the dashboard shows them in the collapsible test panel.

## Dependencies

- `duckdb`
- `dbt-core==1.8.3`
- `dbt-duckdb==1.8.1`
- `pandas==2.2.2`
- `faker==25.2.0`

## License

Prototype for demonstration; not tax or legal advice. Verify nexus thresholds and rules against current state guidance before production use.
