# Recon — Interview demo script

Use this as a talk track for a **10–15 minute** live demo, or compress the **60-second pitch** for “tell me about a project you built.”

---

## Before you start (2 minutes)

**Environment checklist**

- [ ] Python 3.10+ with deps: `pip install -r requirements.txt`
- [ ] `dbt` on PATH (`dbt --version` → 1.8.x)
- [ ] Terminal open at repo root: `recon/`
- [ ] Browser tab ready (do **not** open `file://` dashboard yet)
- [ ] Optional: second terminal for `http.server` after pipeline finishes

**Mental frame for the interviewer**

> “This is a prototype of what a solutions engineer or data team might run on day zero of sales tax onboarding—not production filing software, but a diagnostic that finds bad data before it becomes a compliance incident.”

---

## 60-second elevator pitch

> “I built **Recon**, a sales tax **onboarding diagnostic**. When merchants connect Stripe and Shopify, the first filing often breaks because of data quality—missing billing ZIPs, orphan refunds, B2B orders without exemption certificates, or zero tax collected in states where tax applies.
>
> Recon runs a **real dbt project** on **DuckDB**: mock API-shaped JSON in, staging and marts out, plus **singular dbt tests** that fail when those defects exist. A single **`python pipeline.py`** command regenerates everything and writes **`results.json`**. A static **`dashboard.html`** reads that file—no Streamlit, no React build step—so it feels like an internal fintech ops tool.
>
> I intentionally seeded five merchants with five different dirty profiles so you can show triage by merchant and nexus exposure by state, including **New York AND logic** and **California/Texas sales-only** thresholds.”

---

## Full demo flow (~12 minutes)

### 1. Problem & architecture (2 min)

**Say:**

> “Numeral-style onboarding isn’t just ‘turn on tax’—you need trustworthy transaction history per state. I modeled two sources: Stripe charges/refunds and Shopify orders. The pipeline is three layers: **generate → load → transform → present**.”

**Show** (optional): `README.md` architecture diagram, or sketch on screen:

```
Stripe / Shopify (JSON) → Python load → DuckDB → dbt → results.json → dashboard
```

**Point at repo:**

| Path | One-liner |
|------|-----------|
| `generate/constants.py` | Five merchants + dirty profiles |
| `generate/data_generator.py` | ~2k rows + injected defects |
| `generate/db_loader.py` | Flatten nested billing/metadata → raw tables |
| `recon_dbt/` | Staging → intermediate → marts + tests |
| `pipeline.py` | One command orchestration |
| `dashboard.html` | Ops-facing UI |

---

### 2. Run the pipeline live (3–4 min)

**Say:**

> “One command is the whole story for a solutions engineer: regenerate data, load DuckDB, run dbt, export JSON for the dashboard.”

**Run:**

```bash
pip install -r requirements.txt   # skip if already installed
python pipeline.py
```

**Narrate each step as it prints:**

1. **Generate** — “Faker builds realistic charges and orders; then `inject_dirty_cases` applies each merchant’s defect.”
2. **Load** — “This is where Fivetran would land Parquet in S3; here we flatten JSON into DuckDB.”
3. **dbt deps / seed / run** — “Seed is 50 states + DC nexus rules; models build the unified ledger and marts.”
4. **dbt test** — “Tests are *supposed* to fail—we seeded bad data on purpose.”
5. **Export** — “`data/results.json` is the contract between pipeline and UI.”

**If dbt tests fail:** don’t panic.

> “That’s expected. Singular tests return rows when violations exist; the pipeline still exports pass/fail into JSON for the dashboard.”

**Optional deep dive** (if they ask “show me the data”):

```bash
# Peek at generated JSON
ls data/generated/

# Or query DuckDB after pipeline
python -c "import duckdb; c=duckdb.connect('data/recon.duckdb'); print(c.execute('select merchant_id, issue_type, issue_count from fct_data_issues order by 1,2').df())"
```

---

### 3. Open the dashboard (4–5 min)

**Say:**

> “The UI is deliberately boring in a good way—static HTML, Inter font, Stripe-like KPI cards. It only needs JSON from the pipeline.”

**Run** (new terminal, repo root):

```bash
python -m http.server 8000
```

Open: **http://localhost:8000/dashboard.html**

> “We use a local server because browsers block `fetch` on `file://`—that’s a real gotcha we documented in the README.”

#### Sidebar

- **Merchant dropdown** — “Every view filters by merchant; same JSON file, no backend.”
- **Pipeline status** — “`generated_at` from the last `pipeline.py` run.”
- **Legend** — “Five seeded dirty profiles map to five demo merchants (Acme, Ridge, Nova, Surf, Graze).”

#### KPI row

| Card | What to say |
|------|-------------|
| Transactions (T12M, US) | “Summed from nexus mart—proxy for volume in the trailing window.” |
| Data issues | “Sum of issue counts; card turns red if &gt; 0.” |
| States with nexus | “Count of states where `has_crossed_nexus_threshold` is true.” |
| Data quality score | “`1 − issues / transactions` as a simple onboarding health %.” |

#### Switch merchants (recommended order)

1. **Acme Foods** — missing billing ZIP/state → **Onboarding Issues** table, high severity.
2. **Ridge Gear** — duplicate cross-store (same Shopify order, two stores); mention dbt test `assert_no_duplicate_txn_ids`.
3. **Nova Health** — B2B without exemption certs.
4. **Surf Co** — EUR merchant, USD charges → currency mismatch.
5. **Graze Box** — orphan refunds (`ch_FAKE_*`) → orphan refund issue + failed refund-parent test.

**Say for nexus table:**

> “Only states that **crossed** economic nexus show up. California and Texas are **sales-only** at $500k; New York needs **both** $500k sales **and** 100 transactions—implemented in the mart, not hand-waved.”

Expand **dbt Pipeline Test Results**:

> “Six singular tests—each returns rows only when the rule is violated. Red failures here prove the pipeline detected what we seeded.”

---

### 4. dbt architecture (2–3 min, if technical audience)

Open one file per layer (keep it fast):

**Staging** — `recon_dbt/models/staging/stg_stripe_charges.sql`

> “Single source, single row: flags like `is_missing_zip`, `is_zero_tax_taxable_state`. No joins.”

**Intermediate** — `recon_dbt/models/intermediate/int_unified_transactions.sql`

> “Union Stripe + Shopify, dedupe on charge ID, **Stripe wins** when both exist.”

**Mart** — `recon_dbt/models/marts/fct_nexus_exposure.sql`

> “Business-facing aggregates only—trailing 12 months, join to seed, exposure tier.”

**Test** — `recon_dbt/tests/assert_no_zero_tax_in_taxable_state.sql`

> “This is data quality as code—same checks you’d run before first filing.”

---

### 5. Production story (1 min)

**Say:**

> “DuckDB becomes Snowflake or BigQuery. The generator becomes Fivetran or custom connectors to S3. `pipeline.py` becomes Airflow or dbt Cloud. The dashboard becomes Looker, an internal React app, or an API—but the **layering stays the same**.”

---

## Closing line

> “Recon answers: *Can we trust this merchant’s data enough to file?* and *Where are they exposed on economic nexus?*—before money and penalties are on the line.”

---

## Anticipated questions & short answers

| Question | Answer |
|----------|--------|
| Why DuckDB? | Zero cloud credentials; full SQL + dbt; fast local prototype. |
| Why not Streamlit/React? | Requirement was a single HTML file ops can open anywhere; JSON is the API boundary. |
| Are nexus rules legally exact? | Prototype thresholds from public guidance; production needs per-state maintenance and legal review. |
| Why do dbt tests fail in the demo? | Dirty data is intentional; failures prove detection works. |
| How do you handle NY vs CA nexus? | Seed columns + explicit `CASE` in `fct_nexus_exposure` (AND vs sales-only vs OR). |
| Duplicate Ridge Gear orders? | Cloned charges share `shopify_order_id` across stores; caught by duplicate txn test. |
| What would you add next? | Real connectors, `transaction_count` in results JSON, DC in generator weights, auth on dashboard API. |
| Biggest bug you hit building this? | `file://` fetch blocked; invalid `motion.div` typo in HTML; expanded state list after first pass missed MA/CT/etc. |

---

## Cheat sheet — commands only

```bash
cd recon
pip install -r requirements.txt
python pipeline.py
python -m http.server 8000
# → http://localhost:8000/dashboard.html
```

```bash
cd recon_dbt
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

---

## 5-minute version (panel or timeboxed)

1. **Pitch** (60s) — problem + stack + one command.
2. **Run** `python pipeline.py` (90s) — narrate steps, embrace test failures.
3. **Dashboard** (2 min) — Acme + Graze + expand dbt tests.
4. **Architecture** (60s) — staging / intermediate / marts in one sentence each.
5. **Production** (30s) — Fivetran + warehouse + scheduler.

---

## 30-second version (recruiter screen)

> “I built a sales tax onboarding diagnostic: Python generates dirty Stripe/Shopify data, dbt on DuckDB finds issues and nexus risk, one script exports JSON, and a static dashboard lets ops triage by merchant. It mirrors how I’d structure real pipelines—raw, staging, marts, and tests as code.”
