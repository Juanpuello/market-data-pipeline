# Financial Data Engineer Pipeline — Requirements

## Goal
Build a production‑grade pipeline in Python that extracts option‑volatility financial data from the mock MarketData API, stores raw responses in a blob, stores these values into a raw_data table, transforms and validates values, and writes cleaned rows into a clean_data table. Stack and tools:
- Programming language: Python
- Orchestration: Prefect
- Schema & validation: Pydantic (enforce data models in extraction and inserts)
- Data transformation: pandas
- Cloud provider: Microsoft Azure (blob storage for raw responses, managed DBs)
- Package manager: uv
- Infrastructure as a Code: terraform

## Inputs
- MarketData.get_historical_data(expression: str, start_date: date, end_date: date) -> pd.DataFrame
- Run modes: daily incremental, optional old‑codes inclusion, historical backfill for old codes only (manual trigger)
- Date ranges are inclusive, format YYYY‑MM‑DD.

## Outputs
- raw_data table: append‑only store of raw entries with schema (raw_data_id, expression, date, value, fetch_timestamp, version, ingestion_mode, source_file_uri).
- clean_data table: normalized, validated rows with schema (clean_data_id, date, currency, x, y, ref, value, raw_data_id).
- Raw API response files: dumped into Azure Blob Storage for provenance and replay in a json format with schema (date, value, expression, start_date, end_date, fetch_timestamp).

## API Code Mapping and Reference mapping (includes permitted placeholders)
- USD
  - Old code: DB(COV,VOLSWAPTION,USD,<y>,<x>,PAYER,VOLBPVOL) — expected ref: Libor
  - New code: DB(COV,VOLSWAPTION,USDD,<y>,<x>,PAYER,VOLBPVOL) — expected ref: SOFR
- EUR
  - Code: DB(COV,VOLSWAPTION,EUR,<y>,<x>,PAYER,VOLBPVOL) — expected ref: Euribor
- GBP
  - Old code: DB(COV,VOLSWAPTION,GBP,<y>,<x>,PAYER,VOLBPVOL) — expected ref: Libor
  - New code: DB(COV,VOLSWAPTION,GBP,SONIA,<y>,<x>,PAYER,VOLBPVOL) — expected ref: SONIA
- CHF
  - Old code: DB(COV,VOLSWAPTION,CHF,<y>,<x>,PAYER,VOLBPVOL) — expected ref: Libor
  - New code: DB(COV,VOLSWAPTION,CHF,SARON,<y>,<x>,PAYER,VOLBPVOL) — expected ref: SARON

Placeholders (permitted values)
- x ∈ ['1y', '5y']
- y ∈ ['2y', '5y', '10y']
- Only combinations of these x and y are required and allowed.

## Transformation rules
- Use Pydantic models to validate and enforce schema both at API extraction time and before inserting into tables (do not perform ad‑hoc currency string normalization outside models).
- Transform raw API values into normalized rows: one row per (date, currency, x, y, value, ref) with numeric value and ref which are returned by the API and mapped with the API Code Mapping and Reference mapping.
- USD new code (USDD / SOFR) values must be multiplied by sqrt(252) prior to insertion into clean_data.
- The proposed transactional database will be PostgreSQL for produciton and SQLite for local development/mocking.

## Pipeline Flow
1. Extraction Phase:
  - Fetch from API
  - Store in blob storage  
  - Insert into raw_data with version
  - Trigger transformation

2. Transformation Phase:
  - Retrieve unprocessed combinations
  - Retrieve processed combinations with new version
  - Transform and validate
  - Upsert into clean_data
  - Return metrics

## Ingestion behavior and modes
- Daily incremental (default)
  - Runs on business days via Prefect schedule. For these runs start_date and end_date are the same and point to the pipeline running date.
  - Loads only new‑code series by default.
  - For each expression/date window, fetch only dates+data-codes not already present in raw_data (avoid refetching identical ranges).
- Optional old‑codes mode
  - Enabled via runtime flag; includes old codes for same currency/date ranges.
  - Uses same date range as Daily incremental.
- Historical backfill mode for old codes only (manual)
  - Accepts start_date and end_date parameters.
  - Constraints: start_date must be <= end_date.
  - Backfills must be resumable, chunked, and idempotent.

## Idempotency, deduplication and conflict resolution
- Re‑running the same extraction for the same expression/date range must not create duplicate clean_data rows.
- If an extraction produces duplicate rows in raw_data, emit a warning.
- raw_data remains append‑only; duplicates are represented as additional versions:
  - Add a version column in raw_data: integer ordered by fetch_timestamp for that expression/date combination; latest version = highest number.
- Source file:
  - Each raw_data row must include source_file (blob URI) to identify the dumped raw API response.
- Upserts:
  - Clean_data writes should be transactional: insert new rows or upsert (based on deduplication policy) to maintain idempotency.

## Validation and data quality rules
- Reject or flag rows with missing or non‑numeric values.
- Enforce allowed x/y values; log unexpected combinations.
- Confirm numeric input before applying USD sqrt(252) multiplier.
- Emit metrics per run into logs: rows_fetched, rows_accepted, rows_rejected, conflicts_detected, backfill_progress_percent.

## Operational requirements
- Orchestration and scheduling via Prefect; daily business‑day schedule and manual triggers for flags/backfills.
- Retry policy: transient API failures retried with exponential backoff; persistent failures alert on-call.

## Security and access
- API credentials and secrets stored securely in Azure Key Vault, not in plain text.
- Least privilege DB credentials for writes/updates; blob storage keys scoped appropriately.

## Test cases (high level)
- Default daily ingestion: only new codes fetched and stored; clean_data contains expected rows for permitted x/y.
- Old‑codes ingestion flag: old codes fetched and stored; no duplicate clean_data rows.
- Historical backfill: loads full historical span for old codes within constraints and does not duplicate clean rows; can resume after interruption.
- Deduplication/versioning: repeated ingestion produces raw_data versions and does not create duplicate clean_data rows; overlapping raw_data issues generate warnings.
- Blob dump verification: each raw fetch produces a blob file, and raw_data.source_file_uri points to it.
- Error handling: simulated API intermittent failures trigger retries; persistent failures generate alerts and do not corrupt tables.

