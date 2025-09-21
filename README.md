# Market Data Pipeline

A production-grade ETL pipeline for extracting option volatility financial data, built following the requirements specification.

## Overview

This pipeline implements the following flow:
1. **Extract**: Fetch data from MarketData API, store raw responses in blob storage, insert into `raw_data` table
2. **Transform**: Parse expressions, validate data, apply transformations (USD SOFR sqrt(252) multiplier), create clean records
3. **Load**: Upsert cleaned data into `clean_data` table with deduplication and versioning

## Architecture

```
src/
├── core/
│   └── database.py           # Database connection and setup
├── pipeline/
│   ├── extract/
│   │   └── extractor.py      # API data extraction and raw storage
│   ├── transform/
│   │   └── transformer.py    # Data parsing and validation  
│   ├── load/
│   │   └── loader.py         # Clean data loading with upserts
│   └── orchestrator.py       # ETL coordination with Prefect flows
├── models.py                 # Pydantic/SQLModel schemas with validation
└── market_data_api.py        # Mock API client
config/
└── logging_config.py         # Centralized logging configuration
logs/
└── market_data.log           # Application logs (created automatically)
```

## Features

- **Multiple ingestion modes**: default (new codes), old_codes (all codes), historical (old codes backfill)
- **Prefect orchestration**: Built-in flow and task decorators for workflow management
- **Centralized logging**: Structured logging with console and file output, multiple log levels
- **Idempotency**: Re-running the same extraction/date range doesn't create duplicates
- **Versioning**: Raw data includes version numbers for tracking duplicate fetches
- **Data validation**: Enhanced Pydantic models with custom SQLModel validation
- **Blob storage**: Raw API responses stored as JSON files (simulating Azure blob storage)
- **Transformations**: USD SOFR values multiplied by sqrt(252) as per specification
- **Currency mapping**: Automatic reference rate mapping (Libor, SOFR, SONIA, SARON, Euribor)
- **Comprehensive metrics**: Detailed reporting on pipeline execution with Prefect logging
- **Database migrations**: Alembic integration for schema versioning

## Database Schema

### raw_data table
- `raw_data_id`: Primary key
- `expression`: API expression code
- `date`: Data date from API
- `value`: Raw volatility value (basis points)
- `fetch_timestamp`: When API call was made
- `version`: Version number for expression/date combination
- `ingestion_mode`: Pipeline mode (default/old_codes/historical)
- `source_file_uri`: URI to blob storage file

### clean_data table
- `clean_data_id`: Primary key
- `date`: Data date
- `currency`: Currency code (USD, EUR, GBP, CHF)
- `x`: Option expiry tenor (1y, 5y)
- `y`: Swap tenor (2y, 5y, 10y)
- `ref`: Reference rate (Libor, SOFR, SONIA, SARON, Euribor)
- `value`: Transformed volatility value
- `raw_data_id`: Reference to source raw data

## Usage

### Demo Mode (no arguments)
```bash
uv run python main.py
```
Runs a demonstration with sample data showing different pipeline modes and data integrity validation.

### CLI Mode
```bash
# Default mode (new codes only)
uv run python main.py --mode default --start-date 2025-09-01 --end-date 2025-09-05

# Include old codes
uv run python main.py --mode old_codes --start-date 2025-09-01 --end-date 2025-09-05

# Historical backfill (old codes only)
uv run python main.py --mode historical --start-date 2025-08-01 --end-date 2025-08-31

# Setup database tables
uv run python main.py --setup-db --mode default --start-date 2025-09-01 --end-date 2025-09-01
```

### Database Migrations
```bash
# Generate new migration
alembic revision --autogenerate -m "Add new table"

# Apply migrations
alembic upgrade head

# Check current revision
alembic current
```

## API Expression Mapping

The pipeline supports these expression formats:

### New Codes (Default Mode)
- `DB(COV,VOLSWAPTION,USDD,<y>,<x>,PAYER,VOLBPVOL)` → USD/SOFR (sqrt(252) multiplier)
- `DB(COV,VOLSWAPTION,EUR,<y>,<x>,PAYER,VOLBPVOL)` → EUR/Euribor
- `DB(COV,VOLSWAPTION,GBP,SONIA,<y>,<x>,PAYER,VOLBPVOL)` → GBP/SONIA
- `DB(COV,VOLSWAPTION,CHF,SARON,<y>,<x>,PAYER,VOLBPVOL)` → CHF/SARON

### Old Codes (Old Codes/Historical Mode)
- `DB(COV,VOLSWAPTION,USD,<y>,<x>,PAYER,VOLBPVOL)` → USD/Libor
- `DB(COV,VOLSWAPTION,GBP,<y>,<x>,PAYER,VOLBPVOL)` → GBP/Libor
- `DB(COV,VOLSWAPTION,CHF,<y>,<x>,PAYER,VOLBPVOL)` → CHF/Libor

Where:
- `x` ∈ ['1y', '5y'] (option expiry)
- `y` ∈ ['2y', '5y', '10y'] (swap tenor)

## Pipeline Behavior

### Idempotency
- Re-running the same extraction creates new versions in raw_data but doesn't duplicate clean_data
- Clean records are only updated if newer raw data has higher version or later timestamp

### Deduplication
- Unique constraints prevent duplicate clean data combinations
- Raw data versioning tracks multiple fetches of same expression/date

### Error Handling
- Individual expression failures don't stop pipeline execution
- Comprehensive error reporting and metrics
- Data validation at multiple stages

## Logging

The pipeline includes a centralized logging system with dual output:

### Configuration
- **Console output**: INFO level and above with clean formatting
- **File output**: DEBUG level and above with detailed context (`logs/market_data.log`)
- **Automatic setup**: Logging is configured automatically on application start

### Log Files
- All logs are written to `logs/market_data.log`
- Logs include timestamps, log levels, module names, and function context
- File logs persist across runs for debugging and monitoring

## Development

### Dependencies
- Python 3.12+
- **Core Pipeline**: SQLModel/SQLAlchemy, Pydantic, pandas
- **Orchestration**: Prefect 3.0+ with flow and task decorators
- **Cloud Integration**: Azure Blob Storage, Azure Identity, Azure Key Vault
- **Database**: Alembic for migrations, psycopg2 for PostgreSQL
- **Development**: pytest, black, isort, mypy, pre-commit hooks

### Setup
```bash
# Install dependencies
uv install

# Install development dependencies
uv install --group dev

# Setup database tables
uv run python main.py --setup-db --mode default --start-date 2025-09-01 --end-date 2025-09-01

# Run pipeline
uv run python main.py
```

### Testing
```bash
# Run all tests
uv run pytest

# Run specific test modules
uv run pytest tests/test_extractor.py
uv run pytest tests/test_transformer.py
uv run pytest tests/test_loader.py

# Run with coverage
uv run pytest --cov=src --cov-report=html
```

## Prefect Orchestration Setup

### Work Pool Creation with Automatic Infrastructure Provisioning

This project can be deployed using Prefect's serverless infrastructure with automatic provisioning. Prefect supports multiple cloud providers for push work pools.

#### Prerequisites

Before creating a work pool with automatic infrastructure provisioning, ensure you have:

1. **Cloud CLI installed and authenticated**:
   - **Azure**: Install Azure CLI and authenticate

2. **Required permissions**

3. **Docker installed** for building and pushing images to registry

#### Create Work Pool with Automatic Provisioning

Run the following command to create a new push work pool and automatically provision the necessary infrastructure:

```bash
prefect work-pool create --type aci:push --provision-infra market-data-pool
```

#### Deploy the Pipeline

The pipeline is already configured with Prefect flows and tasks. The orchestrator includes a pre-configured deployment:

```python
# The orchestrator.py already includes this deployment configuration:
if __name__ == "__main__":
    run_pipeline.deploy(
        name="market-data-daily-pipeline",
        work_pool_name="azure-container-instance-pool",
        description="Daily market data ETL pipeline for business days",
        version="1.0.0",
        tags=["market-data", "etl", "daily", "production"],
        cron="0 8 * * 1-5",  # Weekdays at 8 AM
        parameters={
            "start_date": date.today(),
            "end_date": date.today(),
            "ingestion_mode": RunModeEnum.DEFAULT,
            "expressions": None,
            "db_connection_string": "sqlite:///market_data.db",
        },
    )
```

To deploy to your work pool:

```bash
# Deploy the pipeline
cd src/pipeline
uv run python orchestrator.py
```

## Production Considerations

This implementation focuses on core pipeline logic and local development. For production deployment:

- Replace mock API with real MarketData API client
- Implement Azure Blob Storage instead of local file storage
- Use PostgreSQL instead of SQLite
- Add Prefect orchestration and scheduling (see above)
- Implement proper logging and monitoring
- Add Azure Key Vault for secrets management
- Implement retry logic with exponential backoff
- Add alerting for pipeline failures
- Use push work pools for serverless execution (no workers required)
- Log files are appended to by default. Consider implementing log rotation for production use.