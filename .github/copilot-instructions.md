---
applyTo: '**'
---

# Market Data Pipeline - AI Assistant Instructions

## Pipeline Guidelines
Production-grade financial data ETL pipeline that extracts option volatility data, processes through extract-transform-load phases, and stores in normalized database tables. Implements idempotent processing, versioning, and data validation.

## Core Stack
- **Python 3.12+** with uv package manager
- **Database**: SQLite (local), PostgreSQL (production) with Alembic migrations  
- **Orchestration**: Prefect 3.0+ with @flow and @task decorators
- **Validation**: Pydantic models with custom ValidatedSQLModel base class
- **Cloud**: Azure (Blob Storage, Identity, Key Vault)

## Essential Commands
```bash
# Run Python code
uv run python <script.py>
```

## Architecture
```
src/
├── core/database.py           # Database engine, session management
├── pipeline/
│   ├── extract/extractor.py   # API extraction + blob storage
│   ├── transform/transformer.py # Data parsing, validation, transformation
│   ├── load/loader.py         # Clean data upserts with deduplication
│   └── orchestrator.py        # Prefect flows and task coordination
├── models.py                  # Pydantic/SQLModel schemas
└── market_data_api.py         # Mock API client
```

## Key Patterns

### Models
- Use `ValidatedSQLModel` base class for database tables
- Add comprehensive constraints and indexes in `__table_args__`
- Use `@field_validator` for business logic validation
- Enum classes for currency, tenors, run modes

### Error Handling
- Use specific exception types
- Collect errors in metrics dictionaries  
- Continue processing after individual failures
- Log comprehensive error information

## Development Rules
1. Always use uv for Python execution
2. Follow ValidatedSQLModel pattern for new database models
3. Use Prefect decorators for pipeline components
4. Maintain phase-based architecture (extract → transform → load)
5. Test thoroughly with pytest and mocks
6. Store secrets in Azure Key Vault, not in code
7. Consider idempotency in all data operations
8. Use type hints and comprehensive docstrings
