# Pythonic Interface for Great Expectations - Requirements

## 1. Summary

This document has the requirements for creating a high-level, Pydantic-based Python interface for Quants to define and manage Great Expectations (GX) validations. The goal is to abstract away the complexity of the GX API and automate the deployment of a validation object via a CI/CD workflow that uses Github Actions.

## 2. User Persona & Story

*   **Persona:** Quantitative Analyst
*   **User Story:** Quants want to define data quality checks outside of the default yaml file configuration offered by GX. As Quants have python experience that we can leverage, it would be ideal to define data quality checks for pipeline datasets using a simple Python interface so they can programatically create the GX expectation validations. That way they don't need to udnerstand the low-level details of GX or the deployment process. The idea is that they only own the validations.

## 3. Functional Requirements

### 3.1. Python Interface (`GXFacade`)

- A Pydantic-based class or set of models will be created to allow declarative definition of a. Validation object. The interface must allow the user to specify:
  - `datasource_name`: The name of the GX Datasource to connect to which defaults to the database engine in `src/core/database.py`. These will be mantained by the Data Engineer so the interface won't provide a way to create one.
  - `asset_name`: The name of the GX Data Asset (table) to validate.
  - `batch_definition`: The batching strategy to use.
  - The interface must provide a method to add expectations which need to include:
    - The expectation type.
    - A target `column`.
    - Required parameters for the expectation.
    - metadata like `severity`, `"warning"`, or `"error"`.
  - The interface must have a `build()` method that:
    1. Initializes a GX Data Context.
    2. Constructs a `gx.ValidationDefinition`.
    3. Saves the artifacts to the local GX project structure.

### 3.2. Analyst Workflow

- Quants will create a new Python script in `src/pipeline/validate/analyst_validations/`.
- They'll need to import and use the `GXFacade` to define their validation.
- The script will be executable and self-contained, responsible for generating one validation.

### 3.3. CI/CD Automation (GitHub Actions)

- A new GitHub Actions workflow will be created, triggered on pull requests to the `main` branch.
- The workflow will detect changes (new or modified files) within the `src/pipeline/validate/analyst_validations/` directory.
- For each changed file, the workflow will:
  1. Set up a Python environment using `uv`.
  2. Execute the analyst's Python script with the command `uv run python src/pipeline/validate/analyst_validations/new_validation.py`.
  3. If the script fails, the workflow fails.
  4. If the script succeeds, the workflow uploads the newly generated artifacts to a specified Azure Blob Storage container in a `great-expectations/{pipeline}/` that would be retrieved by the Prefect flow when running the validation task.

## 4. Technical Requirements

- **Usability:** clear type hinting, docstrings, and validation error messages provided by Pydantic
- **Idempotency:** Running the GitHub Action multiple times on the same commit should produce the same result, which means the same artifact files are uploaded/overwritten in blob storage.
- **Security:** credentials should be stored as GitHub secrets.
