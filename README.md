# San Francisco Fire Incidents ETL & Analytics Pipeline

## Project Overview

This project provides a robust pipeline to ingest, process, and model fire incident data from the City of San Francisco's open data portal. The primary goal is to make this data readily available and efficiently queryable within a PostgreSQL data warehouse, enabling analysis based on time, location (battalion, district), and incident details.

## Core Highlights

* **Automated Incremental Updates:** Fetches only new or updated data from the source API after the initial load, minimizing processing time and API usage. State is managed via a simple timestamp file.
* **Data Quality Focused:** Incorporates data quality checks at multiple stages:
    * **ETL (Python):** Basic validation (null keys, batch duplicates), type coercion during transformation.
    * **DWH (dbt):** Comprehensive tests (uniqueness, non-null, referential integrity, custom expectations) applied after data loading and transformation.
* **Resilient Loading:** Employs an `UPSERT` (insert or update) strategy when loading data into the staging area, ensuring data reflects the latest state from the source and handles potential re-fetching gracefully.
* **Dimensional Modeling for Analytics:** Transforms raw data into a clean, analytics-ready star schema using dbt. This includes fact (`fct_incidents`) and dimension (`dim_time`, `dim_battalion`, `dim_neighborhood_district`, `dim_analysis_neighborhood`) tables, optimized for BI queries and reporting.
* **Modularity & Maintainability:** The Python ETL code is logically separated into extract, transform, and load components. dbt further modularizes the data transformation logic within the warehouse.
* **Configuration Driven:** Sensitive information (credentials) and key parameters (API endpoints, table names) are externalized to an `.env` file, promoting security and easier configuration management.
* **Reproducible Environment:** Leverages Docker Compose to spin up a consistent PostgreSQL database environment, simplifying setup and ensuring reproducibility.

## Technology Choices & Rationale

This project leverages a combination of Python and dbt, hosted within a Dockerized PostgreSQL environment. Here's why:

1.  **Python (ETL):**
    * **Flexibility:** Ideal for interacting with external APIs (`requests`), handling diverse data formats (JSON), and performing complex, row-by-row transformations or validations (`pandas`) that might be cumbersome in pure SQL.
    * **Ecosystem:** Rich libraries for data manipulation, API interaction, database connectivity (`SQLAlchemy`, `psycopg2`), and configuration (`python-dotenv`).
    * **Control:** Provides fine-grained control over the extraction and initial loading process, including state management for incremental loads and pre-load data quality checks.

2.  **PostgreSQL (Data Warehouse):**
    * **Robust & Open Source:** A powerful, feature-rich, and widely adopted relational database.
    * **JSON Support:** Good capabilities for handling JSON if needed, although this project primarily uses standard relational types.
    * **Window Functions & CTEs:** Excellent SQL capabilities suitable for complex transformations performed by dbt.
    * **Docker Integration:** Easily containerized for development and testing consistency.

3.  **Docker/Docker Compose:**
    * **Environment Consistency:** Ensures the PostgreSQL database runs the same way across different developer machines or deployment environments.
    * **Simplified Setup:** Automates the process of setting up the database service with the correct configuration.
    * **Isolation:** Keeps the database environment separate from the host system.

4.  **dbt (Data Transformation & Modeling):**
    * **SQL-First Transformation:** Enables data transformation logic to be written primarily in SQL, which is often the most efficient language for set-based operations within a data warehouse.
    * **Modularity & Reusability:** Encourages breaking down complex transformations into smaller, reusable models (using `ref`). Macros allow for DRY (Don't Repeat Yourself) code.
    * **Testing:** Built-in framework for defining and running data quality and integrity tests directly on warehouse tables.
    * **Documentation:** Automatically generates documentation about models, columns, and tests.
    * **Version Control Friendly:** Manages transformations as code, making them easy to version control with Git.
    * **Separation of Concerns:** Clearly separates the *in-warehouse* transformation logic from the initial *ETL* process handled by Python.

**Leveraging this Stack:**

This combination allows each tool to play to its strengths: Python handles the interaction with the outside world (API) and initial data preparation/loading, while dbt excels at transforming, testing, and modeling data *within* the warehouse using SQL. Docker ensures the underlying database is consistent and easy to manage. This layered approach results in a more robust, maintainable, and testable data pipeline compared to performing all steps solely in Python or SQL scripts.

## How to Use

1.  **Setup:** Clone the repository, create/configure the `.env` file, install Python dependencies (`requirements.txt`) and dbt, and start the PostgreSQL container (`docker-compose up -d`).
2.  **Run ETL:** Execute `python -m etl.main` to fetch data and load it into the staging table.
3.  **Run dbt:** Navigate to `dbt_project/` and run `dbt run` to build the dimensional models, followed by `dbt test` to validate the data.
4.  **Analyze:** Connect to the PostgreSQL database and query the tables in the `analytics` schema (or the schema defined in `.env`). Use `dbt_project/analysis/incident_aggregations_report.sql` as an example.

*(Refer to the original, more detailed README for full setup and usage instructions if needed.)*
