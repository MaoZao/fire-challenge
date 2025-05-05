# San Francisco Fire Incidents ETL & Analytics Pipeline

## Project Overview

This project provides a robust pipeline to ingest, process, and model fire incident data from the City of San Francisco's open data portal. The primary goal is to make this data readily available and efficiently queryable within a PostgreSQL data warehouse, enabling analysis based on time, location (battalion, district), and incident details. The processed data is then visualized using Apache Superset.

## Core Highlights

* **Automated Incremental Updates:** Fetches only new or updated data from the source API after the initial load, minimizing processing time and API usage. State is managed via a simple timestamp file.
* **Data Quality Focused:** Incorporates data quality checks at multiple stages:
    * **ETL (Python):** Basic validation (null keys, batch duplicates), type coercion during transformation.
    * **DWH (dbt):** Comprehensive tests (uniqueness, non-null, referential integrity, custom expectations) applied after data loading and transformation.
* **Resilient Loading:** Employs an `UPSERT` (insert or update) strategy when loading data into the staging area, ensuring data reflects the latest state from the source and handles potential re-fetching gracefully.
* **Dimensional Modeling for Analytics:** Transforms raw data into a clean, analytics-ready star schema using dbt. This includes fact (`fct_incidents`) and dimension (`dim_time`, `dim_battalion`, `dim_neighborhood_district`) tables, optimized for BI queries and reporting.
* **Interactive Visualizations:** Provides pre-configured dashboards and charts via Apache Superset for easy exploration of the incident data.
* **Modularity & Maintainability:** The Python ETL code is logically separated into extract, transform, and load components. dbt further modularizes the data transformation logic within the warehouse. Superset configurations are stored separately.
* **Configuration Driven:** Sensitive information (credentials) and key parameters (API endpoints, table names) are externalized to an `.env` file, promoting security and easier configuration management.
* **Reproducible Environment:** Leverages Docker Compose to spin up consistent PostgreSQL and Superset environments, simplifying setup and ensuring reproducibility.

## Technology Choices & Rationale

This project leverages a combination of Python, dbt, PostgreSQL, and Apache Superset, orchestrated with Docker Compose. Here's why:

1.  **Python (ETL):**
    * **Flexibility:** Ideal for interacting with external APIs (`requests`), handling diverse data formats (JSON), and performing complex, row-by-row transformations or validations (`pandas`).
    * **Ecosystem:** Rich libraries for data manipulation, API interaction, database connectivity (`SQLAlchemy`, `psycopg2`), and configuration (`python-dotenv`).
    * **Control:** Provides fine-grained control over the extraction and initial loading process, including state management for incremental loads and pre-load data quality checks.

2.  **PostgreSQL (Data Warehouse):**
    * **Robust & Open Source:** A powerful, feature-rich, and widely adopted relational database.
    * **Compatibility:** Works seamlessly with both dbt and Superset.
    * **Window Functions & CTEs:** Excellent SQL capabilities suitable for complex transformations performed by dbt.
    * **Docker Integration:** Easily containerized for development and testing consistency.

3.  **dbt (Data Transformation & Modeling):**
    * **SQL-First Transformation:** Enables efficient, set-based data transformation logic within the data warehouse.
    * **Modularity & Reusability:** Encourages breaking down complex transformations into smaller, reusable models (`ref`).
    * **Testing & Documentation:** Built-in framework for data quality tests and automatic documentation generation.
    * **Version Control Friendly:** Manages transformations as code.
    * **Separation of Concerns:** Clearly separates *in-warehouse* transformation logic from the initial *ETL* process.

4.  **Apache Superset (Visualization):**
    * **Open Source BI:** A powerful, enterprise-ready business intelligence web application.
    * **Rich Visualizations:** Offers a wide array of chart types and customizable dashboards.
    * **SQL Interface:** Allows direct querying of the data warehouse and exploration via its SQL Lab.
    * **Integration:** Connects easily to PostgreSQL and benefits from the clean data model created by dbt.
    * **Docker Deployment:** Can be readily deployed using Docker Compose alongside other services.

5.  **Docker/Docker Compose:**
    * **Environment Consistency:** Ensures PostgreSQL and Superset run the same way across different environments.
    * **Simplified Setup:** Automates the setup and linking of database and visualization services.
    * **Isolation:** Keeps service environments separate from the host system.

**Leveraging this Stack:**

This combination allows each tool to play to its strengths: Python handles external interaction and initial loading, dbt transforms and models data within the warehouse, PostgreSQL stores the data reliably, and Superset provides the user interface for exploration and visualization. Docker ties it all together for easy setup and management. This layered approach results in a robust, maintainable, and user-friendly data pipeline.

*(Note: The project structure now includes a `superset/` directory containing necessary configuration files for setting up Superset dashboards and connections.)*

## How to Use

1.  **Setup:** Clone the repository, create/configure the `.env` file, install Python dependencies (`requirements.txt`) and dbt, and start the PostgreSQL and Superset containers (`docker-compose up -d`). *(Ensure Docker Compose file includes the Superset service)*.
2.  **Run ETL:** Execute `python -m etl.main` to fetch data and load it into the staging table.
3.  **Run dbt:** Navigate to `dbt_project/` and run `dbt run` to build the dimensional models, followed by `dbt test` to validate the data.
4.  **Access Superset:** Open your web browser and navigate to the Superset URL (commonly `http://localhost:8088` if using default settings). Log in using the credentials configured during setup.
5.  **Explore Dashboards:** Navigate to the pre-built dashboards related to SF Fire Incidents to visualize the data. You can also use SQL Lab within Superset to query the `analytics` schema directly.


