# Walmart Data Engineering Pipeline

A personal end-to-end data engineering project that builds a modern retail analytics pipeline using Apache Airflow, dbt, Databricks, and Docker. The goal of the project is to ingest retail transaction data, transform it into a curated analytics layer, and orchestrate the workflow in a clean, production-oriented structure.

This project is designed to demonstrate practical data engineering skills: pipeline orchestration, modular transformation logic, incremental thinking, and layered warehouse modeling using an example Walmart-style sales dataset.

## Project Overview

The repository models a retail data platform for Walmart-style operations, using a simplified but realistic dataset containing:

- customers
- stores
- employees
- products
- orders
- order items

The pipeline moves data through a bronze-to-silver-to-gold pattern inspired by medallion architecture, with transformations organized by business meaning rather than raw source logic.

### What this project demonstrates

- Airflow orchestration for scheduled ETL/ELT workflows
- dbt-based transformation layer with source, staging, and business models
- Data modeling practices for fact and dimension-style outputs
- Dockerized local environment for repeatable setup
- Source freshness checks and data-quality style validation patterns
- Practical warehouse design for analytics consumption

## Business Context

This project simulates a retail data warehouse for a large retail chain. The business use case is to transform operational sales and customer data into a clean reporting layer that supports business questions such as:

- Which products are selling most frequently?
- Which stores are performing best?
- How do customer and order patterns vary by region?
- How can operational data be modeled for downstream analytics and BI?

## Architecture

The solution combines the following components:

- Airflow for orchestration and workflow scheduling
- Docker Compose for local infrastructure setup
- dbt for transformation logic and modeling
- Databricks as the execution/data warehouse platform
- Source CSVs loaded into the warehouse and modeled into curated tables

### High-level flow

1. Raw source data is loaded into the warehouse
2. Airflow runs the orchestration pipeline
3. Source freshness and validation checks are executed
4. Raw tables are transformed into silver models
5. Business-level models are built in the gold layer
6. Resulting tables are ready for analytics or downstream reporting

## Repository Structure

```text
airflow_dbt/
├── docker-compose.yaml
├── Dockerfile
├── requirements.txt
├── .env
├── dags/
│   └── orchestrate.py
├── data project setup/
│   ├── superstore_dataset/
│   ├── ddl/
│   └── load_data.py
├── superstore/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── source/
│   │   ├── silver_t/
│   │   ├── silver_b/
│   │   └── gold/
│   ├── macros/
│   ├── snapshots/
│   ├── tests/
│   └── target/
└── README.md
```

## Core Components

### 1. Orchestration with Airflow

The Airflow DAG defines a production-style pipeline sequence:

- ingest data from the source system
- clean target artifacts
- check source freshness
- run silver transformations
- execute validation tests
- build gold-layer models and snapshots

The orchestration logic is implemented in [airflow_dbt/dags/orchestrate.py](dags/orchestrate.py).

### 2. dbt transformation layers

The dbt project organizes data into logical layers:

- source: raw warehouse sources and metadata
- silver_t: technical transformations and cleaned source tables
- silver_b: business-ready models and joined analytical views
- gold: curated reporting-ready outputs and ephemeral logic

The project configuration and model conventions are defined in [airflow_dbt/superstore/dbt_project.yml](superstore/dbt_project.yml).

### 3. Warehouse modeling

The project uses a layered modeling strategy:

- normalize/clean raw source columns
- standardize names, timestamps, and null handling
- join related entities into broader business views
- expose curated models for reporting and analytics

The business model in [airflow_dbt/superstore/models/silver_b/obt_b.sql](superstore/models/silver_b/obt_b.sql) shows a joined analytical view combining orders, customers, products, employees, and store context.

## Data Pipeline Flow

This project follows a simple but realistic ELT pattern:

- Source data is ingested into a Databricks warehouse
- dbt reads from the source schema
- transformations are applied in a modular, reusable way
- outputs are materialized as tables for downstream use

This approach keeps raw data intact while enabling structured analytics layers for reporting and decisioning.

## Tech Stack

- Python
- Apache Airflow
- dbt Core
- Databricks SQL / Lakehouse
- Docker / Docker Compose
- SQL modeling and transformation logic

## Setup Instructions

### Prerequisites

- Docker Desktop or Docker Engine
- Python 3.11+
- Access to Databricks workspace or equivalent compatible warehouse
- Git

### 1. Clone repository

```bash
git clone <repository-url>
cd airflow_dbt
```

### 2. Install dependencies

```bash
pip install -r requirements.txt
```

### 3. Start the environment

```bash
docker compose up --build
```

### 4. Run the dag workflow

Once the Airflow containers are running, the pipeline can be triggered from the Airflow UI or via the project orchestration logic.

### 5. Run dbt locally

From the dbt project directory:

```bash
cd superstore
dbt debug
dbt run
dbt test
```

## Important Design Decisions

### 1. Layered warehouse architecture

The project intentionally separates raw source, cleaned silver, and curated gold layers. This keeps the pipeline maintainable and makes debugging easier when issues occur in upstream data or transformation logic.

### 2. Airflow-first orchestration

Airflow is used not as a simple scheduler but as the control plane for the workflow. Each stage is explicitly ordered, which improves reproducibility and makes downstream failures easier to diagnose.

### 3. dbt as the transformation backbone

dbt is used as the primary transformation tool because it encourages version-controlled SQL, modular modeling, and testable transformations. It also brings strong engineering habits to analytics code.

### 4. Business-centric modeling

The project favors business interpretation over raw table dumping. Models are designed to answer practical analytical questions rather than just mirror source objects.

### 5. Containerized local environment

Using Docker Compose allows the project to be run consistently across machines. This is especially valuable for portfolio projects because it demonstrates reproducibility and a realistic deployment mindset.

### 6. Source-freshness and validation mindset

The pipeline includes source freshness checks and dbt tests to encourage quality assurance and operational awareness. This shows a bias toward trustworthy data products rather than one-off scripts.

## Project Impact and Value

This project is valuable because it demonstrates the ability to:

- design and orchestrate data pipelines
- transform raw data into curated analytics outputs
- create maintainable DBT models for business use
- structure data work with software engineering best practices
- explain technical decisions clearly to stakeholders, managers, or peers

## Future Enhancements

Potential improvements for the project include:

- adding data quality checks and alerting
- implementing incremental dbt models for scale
- adding documentation for each mart and fact table
- integrating CI/CD for dbt and Airflow validation
- adding dashboarding or BI connectivity for end-user reporting

## Summary

This project represents a realistic end-to-end data engineering solution for retail analytics. It balances technical depth with readability, making it suitable for portfolio presentation, technical interviews, or demonstrating practical pipeline and warehouse design skills.

---

If you'd like, this README can also be adapted into a more polished recruiter-focused version with a stronger personal brand and project narrative.
