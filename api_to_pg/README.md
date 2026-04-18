# DataExp: GoRest API Ingestion Pipeline

## Overview
A Python-based ETL pipeline designed to extract social post data, perform text analysis (word counting), and store the structured results in a PostgreSQL warehouse.

## Architecture
- **Source:** [GoRest API](https://gorest.co.in/)
- **Processing Engine:** Apache Spark (PySpark)
- **Storage:** Local JSON (Raw) & PostgreSQL (Analytics Layer)

## Prerequisites
1. **Java 8 or 11**: Required for PySpark.
2. **PostgreSQL JDBC Driver**: Place `postgresql-42.7.2.jar` in `~/spark_jars`.
3. **Database**: A PostgreSQL instance running on `localhost:5432` with a database named `analytics`.

## Installation
Set up your virtual environment and install dependencies:
```bash
python3 -m venv airflow_env
source airflow_env/bin/activate
pip install requests pyspark
