# DataExp: Personal Expense ETL Pipeline 📊
Postgres ➔ PySpark ➔ DuckDB (WSL2 Environment)
This project implements a local Medallion Architecture to transform raw transactional data from an OLTP source (PostgreSQL) into a high-performance OLAP warehouse (DuckDB) using Apache Spark as the transformation engine.

🏗️ Architecture Overview
Bronze Layer (Source): Raw expense data stored in PostgreSQL (real_expense table).

Silver Layer (Processing): PySpark performs data cleansing, filtering (expenses only), and temporal feature engineering (extracting Year, Month, Day).

Gold Layer (Warehouse): Processed data is ingested into DuckDB for lightning-fast analytical queries and reporting.

🛠️ Tech Stack & Prerequisites
OS: Windows Subsystem for Linux (WSL2 - Ubuntu)

Engine: Apache Spark 4.0.1

Languages: Python 3.10+, SQL

Databases: PostgreSQL (Source), DuckDB (Warehouse)

Drivers: * postgresql-42.7.2.jar (JDBC)

duckdb_jdbc-1.1.3.jar (JDBC)

🚀 Getting Started
1. Clone & Environment Setup
Bash
git clone <your-repo-url>
cd sparkwork
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
2. Dependency Configuration
Ensure your JDBC drivers are located in ~/spark_jars/. You can download them via:

Bash
mkdir -p ~/spark_jars
wget -P ~/spark_jars/ https://jdbc.postgresql.org/download/postgresql-42.7.2.jar
wget -P ~/spark_jars/ https://repo1.maven.org/maven2/org/duckdb/duckdb_jdbc/1.1.3/duckdb_jdbc-1.1.3.jar
3. Execution Flow
Step A: Run the Spark Transformation
This script connects to Postgres, transforms the schema, and outputs Parquet files to /tmp/transformed_expenses/.

Bash
python3 real_expense_transform.py
Step B: Ingest into DuckDB Warehouse
Open the DuckDB CLI to create the persistent warehouse:

Bash
duckdb expense_load.db
Inside the DuckDB prompt, run:

SQL
CREATE OR REPLACE TABLE expense AS 
SELECT * FROM read_parquet('/tmp/transformed_expenses/*.parquet');
📊 Sample Analytical Queries (DuckDB)
Monthly Spending Trend:

SQL
SELECT year, month, SUM(expense_debit) as total_spend
FROM expense
GROUP BY 1, 2
ORDER BY year DESC, total_spend DESC;
Top Expense Categories:

SQL
SELECT category, SUM(expense_debit) as volume
FROM expense
GROUP BY category
ORDER BY volume DESC;
🛡️ Project Governance
Data Quality: Rows with null or zero expense_debit are filtered out during the Silver phase.

Performance: Uses Parquet as an intermediate format to leverage DuckDB's vectorized execution.

Security: Database credentials should be managed via environment variables (not hardcoded).

👨‍💻 Author
Deepak Ray GCP Data Architect | Data Strategy & Software Craftsmanship
