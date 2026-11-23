# PySpark PostgreSQL ETL Project

## Overview
An end-to-end data engineering project integrating PySpark with PostgreSQL.

## Technologies
- Python
- Apache PySpark
- PostgreSQL
- JDBC
- SQL

## Project Features
1. PostgreSQL database connectivity
2. Reading employee and department data using Spark JDBC
3. Data cleaning and transformation
4. Department-wise salary analysis
5. Writing transformed data back to PostgreSQL
6. Incremental ETL processing using watermark control

## Project Files
- postgres_connection.py
- pyspark_postgres.py
- pyspark_data_cleaning.py
- pyspark_incremental.py

## Setup
1. Install Python, Java and PostgreSQL.
2. Install PySpark and psycopg2-binary.
3. Download the PostgreSQL JDBC driver into the jars folder.
4. Update the JDBC driver path in the scripts if needed.
5. Run the Python scripts from PowerShell.

## Security
Database passwords are requested interactively and should never be committed to GitHub.

## Author
Sabarivasan Manoharan
