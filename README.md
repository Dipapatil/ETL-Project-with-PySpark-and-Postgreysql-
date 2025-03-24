# ETL-Project-with-PySpark-and-Postgreysql-
An ETL project that designs and builds a PostgreSQL database with tables, extracts data from CSV files, loads it into the database, transforms the data using PySpark, and schedules the ETL pipeline with Airflow.

## Step 1: creating database and tables in SQL Shel(psql) of postgreysql:
- Open SQL Shell, and create database and tables as shown below,
- CREATE DATABASE etl_project;
- \c etl_project           -- this is used to connect to database

## Step 2: extract data from csv files and upload into the database tables, if csv file contains header then use the syntax : \copy movies from 'path of file' DELIMITER ',' CSV HEADER;
  my csv does not include header so skipping it.

## Step 3: Transformation using pyspark
- You can install PySpark in PyCharm by running the command pip install pyspark in the terminal or by navigating to Settings > Project Interpreter, searching for PySpark package, and selecting it for installation.
