# ETL-Project-with-PySpark-and-Postgreysql-
An ETL project that designs and builds a PostgreSQL database with tables, extracts data from CSV files, loads it into the database, transforms the data using PySpark, and schedules the ETL pipeline with Airflow.

## Tools used: 
Postgrey Sql, Pycharm, Pyspark, jdk, Apache Spark

## Step 1: creating database and tables in SQL Shel(psql) of postgreysql:
- Open SQL Shell, and create database and tables as shown below,
- CREATE DATABASE etl_project;
- \c etl_project           -- this is used to connect to database

## Step 2: extract data from csv files and upload into the database tables,
if csv file contains header then use the syntax : \copy movies from 'path of file' DELIMITER ',' CSV HEADER;
my csv does not include header so skipping it.
![Screenshot of Extracting data from CSV](https://github.com/Dipapatil/ETL-Project-with-PySpark-and-Postgreysql-/blob/main/load_tables_from_csv_file.png)

## Step 3: Transformation using pyspark
- You can install PySpark in PyCharm by running the command pip install pyspark in the terminal or by navigating to Settings > Project Interpreter, searching for PySpark package, and selecting it for installation.
- Install apache spark using this link - https://spark.apache.org/downloads.html
- Download winutils.exe from this link and place it in newly created folder C:\hadoop\bin - https://github.com/cdarlint/winutils/tree/master/hadoop-3.2.2/bin
- install jdk latest version - https://jdk.java.net/
- In setting search for environment varialbe it is in Advanced tab, add new system variables
  - HADDOP_HOME with value C:\hadoop
  - JAVA_HOME with value where you installed java for me it is C:\Program Files\Java\jdk-17.0.1
  - PYTHONPATH with value C:\spark-3.5.5-bin-hadoop3\python\lib\py4j-0.10.9.7-src.zip;
  - SPARK_HOME with value C:spark-3.5.5-bin-hadoop3
- after all above steps double click on path system vaiable and add new for each value below :
  - %HADOOP_HOME%
  - %JAVA_HOME%
  - %SPARK_HOME%
  - %PYTHONPATH%
  - %SPARK_HOME%\python\lib\py4j-0.10.9.7 -src.zip
    ![Screenshot of System Varialbes](https://github.com/Dipapatil/ETL-Project-with-PySpark-and-Postgreysql-/blob/main/environment%20vaiables.png)
- Open command prompt and type pyspark to chekck if it is installed correctly.
- This is a script from pycharm to transform data by summarizing it. [Pyspark Script](https://github.com/Dipapatil/ETL-Project-with-PySpark-and-Postgreysql-/blob/main/main.py)
- New summarized table will be created in database after running python script.
- ![Screenshot of Transformed data](https://github.com/Dipapatil/ETL-Project-with-PySpark-and-Postgreysql-/blob/main/transformed_Data.png)
      
