## EXTRACT DATA FROM POSTGRESQL DATABSE AND THEN TRANSFORM IT AND LOAD AGAIN BACK TO DATABASE - ETL USING PYSPARK

import pyspark
import os
os.environ['PYSPARK_PYTHON'] = 'python'  # Or provide full path to your python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

# create spark session
spark = pyspark.sql.SparkSession\
    .builder \
    .appName("Python Data Transformation using pyspark") \
    .config('spark.driver.extraClassPath',"C:\Dipali projects\postgrey sql and Spark and Airflow_ETL_Project\etl_project_pyspark\postgresql-42.7.5.jar") \
    .getOrCreate()

# read table  movies from db using spark jdbc

def extract_movies_to_dataframe():
    movies_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/etl_project") \
        .option("dbtable","movies") \
        .option("user","postgres") \
        .option("password","your_password") \
        .option("driver","org.postgresql.Driver") \
        .load()
    #print(movies_df.show())
    return movies_df

# read table users from postgreysql databse etl_project

def exract_users_table_to_df():
    users_df = spark.read \
        .format("jdbc") \
        .option("url","jdbc:postgresql://localhost:5432/etl_project") \
        .option("dbtable","users") \
        .option("user","postgres") \
        .option("password","") \
        .option("driver","org.postgresql.Driver") \
        .load()
    #movies_df.show()
    #print(users_df.show())
    return users_df

## trasformation, creating new table

def transform_avg_ratings(movies_df,users_df):
    avg_rating = users_df.groupby("movie_id").mean("rating")
    #print(avg_rating.show())
    # join movies df and avg_rating
    df = movies_df.join(avg_rating, movies_df.id == avg_rating.movie_id)
    #print(df.show())
    return df


def load_to_table(df):
    mode = "overwrite"
    url="jdbc:postgresql://localhost:5432/etl_project"
    properties = {  "user": "postgres",
                    "password": "your_password",
                    "driver":"org.postgresql.Driver"}
    df.write.jdbc(url=url,
                  table = "avg_ratings",
                  mode = mode,
                  properties= properties)

if __name__ == '__main__':
    movies_df = extract_movies_to_dataframe()
    users_df = exract_users_table_to_df()
    ratings_df = transform_avg_ratings(movies_df,users_df)
    load_to_table(ratings_df)
    print("Data Transformations is complete, new table is created")
