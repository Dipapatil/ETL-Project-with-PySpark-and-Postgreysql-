import pyspark
from pyspark.sql import SparkSession

import os
os.environ['PYSPARK_PYTHON'] = 'python'  # Or provide full path to your python executable
os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'

spark = SparkSession.builder.master("local[1]") \
    .appName('test') \
    .getOrCreate()



data = [("ram",55),("geeta",30)]
df = spark.createDataFrame(data,["Name","Age"])
df.show()
print(df)
