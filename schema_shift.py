import glob
import Utilities
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, input_file_name, substring_index, current_timestamp, to_timestamp, upper
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import shutil
import sys


conf = SparkConf().setMaster("local").setAppName("CMS")
sc = SparkContext(conf=conf)

# Configurazione SparkSession
spark = SparkSession.builder. \
    master("local"). \
    config("spark.driver.bindAddress", "localhost"). \
    config("spark.ui.port", "4050"). \
    appName("CMS"). \
    enableHiveSupport(). \
    getOrCreate()

print("inizio a spostare le opere da Raw a Standardized")
fileDirectory = 'raw/opere/lista/'
moveDirectory = 'raw/opere/lista/processed/'
destinationDirectory = 'standardized/opere/lista/'



#legge tutti i file nella directory
df = spark.read\
    .option("mergeSchema", "true")\
    .option("delimiter", ";")\
    .option("header", "true") \
    .option("inferSchema","True")\
    .csv(fileDirectory)

#cambio nome a colonna
if "Nuovo" in df.columns:
    df=df.withColumnRenamed("Nuovo", "colonna da scartare")

#aggiungo colonna
"""if 'active' not in csvDf.columns:
  csvDf = csvDf.withColumn('active', lit(None).cast("int"))"""

# Add the source file name (without the extension) as an additional column to help us keep track of data source
"""csvDf = csvDf.withColumn("sourcefile", lit('01-22-2020.csv'.split('.')[0]))
"""

# List all the files we have in our store to iterate through them
"""file_list = [file.name for file in dbutils.fs.ls("dbfs:{}".format(mountPoint))]
for file in file_list:
  print(file)"""


df.printSchema()
df.show()