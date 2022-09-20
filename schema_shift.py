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
    .option("inferSchema","false")\
    .csv(fileDirectory)

if "Nuovo" in df.columns:
    df=df.withColumnRenamed("Nuovo", "colonna da scartare")

df.printSchema()
df.show()