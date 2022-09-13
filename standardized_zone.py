"""
Il seguente script serve a gestire l'evoluzione dei dati dalla Standardized Zone alla Curated Zone
"""
#import
import glob

import Utilities

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import shutil
import sys

"""

"""
def opere_lista(spark):
    fileDirectory = 'standardized/opere/lista/'
    moveDirectory = 'standardized/opere/lista/processed/'
    destinationDirectory = 'curated/opere/lista/'
    files = os.listdir(fileDirectory)


    #legge tutti i file nella directory
    df = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(fileDirectory)
    df.printSchema()
    df.show()



def main():
    print("Da Raw Standardized a Curated Zone")

    # Configurazione SparkSession
    spark = SparkSession.builder. \
        master("local"). \
        config("spark.driver.bindAddress", "localhost"). \
        config("spark.ui.port", "4050"). \
        appName("MinTemperatures"). \
        enableHiveSupport(). \
        getOrCreate()

    opere_lista(spark)

if __name__ == "__main__":
    main()









