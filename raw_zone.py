"""
Il seguente script serve a gestire l'evoluzione dei dati dalla Raw Zone alla Standardized Zone
"""
#import
import glob

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import col, avg
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
import os
import shutil
import sys

def opere_lista():
    fileDirectory = 'raw/opere/lista/'
    moveDirectory = 'raw/opere/lista/processed/'
    files = os.listdir(fileDirectory)

    os.makedirs(moveDirectory, exist_ok=True)
    for fname in files:
        if (os.path.isfile(fileDirectory + fname)):
            shutil.move(fileDirectory + fname, moveDirectory +fname)


def main():
    print("Da Raw Zone a Standardized Zone")

    # Configurazione SparkSession
    spark = SparkSession.builder. \
        master("local"). \
        config("spark.driver.bindAddress", "localhost"). \
        config("spark.ui.port", "4050"). \
        appName("MinTemperatures"). \
        enableHiveSupport(). \
        getOrCreate()

    opere_lista()

if __name__ == "__main__":
    main()









