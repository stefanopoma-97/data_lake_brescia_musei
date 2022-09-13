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
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, desc
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

    directoryAutori = 'standardized/opere/autori/'


    #recupero lista opere, la ordino e elimino duplicati
    lista_opere = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(fileDirectory)
    lista_opere = lista_opere.orderBy(desc("data_creazione")).drop_duplicates(subset=["id"])
    lista_opere.show()


    lista_autori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(directoryAutori)
    lista_autori = lista_autori.orderBy(desc("data_creazione")).drop_duplicates(subset=["id"])
    lista_autori.show()

    join = lista_opere.alias("lista_opere") \
            .join(lista_autori.alias("lista_autori"), \
                  (func.col("lista_opere.autore") == func.col("lista_autori.nome")),\
                  "left"
                  )\
            .select(func.col("lista_opere.id").alias("id") )
    join.show()




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









