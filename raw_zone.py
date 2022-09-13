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

"""
Vengono lette tutte le opere (da file .csv). 
Viene aggiunto il secolo, gli autori sono messi con la prima lettera maiuscola,
Viene inserito l'header e il nuovo dataframe viene salvato nella standardized zone.

I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano processati ulteriormente
"""
def opere_lista(spark):
    fileDirectory = 'raw/opere/lista/'
    moveDirectory = 'raw/opere/lista/processed/'
    files = os.listdir(fileDirectory)

    schema = StructType([ \
        StructField("ID", StringType(), True), \
        StructField("TagID", IntegerType(), True), \
        StructField("Titolo", StringType(), True), \
        StructField("Tipologia", StringType(), True), \
        StructField("Anno", IntegerType(), True), \
        StructField("Provenienza", StringType(), True), \
        StructField("Autore", StringType(), True),\
        StructField("Timestamp", IntegerType(), True)])

    #legge tutti i file nella directory
    df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)
    df.printSchema()
    df.show()



    os.makedirs(moveDirectory, exist_ok=True)
    for fname in files:
        if (os.path.isfile(fileDirectory + fname)):
            print()
            #shutil.move(fileDirectory + fname, moveDirectory +fname)



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

    opere_lista(spark)

if __name__ == "__main__":
    main()









