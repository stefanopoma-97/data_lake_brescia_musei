#import
import glob

import Utilities

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, desc, input_file_name
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import shutil
import sys
import Utilities



def categorie_visitatori(spark):
    print("credo dataframe per visitatori e categorie")
    directoryCategorie = 'curated/visitatori/categorie/'
    directoryVisitatori = 'curated/visitatori/elenco/'

    if (Utilities.check_csv_files(directoryCategorie) & Utilities.check_csv_files(directoryVisitatori)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                                   ";").csv(
            directoryCategorie)
        lista_visitatori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                                   ";").csv(
            directoryVisitatori)
        lista_categorie.show()
        lista_visitatori.show()

        #TODO si potrebbe prendere solo i visitatori nuovi
        #TODO poi separatamente vedere se c'è una categoria nuova e fare il join con tutti i visitatori
        join = lista_visitatori.alias("visitatori") \
            .join(lista_categorie.alias("categorie"), \
                  (func.col("visitatori.eta") >= func.col("categorie.eta_min")) & (func.col("visitatori.eta") <= func.col("categorie.eta_max")), \
                  "left"
                  ) \
            .select(func.col("visitatori.id").alias("id"), \
                    func.col("visitatori.nome").alias("nome"), \
                    func.col("categorie.id").alias("categoria_id")
                    )
        join.show()

        out = join.groupBy("id","nome").agg(func.collect_list("categoria_id").alias("categorie_id"))
        out.show(truncate=False)
        out.printSchema()


    else:
        print("Attenzione, mancano i visitaotori e/o le categorie")

def opera(spark):
    print("credo dataframe per opere")
    directoryOpere = 'curated/opere/lista/'
    directoryAutori = 'curated/opere/autori/'
    directoryDescrizioni = 'curated/opere/descrizioni/'
    directoryImmagini = 'curated/opere/immagini/'

    if (Utilities.check_csv_files(directoryOpere)):
        opere = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                                   ";").csv(
            directoryOpere)
        autori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                                   ";").csv(
            directoryAutori)
        descrizioni = spark.read.option("header", "true").option("multiline",True).option("inferSchema", "true").option("delimiter",
                                                                                          ";").csv(
            directoryDescrizioni)
        immagini = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                          ";").csv(
            directoryImmagini)


        opere.show()
        autori.show()
        descrizioni.show()
        immagini.show()

        #TODO si potrebbe fare solo con opere senza autore o nuove (e tutti gli autori)
        #join opera con autori
        join_autori = opere.alias("opere") \
            .join(autori.alias("autori"), \
                  (func.col("opere.autore") == func.col("autori.nome")), \
                  "left"
                  ) \
            .select(func.col("opere.*"), func.col("autori.id").alias("autore_id"))

        #TODO si potrebbe fare solo con opere nuove (o senza descrizione)
        join_descrizione = opere.alias("opere") \
            .join(descrizioni.alias("descrizioni"), \
                  (func.col("opere.id") == func.col("descrizioni.id_opera")), \
                  "left"
                  )\
            .select(func.col("opere.*"), func.col("descrizioni.descrizione").alias("descrizione"))

        #TODO solo opere nuove o senza descrizione
        join_immagini = opere.alias("opere") \
            .join(immagini.alias("immagini"), \
                  (func.col("opere.id") == func.col("immagini.id_opera")), \
                  "left"
                  ) \
            .select(func.col("opere.id").alias("id"), func.col("immagini.input_file").alias("file"))\
            .groupBy("id").agg(func.collect_list("file").alias("file"))


        join = join_autori.alias("opere") \
            .join(join_descrizione.alias("descrizioni"), \
                  (func.col("opere.id") == func.col("descrizioni.id")), \
                  "left"
                  )\
            .select(func.col("opere.*"),func.col("descrizioni.descrizione").alias("descrizione"))\
            .join(join_immagini.alias("immagini"), \
              (func.col("opere.id") == func.col("immagini.id")), \
              "left"
              ) \
            .select(func.col("opere.*"),func.col("immagini.file").alias("file"))

        join.show()

    else:
        print("Attenzione, non ci sono opere")


def visita(spark):
    print("credo dataframe per visite")
    directoryOpere = 'curated/opere/lista/'
    directoryVisitatori = 'curated/visitatori/elenco/'
    directoryVisite = 'curated/visitatori/visite/'

    if (Utilities.check_csv_files(directoryOpere)):
        opere = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                         ";").csv(
            directoryOpere)
        visitatori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                          ";").csv(
            directoryVisitatori)
        visite = spark.read.option("header", "true").option("multiline", True).option("inferSchema",
                                                                                           "true").option("delimiter",
                                                                                                          ";").csv(
            directoryVisite)


        opere.show()
        visitatori.show()
        visite.show()

        #TODO si potrebbe partire solo dalle visite nuove
        join = visite.alias("visite") \
            .join(opere.alias("opere"), \
                  (func.col("visite.opera_id") == func.col("opere.id")), \
                  "left"
                  ) \
            .select(func.col("visite.*"), func.col("opere.id").alias("opera"))
        join = join.alias("visite") \
            .join(visitatori.alias("visitatori"),
                  (func.col("visite.visitatore_id") == func.col("visitatori.id")), \
                  "left"
                  ) \
            .select(func.col("visite.*"), func.col("visitatori.id").alias("visitatore"))

        join.where(join.visitatore.isNotNull() & join.opera.isNotNull()).show()

        #TODO le visite escluse potrebbero non essere considerate come fatte

    else:
        print("Attenzione, non ci sono opere")


def main():
    print("Da Curated a Application Zone")

    # Configurazione SparkSession
    spark = SparkSession.builder. \
        master("local"). \
        config("spark.driver.bindAddress", "localhost"). \
        config("spark.ui.port", "4050"). \
        appName("MinTemperatures"). \
        enableHiveSupport(). \
        getOrCreate()

    valore = input("Standardized -> Curated\n"
                   "Seleziona un'opzione:\n"
                   ""
                   "1) Categoria e visitatore\n"
                   "2) Opera, autore, descirzione e immagini\n"
                   "3) Visite, visitatore e opera\n"
                   "4) Tutti\n")

    if (valore == '1'):
        categorie_visitatori(spark)
    elif (valore == '2'):
        opera(spark)
    elif (valore == '3'):
        visita(spark)
    elif (valore == '4'):
        print("tutti")
        #TODO implementare tutti

    #categorie_visitatori(spark)
    #opera(spark)
    visita(spark)





if __name__ == "__main__":
    main()