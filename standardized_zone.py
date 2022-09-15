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
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, desc, input_file_name
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import shutil
import sys
import Utilities



def categoria_visitatori(spark):
    print("inizio a spostare le categorie da Standardized a Curated")
    fileDirectory = 'standardized/visitatori/categorie/'
    moveDirectory = 'standardized/visitatori/categorie/processed/'
    destinationDirectory = 'curated/visitatori/categorie/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.union(lista_categorie_salvate)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono categorie già salvate")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuova categoria nella standardized")

def visitatori(spark):
    print("inizio a spostare i visitatori da Standardized a Curated")
    fileDirectory = 'standardized/visitatori/elenco/'
    moveDirectory = 'standardized/visitatori/elenco/processed/'
    destinationDirectory = 'curated/visitatori/elenco/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Visitatori trovati nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Visitatori trovati nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.union(lista_categorie_salvate)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono visitatori già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuovo visitatore nella standardized")

def visite(spark):
    print("inizio a spostare le visite da Standardized a Curated")
    fileDirectory = 'standardized/visitatori/visite/'
    moveDirectory = 'standardized/visitatori/visite/processed/'
    destinationDirectory = 'curated/visitatori/visite/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Visite trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Visitatori trovati nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.union(lista_categorie_salvate)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono visitatori già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuovo visitatore nella standardized")

def immagini(spark):
    print("inizio a spostare le immagini da Standardized a Curated")
    fileDirectory = 'standardized/opere/immagini/'
    moveDirectory = 'standardized/opere/immagini/processed/'
    destinationDirectory = 'curated/opere/immagini/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["input_file","id_opera"])
        print("Immagini trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Visitatori trovati nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.union(lista_categorie_salvate)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["input_file","id_opera"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono visitatori già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuovo visitatore nella standardized")

def autori(spark):
    print("inizio a spostare gli autori da Standardized a Curated")
    fileDirectory = 'standardized/opere/autori/'
    moveDirectory = 'standardized/opere/autori/processed/'
    destinationDirectory = 'curated/opere/autori/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Autori trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Autori trovati nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.union(lista_categorie_salvate)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono visitatori già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuovo visitatore nella standardized")

def descrizioni(spark):
    print("inizio a spostare le descrizioni da Standardized a Curated")
    fileDirectory = 'standardized/opere/descrizioni/'
    moveDirectory = 'standardized/opere/descrizioni/processed/'
    destinationDirectory = 'curated/opere/descrizioni/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("multiline",True).option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id_opera", "descrizione"])
        print("Descrizioni trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("multiline",True).option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Autori trovati nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.union(lista_categorie_salvate)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["descrizione", "id_opera"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono visitatori già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuovo visitatore nella standardized")

def opere(spark):
    print("inizio a spostare le opere da Standardized a Curated")
    fileDirectory = 'standardized/opere/lista/'
    moveDirectory = 'standardized/opere/lista/processed/'
    destinationDirectory = 'curated/opere/lista/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Opere trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Opere trovate nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.union(lista_categorie_salvate)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono opere già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuovo visitatore nella standardized")
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
    print("Da Standardized a Curated Zone")

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
          "1) Categoria visitatore\n"
          "2) Visitatori\n"
          "3) Visite\n"
          "4) Immagini\n"
          "5) Autori\n"
          "6) Descrizioni\n"
          "7) Opere\n"
          "0) Tutti\n")

    if (valore=='1'):
        categoria_visitatori(spark)
    elif (valore=='2'):
        visitatori(spark)
    elif (valore == '3'):
        visite(spark)
    elif (valore == '4'):
        immagini(spark)
    elif (valore == "5"):
        autori(spark)
    elif (valore == '6'):
        descrizioni(spark)
    elif (valore == '7'):
        opere(spark)
    elif (valore == '0'):
        print()
        #TODO implementare tutti



if __name__ == "__main__":
    main()









