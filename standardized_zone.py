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
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType, ArrayType
import os
import shutil
import sys
import Utilities


"""
Vengono lette tutte le categorie (da file.csv) nella cartella standardized/visitatori/categorie/

Dal dataframe vengono rimossi i duplicati.
Il dataframe è unito con quello contenente le categorie già salvate nella curated
Vengono mantenute le categorie inserite in una data più recente in caso di duplicati

Duplicati: categorie con stesso ID, viene mantenuta quella con data di creazione più recente
Accettati: deve esserci almeno id e nome_categoria
"""
def categoria_visitatori(spark):
    print("inizio a spostare le categorie da Standardized a Curated")
    fileDirectory = 'standardized/visitatori/categorie/'
    moveDirectory = 'standardized/visitatori/categorie/processed/'
    destinationDirectory = 'curated/visitatori/categorie/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        #rimuovo categorie che non hanno id e nome_categoria
        lista_categorie = lista_categorie.filter(func.col("id").isNotNull() & func.col("nome_categoria").isNotNull())
        #rimuovo duplicati
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Categorie lette (rimossi duplicati e righe non accettabili")
        lista_categorie_no_duplicates.show()

        """
        Questo approccio consente di passare ad UDF gli array di contenenti tutte le righe associate ad uno stesso id
        la funzione può elaborare come vuole queste informazioni e restituire una lista
        la lista viene inserita in una nuova colonna
        accedento df.colonna[i] si prendono i vari elmenti della lista e con withColumn li si inserisce sopra id, eta_mic ecc
        alla fine per ogni id si ottiene una riga frutto dell'elaborazione della funzione UDF
        """
        """group = lista_categorie.alias("df").select("id",func.struct(["df.*"]).alias("newcol")).groupBy("id").agg(func.collect_list("newcol").alias("newcol"))
        group.show(10, False)
        udfCheckDuplicatiCategoria = udf(Utilities.check_categorie_duplicate, ArrayType(StringType()))
        #udfCheckDuplicatiCategoria = udf(Utilities.check_categorie_duplicate, StructType())
        group2 = group.withColumn("risultato", udfCheckDuplicatiCategoria(func.col("id"),func.col("newcol")))
        group2.withColumn("colonna creata",group2.risultato[1]).show()
        group2.printSchema()
        group2.select(group2.risultato[0]).show()"""




        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Categorie presenti nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.unionByName(lista_categorie_salvate, allowMissingColumns=True)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti con le categorie nella curated")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono categorie già salvate nella curated")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuova categoria nella standardized")

"""
Vengono lette tutti i visitatori (da file.csv) nella cartella standardized/visitatori/elenco/

Dal dataframe vengono rimossi i duplicati.
Il dataframe è unito con quello contenente i visitatori già salvati nella curated
Vengono mantenute i visitatori inseriti in una data più recente in caso di duplicati

Duplicati: visitatori con stesso ID, viene mantenuto il più recente
Accettati: deve esserci almeno ID
"""
def visitatori(spark):
    print("inizio a spostare i visitatori da Standardized a Curated")
    fileDirectory = 'standardized/visitatori/elenco/'
    moveDirectory = 'standardized/visitatori/elenco/processed/'
    destinationDirectory = 'curated/visitatori/elenco/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        # rimuovo visitatori che non hanno id
        lista_categorie = lista_categorie.filter(func.col("id").isNotNull())
        # rimuovo duplicati
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Visitatori trovati nella standardized (no dupplicati e rimosse righe non accettate)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Visitatori trovati nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.unionByName(lista_categorie_salvate, allowMissingColumns=True)
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

"""
Vengono lette tutte le visite (da file.csv) nella cartella standardized/visitatori/visite/

Dal dataframe vengono rimossi i duplicati.
Il dataframe è unito con quello contenente le visite già salvate nella curated
Vengono mantenute le visite inseriti in una data più recente in caso di duplicati (non dovrebbe succedere)

Duplicati: visite con stesso ID
Accettati: deve esserci id, opera_id e visitatore_id
"""
def visite(spark):
    print("inizio a spostare le visite da Standardized a Curated")
    fileDirectory = 'standardized/visitatori/visite/'
    moveDirectory = 'standardized/visitatori/visite/processed/'
    destinationDirectory = 'curated/visitatori/visite/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        # rimuovo visite che non hanno id, opera_id e visitatore_id
        lista_categorie = lista_categorie.filter(func.col("id").isNotNull() & func.col("visitatore_id").isNotNull() & func.col("opera_id").isNotNull())
        # rimuovo duplicati
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Visite trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Visite trovate nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.unionByName(lista_categorie_salvate, allowMissingColumns=True)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono Visite già salvate")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuova Visita nella standardized")

"""
Vengono lette tutte le immagini (da file.csv) nella cartella standardized/opere/immagini/

Dal dataframe vengono rimossi i duplicati.
Il dataframe è unito con quello contenente le immagini già salvate nella curated
Vengono mantenute le immagini inseriti in una data più recente in caso di duplicati (non dovrebbe succedere)

Dupplicati: considerano duplicati due immagini con lo stesso source_file e id_opera
Accettati: devono avere almeno id_opera e source_file
"""
def immagini(spark):
    print("inizio a spostare le immagini da Standardized a Curated")
    fileDirectory = 'standardized/opere/immagini/'
    moveDirectory = 'standardized/opere/immagini/processed/'
    destinationDirectory = 'curated/opere/immagini/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        # rimuovo visite che non hanno id, opera_id e visitatore_id
        lista_categorie = lista_categorie.filter(
            func.col("id_opera").isNotNull() & func.col("source_file").isNotNull())
        # rimuovo duplicati
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["source_file","id_opera"])
        print("Immagini trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Immagini trovate nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.unionByName(lista_categorie_salvate, allowMissingColumns=True)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["source_file","id_opera"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono immagini già salvate")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuova immagini nella standardized")

"""
Vengono letti tutti gli autori (da file.csv) nella cartella standardized/opere/autori/

Dal dataframe vengono rimossi i duplicati.
Il dataframe è unito con quello contenente gli autori già salvati nella curated
Vengono mantenuti gli autori inseriti in una data più recente in caso di duplicati

Duplicati: Si considerano duplicati due autori con lo stesso ID
Accettati: un autore deve almeno avere un id
"""
def autori(spark):
    print("inizio a spostare gli autori da Standardized a Curated")
    fileDirectory = 'standardized/opere/autori/'
    moveDirectory = 'standardized/opere/autori/processed/'
    destinationDirectory = 'curated/opere/autori/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)

        # rimuovo autori che non hanno id
        lista_categorie = lista_categorie.filter(
            func.col("id").isNotNull())
        # rimuovo duplicati
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        print("Autori trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()
        lista_categorie_no_duplicates.printSchema()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Autori trovati nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.unionByName(lista_categorie_salvate, allowMissingColumns=True)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            print("Dataframe uniti")
            union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            print("non ci sono autori già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)
            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)

        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessun nuovo autore nella standardized")

"""
Vengono lette tutte le descrizioni (da file.csv) nella cartella standardized/opere/descrizioni/

Dal dataframe vengono rimossi i duplicati.
Il dataframe è unito con quello contenente le descrizioni già salvate nella curated
Vengono mantenute le descrizioni inseriti in una data più recente in caso di duplicati 

Duplicati: Si considerano duplicati due descrzioni con stesso contenuto e id_opera
Accettati: deve avere contenuto e id opera
"""
#TODO attualmente un opera ha una descrizione e il sistema di gestione dei duplicati mantiene la più recente
#TODO si può prevedere di mantenere più descrizioni per una stessa opera
def descrizioni(spark):
    print("inizio a spostare le descrizioni da Standardized a Curated")
    fileDirectory = 'standardized/opere/descrizioni/'
    moveDirectory = 'standardized/opere/descrizioni/processed/'
    destinationDirectory = 'curated/opere/descrizioni/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("multiline",True).option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        # rimuovo descrizioni che non hanno descrizione e id_opera
        lista_categorie = lista_categorie.filter(
            func.col("id_opera").isNotNull() & func.col("descrizione").isNotNull())
        # rimuovo duplicati
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id_opera","descrizione"])
        print("Descrizioni trovate nella standardized (no dupplicati)")
        lista_categorie_no_duplicates.show()
        lista_categorie_no_duplicates.printSchema()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("multiline",True).option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            print("Descrizioni trovate nella curated")
            lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.unionByName(lista_categorie_salvate, allowMissingColumns=True)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id_opera","descrizione"])
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

"""
Vengono lette tutte le opere (da file.csv) nella cartella standardized/opere/lista/

Dal dataframe vengono rimossi i duplicati.
Il dataframe è unito con quello contenente le opere già salvate nella curated
Vengono mantenute le opere inserite in una data più recente in caso di duplicati

Duplicati: Si considerano duplicati due opere con lo stesso ID
Accettati: opera deve avere id, titolo
"""
def opere(spark):
    #print("inizio a spostare le opere da Standardized a Curated")
    fileDirectory = 'standardized/opere/lista/'
    moveDirectory = 'standardized/opere/lista/processed/'
    destinationDirectory = 'curated/opere/lista/'

    if (Utilities.check_csv_files(fileDirectory)):
        lista_categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            fileDirectory)
        # rimuovo opere che non hanno id e titolo
        lista_categorie = lista_categorie.filter(
            func.col("id").isNotNull() & func.col("titolo").isNotNull())
        # rimuovo duplicati
        lista_categorie_no_duplicates = Utilities.drop_duplicates_row(lista_categorie, "data_creazione",["id"])
        #print("Opere trovate nella standardized (no dupplicati)")
        #lista_categorie_no_duplicates.show()

        os.makedirs(destinationDirectory, exist_ok=True)
        if (Utilities.check_csv_files(destinationDirectory)):
            lista_categorie_salvate=spark.read.option("header", "true").option("inferSchema", "true").option("delimiter", ";").csv(
            destinationDirectory)
            #print("Opere trovate nella curated")
            #lista_categorie_salvate.show()
            union = lista_categorie_no_duplicates.unionByName(lista_categorie_salvate, allowMissingColumns=True)
            union = Utilities.drop_duplicates_row(union, "data_creazione",["id"])
            #print("Dataframe uniti")
            #union.show()
            union.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)
            Utilities.remove_input_file(destinationDirectory, lista_categorie_salvate)


        else:
            #print("non ci sono opere già salvati")
            os.makedirs(destinationDirectory, exist_ok=True)

            if (lista_categorie_no_duplicates.count() > 0):
                lista_categorie_no_duplicates.write.mode("append").option("header", "true").option("delimiter", ";").csv(
                    destinationDirectory)


        Utilities.move_input_file(moveDirectory, fileDirectory, lista_categorie)
    else:
        print("Non c'è nessuna nuova opere nella standardized")
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
"""



def main():
    print("Da Standardized a Curated Zone")

    # Configurazione SparkSession
    spark = SparkSession.builder. \
        master("local"). \
        config("spark.driver.bindAddress", "localhost"). \
        config("spark.ui.port", "4050"). \
        appName("CMS"). \
        enableHiveSupport(). \
        getOrCreate()



    valore = input("Standardized -> Curated\n"
          "Seleziona un'opzione:\n"
          ""
           "1) Opere\n"
           "2) Descrizioni\n"
           "3) Autori\n"
           "4) Immagini\n"
           "5) Categorie\n"
           "6) Visitatori\n"
           "7) Visite\n"
           "0) Tutti\n")

    if (valore=='5'):
        categoria_visitatori(spark)
    elif (valore=='6'):
        visitatori(spark)
    elif (valore == '7'):
        visite(spark)
    elif (valore == '4'):
        immagini(spark)
    elif (valore == "3"):
        autori(spark)
    elif (valore == '2'):
        descrizioni(spark)
    elif (valore == '1'):
        opere(spark)
    elif (valore == '0'):
        categoria_visitatori(spark)
        visitatori(spark)
        visite(spark)
        immagini(spark)
        autori(spark)
        descrizioni(spark)
        opere(spark)

if __name__ == "__main__":
    main()









