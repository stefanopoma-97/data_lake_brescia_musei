#import
import glob

import Utilities
from py2neo import Node,Relationship,Graph,Path,Subgraph
from py2neo import NodeMatcher,RelationshipMatcher
from py2neo import Graph, Node, Relationship
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, desc, input_file_name, explode
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import shutil
import sys
import Utilities



def get_visitatori(spark):
    print("GET visitatori e array categorie")
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
            .select(func.col("visitatori.*"), \
                    func.col("categorie.id").alias("categoria_id")
                    ) \
            .groupBy("id","nome","cognome","sesso","eta","data_creazione").agg(func.collect_list("categoria_id").alias("categorie_id"))

        print("join visitatori e categorie")
        join.show()

        return join


    else:
        print("Attenzione, mancano i visitaotori e/o le categorie")

def get_opere(spark):
    print("GET opere")
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

        print("opere")
        opere.show()

        print("autori")
        autori.show()

        print("descrizioni")
        descrizioni.show()

        print("immagini")
        immagini.show()

        #TODO si potrebbe fare solo con opere senza autore o nuove (e tutti gli autori)
        #join opera con autori
        join_autori = opere.alias("opere") \
            .join(autori.alias("autori"), \
                  (func.col("opere.autore") == func.col("autori.nome")), \
                  "left"
                  ) \
            .select(func.col("opere.id").alias("id"), func.col("autori.id").alias("autore_id"))

        print("join autori")
        join_autori.show()

        #TODO si potrebbe fare solo con opere nuove (o senza descrizione)
        join_descrizione = opere.alias("opere") \
            .join(descrizioni.alias("descrizioni"), \
                  (func.col("opere.id") == func.col("descrizioni.id_opera")), \
                  "left"
                  )\
            .select(func.col("opere.id").alias("id"), func.col("descrizioni.descrizione").alias("descrizione"))

        print("join descrizione")
        join_descrizione.show()

        #TODO solo opere nuove o senza descrizione
        join_immagini = opere.alias("opere") \
            .join(immagini.alias("immagini"), \
                  (func.col("opere.id") == func.col("immagini.id_opera")), \
                  "left"
                  ) \
            .select(func.col("opere.id").alias("id"), func.col("immagini.input_file").alias("file"))\
            .groupBy("id").agg(func.collect_list("file").alias("file"))

        print("join immagini")
        join_immagini.show()


        join = opere.alias("opere") \
            .join(join_descrizione.alias("descrizioni"), \
                  (func.col("opere.id") == func.col("descrizioni.id")), \
                  "left"
                  ) \
            .join(join_immagini.alias("immagini"), \
                  (func.col("opere.id") == func.col("immagini.id")), \
                  "left"
                  ) \
            .join(join_autori.alias("autori"), \
                  (func.col("opere.id") == func.col("autori.id")), \
                  "left"
                  ) \
            .select(func.col("opere.*"),
                    func.col("descrizioni.descrizione").alias("descrizione"),
                    func.col("immagini.file").alias("immagini"),
                    func.col("autori.autore_id").alias("autore_id")
                    )

        join.show()
        return join

    else:
        print("Attenzione, non ci sono opere")


def get_visite(spark):
    print("GET visite")
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

        return join.where(join.visitatore.isNotNull() & join.opera.isNotNull())


        #TODO le visite escluse potrebbero non essere considerate come fatte

    else:
        print("Attenzione, non ci sono opere")

def get_categorie(spark):
    print("GET Categorie")
    directoryCategorie = 'curated/visitatori/categorie/'


    if (Utilities.check_csv_files(directoryCategorie)):
        categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                         ";").csv(
            directoryCategorie)

        categorie.show()

        # TODO si potrebbe estrarre solo quelle nuove
        return categorie

    else:
        print("Attenzione, non ci sono nuove categorie")

def get_immagini(spark):
    print("GET Immagini")
    directoryCategorie = 'curated/opere/immagini/'


    if (Utilities.check_csv_files(directoryCategorie)):
        categorie = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                         ";").csv(
            directoryCategorie)

        categorie.show()

        # TODO si potrebbe estrarre solo quelle nuove
        return categorie

    else:
        print("Attenzione, non ci sono nuove categorie")

def get_autori(spark):
    print("GET Autori")
    directoryAutori = 'curated/opere/autori/'


    if (Utilities.check_csv_files(directoryAutori)):
        autori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                         ";").csv(
            directoryAutori)

        autori.show()

        # TODO si potrebbe estrarre solo quelle nuove
        return autori

    else:
        print("Attenzione, non ci sono nuovi autori")

def drop_graph():
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_cms_brescia"))
    graph.delete_all()

def write_neo4j(spark):
    print("Scrittura su DB")

    graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_cms_brescia"))
    graph.delete_all()

    #Creo nodi delle categorie
    categorie = get_categorie(spark)
    categorie.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("labels", ":Categoria") \
        .option("node.keys", "id") \
        .save()

    #creo nodi visitatori e poi relazione Visitatore->Categoria
    visitatori = get_visitatori(spark)
    visitatori.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("labels", ":Visitatore") \
        .option("node.keys", "id") \
        .save()

    df2 = visitatori.select(visitatori.id, explode(visitatori.categorie_id).alias("categorie_id"))
    df2.printSchema()
    df2.show()

    df2.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("relationship", "CATEGORIA") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.properties", "categorie_id:categoria_id, id:visitatore_id")\
        .option("relationship.source.labels", ":Visitatore") \
        .option("relationship.source.save.mode", "overwrite") \
        .option("relationship.source.node.keys", "id") \
        .option("relationship.target.labels", ":Categoria") \
        .option("relationship.target.node.keys", "categorie_id:id") \
        .option("relationship.target.save.mode", "overwrite") \
        .save()

    #Creo nodo immagine
    immagini = get_immagini(spark)
    immagini.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("labels", ":Immagine") \
        .option("node.keys", "input_file") \
        .save()

    #creo nodo autore
    autori = get_autori(spark)
    autori.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("labels", ":Autore") \
        .option("node.keys", "id") \
        .save()

    #creo opere
    opere = get_opere(spark)
    opere.drop("immagini","autore_id").write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("labels", ":Opera") \
        .option("node.keys", "id") \
        .save()
    opere.show()

    # opera->autore
    #necessario toglere le relazioni null
    opere.filter(opere.autore_id.isNotNull()).write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("relationship", "CREATA") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Opera") \
        .option("relationship.source.save.mode", "overwrite") \
        .option("relationship.source.node.keys", "id") \
        .option("relationship.target.labels", ":Autore") \
        .option("relationship.target.node.keys", "autore_id:id") \
        .option("relationship.target.save.mode", "overwrite") \
        .save()

    df2 = opere.select(opere.id, explode(opere.immagini).alias("immagini"))
    df2.printSchema()
    df2.show()

    #opera->immagini
    df2.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("relationship", "IMMAGINI") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Opera") \
        .option("relationship.source.save.mode", "overwrite") \
        .option("relationship.source.node.keys", "id") \
        .option("relationship.target.labels", ":Immagine") \
        .option("relationship.target.node.keys", "immagini:input_file") \
        .option("relationship.target.save.mode", "overwrite") \
        .save()


    #creo visite
    visite = get_visite(spark)
    print("Visite prese")
    visite.show()
    visite.drop("opera", "visitatore").write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("labels", ":Visita") \
        .option("node.keys", "id") \
        .save()
    visita_r = visite.select("id","visitatore_id","opera_id")
    visita_r.show()
    visita_r.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("relationship", "VISITA_OPERA") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Visita") \
        .option("relationship.source.save.mode", "overwrite") \
        .option("relationship.source.node.keys", "id") \
        .option("relationship.target.labels", ":Opera") \
        .option("relationship.target.node.keys", "opera_id:id") \
        .option("relationship.target.save.mode", "overwrite") \
        .save()
    visita_r.write \
        .mode("overwrite") \
        .format("org.neo4j.spark.DataSource") \
        .option("url", "bolt://localhost:7687") \
        .option("relationship", "VISITA_VISITATORE") \
        .option("relationship.save.strategy", "keys") \
        .option("relationship.source.labels", ":Visita") \
        .option("relationship.source.save.mode", "overwrite") \
        .option("relationship.source.node.keys", "id") \
        .option("relationship.target.labels", ":Visitatore") \
        .option("relationship.target.node.keys", "visitatore_id:id") \
        .option("relationship.target.save.mode", "overwrite") \
        .save()


def main():
    print("Da Curated a Application Zone")

    conf = SparkConf().setMaster("local").setAppName("cms_curated").set("spark.jars",
                                                                            r"C:\spark-3.3.0-bin-hadoop3\spark-3.3.0-bin-hadoop3\bin\neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar")
    sc = SparkContext(conf=conf)
    # Configurazione SparkSession
    spark = SparkSession.builder. \
        master("local") \
        .config("neo4j.url", "bolt://localhost:7687") \
        .config("neo4j.authentication.type", "basic") \
        .config("neo4j.authentication.basic.username", "neo4j") \
        .config("neo4j.authentication.basic.password", "neo4j_cms_brescia") \
        .config("spark.driver.bindAddress", "localhost"). \
        config("spark.ui.port", "4050"). \
        appName("cms_curated"). \
        enableHiveSupport(). \
        getOrCreate()

    valore = input("Standardized -> Curated\n"
                   "Seleziona un'opzione:\n"
                   ""
                   "1) Categoria e visitatore\n"
                   "2) Opera, autore, descirzione e immagini\n"
                   "3) Visite, visitatore e opera\n"
                   "4) Tutti\n"
                   "5) Drop DB\n")

    if (valore == '1'):
        get_visitatori(spark)
    elif (valore == '2'):
        get_opere(spark)
    elif (valore == '3'):
        get_visite(spark)
    elif (valore == '4'):
        print("tutti")
        write_neo4j(spark)
        #get_categorie(spark)
        #get_visitatori(spark)
        #get_immagini(spark)
        #get_autori(spark)
        #get_opere(spark)
        #get_visite(spark)
        #TODO implementare tutti
    elif (valore == '5'):
        drop_graph()

    #categorie_visitatori(spark)
    #opera(spark)
    #visita(spark)







if __name__ == "__main__":
    main()