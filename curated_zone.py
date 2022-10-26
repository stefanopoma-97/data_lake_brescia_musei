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


"""
Restituisce un DataFrame contenente tutti i visitatori estratti dalla curated
Viene anche aggiunta una colonna contenente le categorie associata al visitatore (Array di ID)
"""


#TODO si potrebbe prendere solo i visitatori nuovi
#TODO poi separatamente vedere se c'è una categoria nuova e fare il join con tutti i visitatori
#TODO il passaggio da standardized a curate potrebbe inserire una colonna "processato" = False
#TODO il passaggio da curated ad application potrebbe impostare il valore a True a tutte le informazioni salvate sul grafo
def get_visitatori_e_categorie(spark):
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
        print("Categorie:")
        lista_categorie.show()
        print("Visitatori:")
        lista_visitatori.show()


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

def get_visitatori(spark):
    print("GET visitatori")
    directoryVisitatori = 'curated/visitatori/elenco/'

    if (Utilities.check_csv_files(directoryVisitatori)):
        lista_visitatori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                                   ";").csv(
            directoryVisitatori)
        print("Visitatori:")
        lista_visitatori.show()


        return lista_visitatori


    else:
        print("Attenzione, mancano i visitaotori e/o le categorie")
        return None


"""
Restituisce un dataframe contenente tutte le opere
alle opere è associato l'id dell'autore, la descrizione, l'elenco delle immagini (Array di ID)
"""
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

        if (Utilities.check_csv_files(directoryAutori)):
            autori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                                       ";").csv(
                directoryAutori)
        else:
            autori = Utilities.empty_autori(spark)

        if (Utilities.check_csv_files(directoryDescrizioni)):
            descrizioni = spark.read.option("header", "true").option("multiline",True).option("inferSchema", "true").option("delimiter",
                                                                                              ";").csv(
                directoryDescrizioni)
        else:
            descrizioni = Utilities.empty_descrizioni(spark)

        if (Utilities.check_csv_files(directoryImmagini)):
            immagini = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                              ";").csv(
                directoryImmagini)
        else:
            immagini = Utilities.empty_immagini(spark)

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
                  (func.lower(func.col("opere.autore")) == func.lower(func.col("autori.nome"))), \
                  "left"
                  ) \
            .select(func.col("opere.id").alias("id"), func.col("autori.id").alias("autore_id"))

        #print("join autori")
        #join_autori.show()

        #TODO si potrebbe fare solo con opere nuove (o senza descrizione)
        join_descrizione = opere.alias("opere") \
            .join(descrizioni.alias("descrizioni"), \
                  (func.col("opere.id") == func.col("descrizioni.id_opera")), \
                  "left"
                  )\
            .select(func.col("opere.id").alias("id"), func.col("descrizioni.descrizione").alias("descrizione"))\
            .groupBy("id").agg(func.collect_list("descrizione").alias("descrizione"))
        #print("join descrizione")
        #join_descrizione.show()

        #TODO solo opere nuove o senza descrizione
        join_immagini = opere.alias("opere") \
            .join(immagini.alias("immagini"), \
                  (func.col("opere.id") == func.col("immagini.id_opera")), \
                  "left"
                  ) \
            .select(func.col("opere.id").alias("id"), func.col("immagini.source_file").alias("file"))\
            .groupBy("id").agg(func.collect_list("file").alias("file"))

        #print("join immagini")
        #join_immagini.show()


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
        print("Dataframe:")
        join.show()
        return join

    else:
        print("Attenzione, non ci sono opere")
        return None

"""
Restituisce un datframe contenente le visite, a cui viene anche aggiunta una colonna per l'id del visitatore e l'id dell'opera.
Se gli id non esistono viene inserito null
"""
def get_visite(spark):
    #print("GET visite")
    directoryOpere = 'curated/opere/lista/'
    directoryVisitatori = 'curated/visitatori/elenco/'
    directoryVisite = 'curated/visitatori/visite/'

    if (Utilities.check_csv_files(directoryOpere) and Utilities.check_csv_files(directoryVisitatori) and Utilities.check_csv_files(directoryVisite)):
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
        """
        print("Opere:")
        opere.show()
        print("Visitatori:")
        visitatori.show()
        print("Visite:")
        visite.show()
        """

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

        join = join.where(join.visitatore.isNotNull() & join.opera.isNotNull())
        print("Dataframe")
        #join.show()
        return join


        #TODO le visite escluse potrebbero non essere considerate come fatte

    else:
        print("Attenzione, non ci sono visite e/o visitatori e/o opere")
        return None

def get_visite_and_write(spark):
    #print("GET visite")
    #graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_cms_brescia"))
    #graph.delete_all()
    directoryOpere = 'curated/opere/lista/'
    directoryVisitatori = 'curated/visitatori/elenco/'
    directoryVisite = 'curated/visitatori/visite/'

    if (Utilities.check_csv_files(directoryOpere) and Utilities.check_csv_files(directoryVisitatori) and Utilities.check_csv_files(directoryVisite)):
        print("leggo opere: inizio")
        opere = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                         ";").csv(
            directoryOpere)
        print("leggo opere: fine")
        print("leggo visitatori: inizio")
        visitatori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                          ";").csv(
            directoryVisitatori)
        print("leggo visitatori: fine")
        print("leggo visite: inizio")
        visite = spark.read.option("header", "true").option("multiline", True).option("inferSchema",
                                                                          "true").option("delimiter",
                                                                                                          ";").csv(
            directoryVisite)
        print("leggo visite: fine")



        #TODO si potrebbe partire solo dalle visite nuove
        print("join visite-opere: inizio")
        join = visite.alias("visite") \
            .join(opere.alias("opere"), \
                  (func.col("visite.opera_id") == func.col("opere.id")), \
                  "left"
                  ) \
            .select(func.col("visite.*"), func.col("opere.id").alias("opera"))
        print("join visite-opere: fine")
        print("join visite-opere-visitatori: inizio")
        join = join.alias("visite") \
            .join(visitatori.alias("visitatori"),
                  (func.col("visite.visitatore_id") == func.col("visitatori.id")), \
                  "left"
                  ) \
            .select(func.col("visite.*"), func.col("visitatori.id").alias("visitatore"))

        visite = join.where(join.visitatore.isNotNull() & join.opera.isNotNull())
        print("join visite-opere-visitatori: fine")

        # scrivo opere
        print("scrivo opere: inizio")
        opere.drop("immagini", "autore_id").write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Opera") \
            .option("node.keys", "id") \
            .save()
        print("scrivo opere: fine")
        #opere.show()

        print("scrivo visitatori: inizio")
        visitatori.write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Visitatore") \
            .option("node.keys", "id") \
            .save()
        print("scrivo visitatori: fine")

        print("scrivo visite: inizio")
        #scrivo visite e collegamenti con opera e visitatore
        visite.drop("opera", "visitatore").write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Visita") \
            .option("node.keys", "id") \
            .save()
        print("scrivo visite: fine")

        print("scrivo VISITA_OPERA: inizio")
        visita_r = visite.select("id", "visitatore_id", "opera_id")
        # visita_r.show()
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
        print("scrivo VISITA_OPERA: fine")
        print("scrivo VISITA_VISITATORE: inizio")
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
        print("scrivo VISITA_VISITATORE: fine")





    else:
        print("Attenzione, non ci sono visite e/o visitatori e/o opere")
        return None


def get_visite_and_write_no_rel(spark):
    #print("GET visite")
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_cms_brescia"))
    graph.delete_all()
    directoryOpere = 'curated/opere/lista/'
    directoryVisitatori = 'curated/visitatori/elenco/'
    directoryVisite = 'curated/visitatori/visite/'

    if (Utilities.check_csv_files(directoryOpere) and Utilities.check_csv_files(directoryVisitatori) and Utilities.check_csv_files(directoryVisite)):
        print("leggo opere: inizio")
        opere = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                         ";").csv(
            directoryOpere)
        print("leggo opere: fine")
        print("leggo visitatori: inizio")
        visitatori = spark.read.option("header", "true").option("inferSchema", "true").option("delimiter",
                                                                                          ";").csv(
            directoryVisitatori)
        print("leggo visitatori: fine")
        print("leggo visite: inizio")
        visite = spark.read.option("header", "true").option("multiline", True).option("inferSchema",
                                                                          "true").option("delimiter",
                                                                                                          ";").csv(
            directoryVisite)
        print("leggo visite: fine")



        #TODO si potrebbe partire solo dalle visite nuove
        print("join visite-opere: inizio")
        join = visite.alias("visite") \
            .join(opere.alias("opere"), \
                  (func.col("visite.opera_id") == func.col("opere.id")), \
                  "left"
                  ) \
            .select(func.col("visite.*"), func.col("opere.id").alias("opera"))
        print("join visite-opere: fine")
        print("join visite-opere-visitatori: inizio")
        join = join.alias("visite") \
            .join(visitatori.alias("visitatori"),
                  (func.col("visite.visitatore_id") == func.col("visitatori.id")), \
                  "left"
                  ) \
            .select(func.col("visite.*"), func.col("visitatori.id").alias("visitatore"))

        visite = join.where(join.visitatore.isNotNull() & join.opera.isNotNull())
        print("join visite-opere-visitatori: fine")

        # scrivo opere
        print("scrivo opere: inizio")
        opere.drop("immagini", "autore_id").write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Opera") \
            .option("node.keys", "id") \
            .save()
        print("scrivo opere: fine")
        #opere.show()

        print("scrivo visitatori: inizio")
        visitatori.write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Visitatore") \
            .option("node.keys", "id") \
            .save()
        print("scrivo visitatori: fine")

        print("scrivo visite: inizio")
        #scrivo visite e collegamenti con opera e visitatore
        visite.drop("opera", "visitatore").write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Visita") \
            .option("node.keys", "id") \
            .save()
        print("scrivo visite: fine")


    else:
        print("Attenzione, non ci sono visite e/o visitatori e/o opere")
        return None


"""
Restituisce un dataframe contenente le categorie
"""
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
        return None

"""
Restituisce un dataframe contente le immagini
"""
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
        print("Attenzione, non ci sono nuove immagini")
        return None

"""
Restituisce un dataframe contenente le immagini
"""
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
        return None

"""
Cancella tutti i nodi e le relazioni nel grafo
"""
def drop_graph():
    graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_cms_brescia"))
    graph.delete_all()
    print("Elementi del grafo eliminati")

"""
Analizza tutti i file contenente nella curated zone per creare nodi e relazioni in un fonte 1 Neo4j
Il grafo viene inizialmente eliminato e poi riscritto
"""
#TODO integrando i controlli sui dati già processati si può evitare di fare un drop del grafo
def write_neo4j(spark):
    print("Scrittura su DB di tutti i nodi e relazioni nella Curated Zone")

    graph = Graph("bolt://localhost:7687", auth=("neo4j", "neo4j_cms_brescia"))
    #graph.delete_all()

    #Creo nodi delle categorie
    categorie = get_categorie(spark)
    if (categorie != None):
        categorie.write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Categoria") \
            .option("node.keys", "id") \
            .save()
    else:
        print("Attenzione, nella curated zone mancano le categorie. Non è possibile scriverle nel grafo")

    #creo nodi visitatori e poi relazione Visitatore->Categoria
    visitatori = get_visitatori(spark)
    if (visitatori != None):
        visitatori.write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Visitatore") \
            .option("node.keys", "id") \
            .save()
    else:
        print("Attenzione, nella curated zone mancano i visitatori. Non è possibile scriverli nel grafo")

    if (categorie!= None and visitatori!=None):
        visitatori = get_visitatori_e_categorie(spark)
        df2 = visitatori.select(visitatori.id, explode(visitatori.categorie_id).alias("categorie_id"))
        df2.printSchema()
        df2.show()

        # necessario perchè non c'è una vincolo 1:1
        df2.write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("query", "MATCH (v:Visitatore {id:event.id})-[r:CATEGORIA]->() DELETE r") \
            .save()

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
    else:
        print("Attenzione, nella curated zone mancano le categorie e/o i visitatori. Non è possibile scrivere la relazione nel grafo")

    #Creo nodo immagine
    immagini = get_immagini(spark)
    if (immagini!=None):
        immagini.write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Immagine") \
            .option("node.keys", "source_file") \
            .save()
    else:
        print("Attenzione, nella curated zone mancano le immagini. Non è possibile scriverle nel grafo")

    #creo nodo autore
    autori = get_autori(spark)
    if (autori!=None):
        autori.write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Autore") \
            .option("node.keys", "id") \
            .save()
    else:
        print("Attenzione, nella curated zone mancano gli autori. Non è possibile scriverli nel grafo")

    #creo opere

    opere = get_opere(spark)
    if (opere!=None):
        opere.drop("immagini","autore_id").write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Opera") \
            .option("node.keys", "id") \
            .save()
        opere.show()

        # opera->autore

        #necessario perchè non c'è una vincolo 1:1
        opere.filter(opere.autore_id.isNotNull()).write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("query", "MATCH (n:Opera {id:event.id})-[r:CREATA]->() DELETE r")\
            .save()

        #necessario togliere le relazioni null
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
            .option("relationship", "MULTIMEDIA") \
            .option("relationship.save.strategy", "keys") \
            .option("relationship.source.labels", ":Opera") \
            .option("relationship.source.save.mode", "overwrite") \
            .option("relationship.source.node.keys", "id") \
            .option("relationship.target.labels", ":Immagine") \
            .option("relationship.target.node.keys", "immagini:source_file") \
            .option("relationship.target.save.mode", "overwrite") \
            .save()
    else:
        print("Attenzione, nella curated zone mancano le opere. Non è possibile scriverle nel grafo")


    #creo visite
    visite = get_visite(spark)
    if (visite!=None):
        print("Visite prese")
        #visite.show()
        visite.drop("opera", "visitatore").write \
            .mode("overwrite") \
            .format("org.neo4j.spark.DataSource") \
            .option("url", "bolt://localhost:7687") \
            .option("labels", ":Visita") \
            .option("node.keys", "id") \
            .save()
        visita_r = visite.select("id","visitatore_id","opera_id")
        #visita_r.show()
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
    else:
        print("Mancano dei dati: visite, visitatori, opere")
        print("per inserire una visita devono essere presenti tutte e 3 le informazioni nella curated zone")


def main():
    print("Da Curated a Application Zone")
    percorso_jar=r"C:\spark-3.3.0-bin-hadoop3\spark-3.3.0-bin-hadoop3\bin\neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar"

    conf = SparkConf().setMaster("local").setAppName("cms_curated").set("spark.jars",percorso_jar)
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
                   "5) Drop DB\n"
                   "6) Visite, visitatore e opera e SALVA\n")

    if (valore == '1'):
        get_visitatori_e_categorie(spark)
    elif (valore == '2'):
        get_opere(spark)
    elif (valore == '3'):
        get_visite(spark)
    elif (valore == '4'):
        print("tutti")
        write_neo4j(spark)
    elif (valore == '5'):
        drop_graph()
    elif (valore == '6'):
        get_visite_and_write(spark)





if __name__ == "__main__":
    main()