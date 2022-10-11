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
emptyRDD = spark.sparkContext.emptyRDD()
schema = StructType([
  StructField('id', StringType(), True),
  StructField('nome', StringType(), True),
  StructField('anno_nascita', IntegerType(), True),
    StructField('source_file', StringType(), True),
    StructField('data_creazione', TimestampType(), True),
    StructField('fonte', StringType(), True)
  ])
df = spark.createDataFrame(emptyRDD,schema)
df.show()
df.printSchema()