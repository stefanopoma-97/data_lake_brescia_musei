import gc
from copy import deepcopy
import timeit
import tracemalloc
import raw_zone, standardized_zone, curated_zone

from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, input_file_name, substring_index, current_timestamp, to_timestamp, upper, lit, when, date_format
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import Utilities
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import time







SETUP_SPARK='''
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, input_file_name, substring_index, current_timestamp, to_timestamp, upper, lit, when, date_format
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import Utilities
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
'''

TEST_SPARK='''
conf = SparkConf().setMaster("local").setAppName("CMS")
sc = SparkContext(conf=conf)

# Configurazione SparkSession
spark = SparkSession.builder. \
    master("local"). \
    config("spark.driver.bindAddress", "localhost"). \
    config("spark.ui.port", "4050"). \
    appName("CMS"). \
    enableHiveSupport(). \
    getOrCreate()
sc.stop()
spark.stop()
'''

SETUP_TEST='''
'''

TEST='''
print("u")'''

SETUP_RAW_ZONE= '''
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, input_file_name, substring_index, current_timestamp, to_timestamp, upper, lit, when, date_format
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import Utilities
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import raw_zone 


conf = SparkConf().setMaster("local").setAppName("CMS")
sc = SparkContext(conf=conf)
sc.stop()

# Configurazione SparkSession
spark = SparkSession.builder. \
    master("local"). \
    config("spark.driver.bindAddress", "localhost"). \
    config("spark.ui.port", "4050"). \
    appName("CMS"). \
    enableHiveSupport(). \
    getOrCreate()
sc.stop()
'''

TEST_RAW_100='''
raw_zone.opere_sottocartelle(spark, sc)
'''

percorso_jar = r"C:\spark-3.3.0-bin-hadoop3\spark-3.3.0-bin-hadoop3\bin\neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar"

conf = SparkConf().setMaster("local").setAppName("cms_curated").set("spark.jars", percorso_jar)
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


#evito di spostare in processed
def raw_zone_function(spark, sc):
    raw_zone.opere_sottocartelle(spark, sc)

#evito di spostare in processed e scrivere nella curated, nella curated zone ci sono metà elementi dupplicati
def std_zone_function(spark, sc):
    standardized_zone.opere(spark)

#join per ottenere visite, opere e visitatore
def curated_zone_function_visite(spark, sc):
    curated_zone.get_visite(spark)

def curated_zone_function_visite_save_no_rel(spark, sc):
    curated_zone.get_visite_and_write_no_rel(spark)

#join per ottenere visite, opere e visitatore, salva poi su DB
#100 -> grafo da 202 elementi
#1000 -> grafo da 2012
#10000 -> grafo da 29999
def curated_zone_function_visite_save(spark, sc):
    curated_zone.get_visite_and_write(spark)

def setup_spark():
    conf = SparkConf().setMaster("local").setAppName("CMS")
    sc = SparkContext(conf=conf)

    # Configurazione SparkSession
    spark = SparkSession.builder. \
        master("local"). \
        config("spark.driver.bindAddress", "localhost"). \
        config("spark.ui.port", "4050"). \
        appName("CMS"). \
        enableHiveSupport(). \
        getOrCreate()
    sc.stop()
    spark.stop()

nome="TEST_CURATED_VISITE_SALVATE_NO_REL_1000"
numero_esecuzioni=20
dates=[]
for i in range(numero_esecuzioni):
    st = time.time()
    curated_zone_function_visite_save_no_rel(spark, sc)
    et = time.time()
    dates.append(et-st)
    print("ciclo: "+str(i))
print(dates)


print("FINITO ESECUZIONE")

out = open("Output/Test tempo/"+nome+".txt", "w")
out.write("tempi in secondi"+"\n")
for d in dates:
    out.write(str(d)+"\n")
out.close()

import stampa
stampa.stampa(nome, numero_esecuzioni+1)
