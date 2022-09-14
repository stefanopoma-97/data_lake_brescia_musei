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

        """
        out = join.groupBy("id", "nome").agg(
            func.to_json(
                func.collect_list(
                    func.struct(*[func.col("categoria_id").alias("categoria_id") for c in join.columns])
                )
            ).alias("data")
        )
        """

        out = join.groupBy("id","nome").agg(func.collect_list("categoria_id").alias("categorie_id"))
        out.show(truncate=False)
        out.printSchema()


    else:
        print("Attenzione, mancano i visitaotori e/o le categorie")


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

    categorie_visitatori(spark)



if __name__ == "__main__":
    main()