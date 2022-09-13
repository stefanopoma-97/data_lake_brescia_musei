"""
Il seguente script serve a gestire l'evoluzione dei dati dalla Raw Zone alla Standardized Zone
"""
#import
import glob
import Utilities
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, input_file_name, substring_index, current_timestamp, to_timestamp
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType
import os
import shutil
import sys


"""
Vengono lette tutte le opere (da file.csv) nella cartella raw/opere/lista/
Struttura: 1;34455;Titolo 1;Tipologia 1;1900;Provenienza;autore1 cognome;1663053975
Viene aggiunto il secolo, gli autori sono messi con la prima lettera maiuscola, viene derivata la data dal timestamp
Viene inserito l'header e il nuovo dataframe viene salvato nella standardized zone.

I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano analizzati due volte
"""
def opere_lista(spark):
    fileDirectory = 'raw/opere/lista/'
    moveDirectory = 'raw/opere/lista/processed/'
    destinationDirectory = 'standardized/opere/lista/'

    #schema del csv
    schema = StructType([ \
        StructField("id", StringType(), True), \
        StructField("tagid", IntegerType(), True), \
        StructField("titolo", StringType(), True), \
        StructField("tipologia", StringType(), True), \
        StructField("anno", IntegerType(), True), \
        StructField("provenienza", StringType(), True), \
        StructField("autore", StringType(), True),\
        StructField("timestamp", IntegerType(), True)])

    #legge tutti i file nella directory
    df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)
    df.printSchema()

    #udf per estrarre il secolo
    udfFunction_GetCentury = udf(Utilities.centuryFromYear)

    #modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
    new = df.withColumn("data_creazione", to_timestamp(from_unixtime("timestamp")))\
            .withColumn("autore", initcap("autore"))\
            .withColumn("secolo", udfFunction_GetCentury(df.anno))
    new.show()
    new.printSchema()


    #salvataggio del DataFrame (solo se contiene informazioni)
    os.makedirs(destinationDirectory, exist_ok=True)
    if (new.count()>0):
        new.drop("timestamp").write.mode("append").option("header", "true").option("delimiter",";").csv(destinationDirectory)

    #i file letti vengono spostati nella cartella processed
    os.makedirs(moveDirectory, exist_ok=True)
    data = df.withColumn("input_file", input_file_name())
    lista = data.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        shutil.move(fileDirectory + fname, moveDirectory + fname)

"""
vengono lette tutti i file contenenti la descrizione di un'opera.
Viene creato un Datafram con id_opera, titolo_opera, descrizione

"""
def opere_descrizioni(spark, sc):
    fileDirectory = 'raw/opere/descrizioni/'
    moveDirectory = 'raw/opere/descrizioni/processed/'
    destinationDirectory = 'standardized/opere/descrizioni/'

    #schema del csv
    schema = StructType([ \
        StructField("input_file", StringType(), True),
        StructField("descrizione", StringType(), True)])

    #legge tutti i file nella directory
    rdd = sc.wholeTextFiles(fileDirectory)
    df = spark.createDataFrame(rdd, schema)
    df.show()


    udfGetID= udf(Utilities.getIDFromFile)
    udfModificationDate = udf(Utilities.modificationDate)
    udfGetTitolo = udf(Utilities.getTitoloFromFile)
    udfFilePath = udf(Utilities.filePath)
    df = df.withColumn("id_opera", udfGetID(func.substring_index(func.col("input_file"),"/",-1)))\
        .withColumn("titolo_opera", initcap(udfGetTitolo(func.substring_index(func.col("input_file"),"/",-1)))) \
        .withColumn("data_creazione",current_timestamp()) \
        .withColumn("input_file", udfFilePath(func.col("input_file")))

    df = df.withColumn("data_modifica", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))
    df.printSchema()
    df.show()



    # salvataggio del DataFrame (solo se contiene informazioni)
    os.makedirs(destinationDirectory, exist_ok=True)
    if (df.count() > 0):
        df.select("descrizione","id_opera","titolo_opera","data_creazione").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)

    os.makedirs(moveDirectory, exist_ok=True)
    lista = df.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        #shutil.move(fileDirectory + fname, moveDirectory + fname)

"""
vengono lette tutti i file contenenti gli autori (ID, Nome, Anno).
Viene creato un Dataframe e salvato nella standardized zone
"""
def opere_autori(spark, sc):
    fileDirectory = 'raw/opere/autori/'
    moveDirectory = 'raw/opere/autori/processed/'
    destinationDirectory = 'standardized/opere/autori/'

    # schema del csv
    schema = StructType([ \
        StructField("id", StringType(), True), \
        StructField("nome", StringType(), True), \
        StructField("anno", IntegerType(), True)])

    # legge tutti i file nella directory
    df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)
    df.printSchema()


    # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
    new = df.withColumn("nome", initcap("nome")).withColumn("data_creazione",current_timestamp())
    new.show()

    # salvataggio del DataFrame (solo se contiene informazioni)
    os.makedirs(destinationDirectory, exist_ok=True)
    if (new.count() > 0):
        new.write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)

    # i file letti vengono spostati nella cartella processed
    os.makedirs(moveDirectory, exist_ok=True)
    data = df.withColumn("input_file", input_file_name())
    lista = data.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        shutil.move(fileDirectory + fname, moveDirectory + fname)


def opere_immagini(spark, sc):

    fileDirectory = 'raw/opere/immagini/'
    moveDirectory = 'raw/opere/immagini/processed/'
    destinationDirectory = 'standardized/opere/immagini/'

    df = spark.read.text(fileDirectory)
    df.printSchema()
    df.show()

    udfGetID = udf(Utilities.getIDFromFile)
    udfGetTitolo = udf(Utilities.getTitoloFromFile)
    udfRecreateSpace = udf(Utilities.recreateSPace)
    df = df.withColumn("input_file", udfRecreateSpace(input_file_name()))\
        .withColumn("id_opera", udfGetID(func.substring_index(func.col("input_file"), "/", -1))) \
        .withColumn("titolo_opera", initcap(udfGetTitolo(func.substring_index(func.col("input_file"), "/", -1)))) \
        .withColumn("data_creazione", current_timestamp())

    df.show(20, False)

    # salvataggio del DataFrame (solo se contiene informazioni)
    os.makedirs(destinationDirectory, exist_ok=True)
    if (df.count() > 0):
        df.select("input_file","id_opera","titolo_opera","data_creazione").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)
    """
    os.makedirs(moveDirectory, exist_ok=True)
    lista = df.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        shutil.move(fileDirectory + fname, moveDirectory + fname)
    """


def main():
    print("Da Raw Zone a Standardized Zone")

    conf = SparkConf().setMaster("local").setAppName("CMS")
    sc = SparkContext(conf=conf)

    # Configurazione SparkSession
    spark = SparkSession.builder. \
        master("local"). \
        config("spark.driver.bindAddress", "localhost"). \
        config("spark.ui.port", "4050"). \
        appName("MinTemperatures"). \
        enableHiveSupport(). \
        getOrCreate()

    #opere_lista(spark)
    opere_descrizioni(spark, sc)
    #opere_autori(spark, sc)
    #opere_immagini(spark, sc)

if __name__ == "__main__":
    main()









