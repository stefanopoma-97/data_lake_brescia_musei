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
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, input_file_name, substring_index, current_timestamp, to_timestamp, upper, lit, when
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
Viene inoltre ricavata la data di creazione dal timestamp

I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano analizzati due volte
"""
def opere_lista(spark):
    print("inizio a spostare le opere da Raw a Standardized")
    fileDirectory = 'raw/opere/lista/'
    moveDirectory = 'raw/opere/lista/processed/'
    destinationDirectory = 'standardized/opere/lista/'


    if (Utilities.check_csv_files(fileDirectory)):
        # schema del csv
        schema = StructType([ \
            StructField("id", StringType(), True), \
            StructField("tagid", IntegerType(), True), \
            StructField("titolo", StringType(), True), \
            StructField("tipologia", StringType(), True), \
            StructField("anno", IntegerType(), True), \
            StructField("provenienza", StringType(), True), \
            StructField("autore", StringType(), True), \
            StructField("timestamp", IntegerType(), True)])

        #legge tutti i file nella directory
        df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)

        #udf per estrarre il secolo
        udfFunction_GetCentury = udf(Utilities.centuryFromYear)

        #modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        new = df.withColumn("data_creazione", to_timestamp(from_unixtime("timestamp")))\
                .withColumn("autore", initcap("autore"))\
                .withColumn("secolo", udfFunction_GetCentury(df.anno))
        print("Opere lette")
        new.show()
        #new.printSchema()


        #salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (new.count()>0):
            new.drop("timestamp").write.mode("append").option("header", "true").option("delimiter",";").csv(destinationDirectory)

        #i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)

    else:
        print("Non ci sono opere nella Raw Zone")
"""
la funzione serve ad identificare le sottocartelle (fonti) di raw/opere/lista ed eseguire la funzione
opere_lista_new() su ognuna delle sottocartelle trovate
"""
def opere_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/opere/lista")
    fileDirectory = 'raw/opere/lista/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        opere_lista_new(spark, sc, c)


"""
Vengono lette tutte le opere (da file.csv) nella cartella raw/opere/lista/sottocartella
Struttura finale: id;tagid;titolo;tipologia;anno;secolo;provenienza;autore;data_creazione;source_file;fonte

I file csv vengono confrontati con la struttura sopra riportata, colonne con un nome simile a quelle dello schema vengono rinominate.
Se alcune colonne non esistono nel file vengono create e i valori vengono impostati a null

Dopo aver applicato lo schema vengono eseguite una serie di operazioni (controllando prima che il campo da processare non sia null):
Viene aggiunto il secolo, gli autori sono messi con la prima lettera maiuscola, viene derivata la data dal timestamp
Viene inserito l'header e il nuovo dataframe viene salvato nella standardized zone.
Viene inoltre ricavata la data di creazione dal timestamp.


I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano analizzati due volte
"""
def opere_lista_new(spark, sc, fileDirectory):
    print("inizio a spostare le opere da Raw a Standardized: "+fileDirectory)
    #fileDirectory = 'raw/opere/lista/'
    moveDirectory = fileDirectory + "processed/"
    destinationDirectory = 'standardized/opere/lista/'


    if (Utilities.check_csv_files(fileDirectory)):

        #legge tutti i file nella directory
        df = spark.read\
            .option("mergeSchema", "true")\
            .option("delimiter", ";")\
            .option("inferSchema", "false") \
            .option("header", "true") \
            .csv(fileDirectory)


        #le colonne vengono messe in minuscolo e senza spazi, / o _
        for column in df.columns:
            new_column = column.replace(' ', '').replace('/', '').replace('_', '')
            df = df.withColumnRenamed(column, new_column.lower())



        #id;tagid;titolo;tipologia;anno;secolo;provenienza;autore;data_creazione;nome_file;fonte

        #Cambio ID
        possibili_id = []
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "id")
        if 'id' not in df.columns:
            df = df.withColumn('id', lit(None).cast("string"))

        # Cambio Tag Id
        possibili_id = ["tagid"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "tag_id")
        if 'tag_id' not in df.columns:
            df = df.withColumn('tag_id', lit(None).cast("string"))

        # Cambio titolo
        possibili_id = ["nome"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "titolo")
        if 'titolo' not in df.columns:
            df = df.withColumn('titolo', lit(None).cast("string"))

        # Cambio tipologia
        possibili_id = ["tipo"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "tipologia")
        if 'tipologia' not in df.columns:
            df = df.withColumn('tipologia', lit(None).cast("string"))

        # Cambio anno
        possibili_id = ["data"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "anno")
        if 'anno' not in df.columns:
            df = df.withColumn('anno', lit(None).cast("int"))

        # Cambio provenienza
        possibili_id = ["luogo"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "provenienza")
        if 'provenienza' not in df.columns:
            df = df.withColumn('provenienza', lit(None).cast("string"))

        # Cambio autore
        possibili_id = ["creatore", "artista"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "autore")
        if 'autore' not in df.columns:
            df = df.withColumn('autore', lit(None).cast("string"))

        # Cambio timestamp
        possibili_id = ["time"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "timestamp")
        if 'timestamp' not in df.columns:
            df = df.withColumn('timestamp', lit(None).cast("int"))

        #cast e ordinamento colonne
        df = df.alias("df").select(
            func.col("id"),
            func.col("tag_id"),
            func.col("titolo"),
            func.col("tipologia"),
            func.col("anno").cast("int"),
            func.col("provenienza"),
            func.col("autore"),
            func.col("timestamp").cast("int")
        )



        udfFunction_GetCentury = udf(Utilities.centuryFromYear)
        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfSourceFile = udf(Utilities.filePathInProcessed)
        udfFonte = udf(Utilities.filePathFonte)



        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        df = df.withColumn("input_file", udfFilePath(input_file_name())) \
                .withColumn("source_file", udfSourceFile(input_file_name()))\
                .withColumn("data_creazione",
                           when(func.col("timestamp").isNull(), to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))
                           .otherwise(to_timestamp(from_unixtime("timestamp")))
                           ) \
                .withColumn("fonte", udfFonte(input_file_name())) \
                .withColumn("autore",
                            when(func.col("autore").isNotNull(),initcap("autore"))
                            ) \
                .withColumn("secolo",
                            when(func.col("anno").isNotNull(),udfFunction_GetCentury(df.anno)))

        df.printSchema()
        df.show(10, False)

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (df.count() > 0):
            df.drop("timestamp","input_file").write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)


    else:
        print("Non ci sono opere in "+fileDirectory)

"""

Vengono lette tutte le descrizioni (da file.txt) nella cartella raw/opere/descrizioni/
Struttura: testo qualsiasi
Il nome del file .txt deve essere del tipo "nome-ID OPERA.txt"

Viene creato un dataframe contenente id_opera, titolo_opera, descrizione, data_creazione

I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano analizzati due volte

"""
def opere_descrizioni(spark, sc):
    print("inizio a spostare le descrizioni da Raw a Standardized")
    fileDirectory = 'raw/opere/descrizioni/'
    moveDirectory = 'raw/opere/descrizioni/processed/'
    destinationDirectory = 'standardized/opere/descrizioni/'

    if (Utilities.check_txt_files(fileDirectory)):

        #schema del csv
        schema = StructType([ \
            StructField("input_file", StringType(), True),
            StructField("descrizione", StringType(), True)])

        #legge tutti i file nella directory
        rdd = sc.wholeTextFiles(fileDirectory)
        df = spark.createDataFrame(rdd, schema)
        #df.show()


        udfGetID= udf(Utilities.getIDFromFile)
        udfModificationDate = udf(Utilities.modificationDate)
        udfGetTitolo = udf(Utilities.getTitoloFromFile)
        udfFilePath = udf(Utilities.filePath)
        df = df.withColumn("id_opera", udfGetID(func.substring_index(func.col("input_file"),"/",-1)))\
            .withColumn("titolo_opera", initcap(udfGetTitolo(func.substring_index(func.col("input_file"),"/",-1)))) \
            .withColumn("input_file", udfFilePath(func.col("input_file")))

        df = df.withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))
        #df.printSchema()
        print("Descrizioni lette")
        df.show()



        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (df.count() > 0):
            df.select("descrizione","id_opera","titolo_opera","data_creazione").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)

        Utilities.move_input_file_from_df(moveDirectory, fileDirectory, df)

    else:
        print("Non ci sono descrizioni nella Raw Zone")

def opere_descrizioni_new(spark, sc, fileDirectory):
    print("inizio a spostare le opere da Raw a Standardized: "+fileDirectory)
    #fileDirectory = 'raw/opere/lista/'
    moveDirectory = fileDirectory + "processed/"
    destinationDirectory = 'standardized/opere/descrizioni/'


    if (Utilities.check_txt_files(fileDirectory)):

        # schema del csv
        schema = StructType([ \
            StructField("input_file", StringType(), True),
            StructField("descrizione", StringType(), True)])

        # legge tutti i file nella directory
        rdd = sc.wholeTextFiles(fileDirectory)
        df = spark.createDataFrame(rdd, schema)
        # df.show()

        udfGetID = udf(Utilities.getIDFromFile)
        udfModificationDate = udf(Utilities.modificationDate)
        udfGetTitolo = udf(Utilities.getTitoloFromFile)
        udfFilePath = udf(Utilities.filePath)
        udfSourceFile = udf(Utilities.filePathInProcessed)
        udfFonte = udf(Utilities.filePathFonte)

        df = df.withColumn("id_opera", udfGetID(func.substring_index(func.col("input_file"), "/", -1))) \
            .withColumn("titolo_opera", initcap(udfGetTitolo(func.substring_index(func.col("input_file"), "/", -1)))) \
            .withColumn("input_file", udfFilePath(func.col("input_file"))) \
            .withColumn("source_file", udfSourceFile(func.col("input_file"))) \
            .withColumn("fonte", udfFonte(func.col("input_file")))\
            .withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))

        # df.printSchema()
        print("Descrizioni lette")
        df.show(10, False)

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (df.count() > 0):
            df.drop("input_file").write.mode("append").option("header",
                                                                                                               "true").option(
                "delimiter", ";").csv(destinationDirectory)

        Utilities.move_input_file_from_df(moveDirectory, fileDirectory, df)




    else:
        print("Non ci sono opere in "+fileDirectory)


"""
la funzione serve ad identificare le sottocartelle (fonti) di raw/opere/lista ed eseguire la funzione
opere_lista_new() su ognuna delle sottocartelle trovate
"""
def descrizioni_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/opere/descrizioni")
    fileDirectory = 'raw/opere/descrizioni/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        opere_descrizioni_new(spark, sc, c)


"""
Vengono lette tutti gli autori (da file.csv) nella cartella raw/opere/autori/
Struttura: ID;Nome e cognome;Anno di nascita

Il nome viene messo con le maiuscole, viene ricavata la data di creazione del file
I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano analizzati due volte
"""
#TODO possibilità di spezzare i campi nome e cognome
def opere_autori(spark, sc):
    print("inizio a spostare gli autori da Raw a Standardized")
    fileDirectory = 'raw/opere/autori/'
    moveDirectory = 'raw/opere/autori/processed/'
    destinationDirectory = 'standardized/opere/autori/'

    if (Utilities.check_csv_files(fileDirectory)):
        # schema del csv
        schema = StructType([ \
            StructField("id", StringType(), True), \
            StructField("nome", StringType(), True), \
            StructField("anno", IntegerType(), True)])

        # legge tutti i file nella directory
        df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)
        #df.printSchema()

        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        new = df.withColumn("nome", initcap("nome"))\
                .withColumn("input_file",udfFilePath(input_file_name()))
        new = new.withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))

        print("Autori trovati")
        new.show()
        #new.printSchema()

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (new.count() > 0):
            new.drop("input_file").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)

    else:
        print("Non ci sono autori nella Raw Zone")
"""
Vengono lette tutte le immagini (da file.jpeg) nella cartella raw/opere/immagini/
Struttura del nome file: NOME-ID OPERA.jpeg

Nel DataFrame viene salvato il path, l'id dell'opera associata, il titolo e la data di creazione del file
"""
#TODO gestire svariati altri formati per poi convertire tutto in jpeg
def opere_immagini(spark, sc):
    print("inizio a spostare le immagini da Raw a Standardized")
    fileDirectory = 'raw/opere/immagini/'
    moveDirectory = 'raw/opere/immagini/processed/'
    destinationDirectory = 'standardized/opere/immagini/'

    if (Utilities.check_jpeg_files(fileDirectory)):

        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfFilePathInProcessed = udf(Utilities.filePathInProcessed)
        udfGetID = udf(Utilities.getIDFromFile)
        udfGetTitolo = udf(Utilities.getTitoloFromFile)

        schema = StructType([ \
            StructField("input_file", StringType(), True),
            StructField("immagine", StringType(), True)])

        # legge tutti i file nella directory
        rdd = sc.wholeTextFiles(fileDirectory)
        df1 = spark.createDataFrame(rdd, schema)
        df = df1.withColumn("input_file", udfFilePath(func.col("input_file")))
        #df.show()
        print("Numero di immagini trovate: " + str(df.count()))


        df = df.withColumn("id_opera", udfGetID(func.substring_index(func.col("input_file"), "/", -1))) \
            .withColumn("titolo_opera", initcap(udfGetTitolo(func.substring_index(func.col("input_file"), "/", -1)))) \

        df = df.withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))

        new = df.withColumn("input_file", udfFilePathInProcessed(func.col("input_file")))
        #new.select("input_file").show(2, False)
        #new.printSchema()


        if (new.count() > 0):
            new.select("input_file","id_opera","titolo_opera","data_creazione").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)
            print("Immagini trovate")
            new.select("input_file", "id_opera", "titolo_opera", "data_creazione").show()
        # salvataggio del DataFrame (solo se contiene informazioni)
        Utilities.move_input_file_from_df(moveDirectory, fileDirectory, df)


    else:
        print("Non ci sono immagini nella Raw one")

"""
Vengono lette tutte le categorie (da file.csv) nella cartella raw/visitatori/categorie/
Struttura del nome file: ID;Nome categoria,ETA MIN-ETA MAX

La fascia di età viene spostata in due colonne distinte: eta_min e eta_max
Viene derivata la data di creazione
"""
#TODO le categorie ora sono solo legate alla fascia di età, si possono inserire altri criteri
#TODO servirebbe un campo per identficare la tipologia di categoria (Età, Sesso ecc.)
#TODO anche le lavorazioni nella standardized zone andrebbero modificate di conseguenza
def visitatori_categorie(spark, sc):
    print("inizio a spostare le categorie da Raw a Standardized")

    fileDirectory = 'raw/visitatori/categorie/'
    moveDirectory = 'raw/visitatori/categorie/processed/'
    destinationDirectory = 'standardized/visitatori/categorie/'

    if (Utilities.check_csv_files(fileDirectory)):
        # schema del csv
        schema = StructType([ \
            StructField("id", StringType(), True), \
            StructField("nome", StringType(), True), \
            StructField("fascia_eta", StringType(), True)])

        # legge tutti i file nella directory
        df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)
        #df.printSchema()
        #df.show()

        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfEtaMin = udf(Utilities.getEtaMin)
        udfEtaMax = udf(Utilities.getEtaMax)

        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        new = df.withColumn("nome", initcap("nome"))\
                .withColumn("input_file",udfFilePath(input_file_name()))\
                .withColumn("eta_min", udfEtaMin(func.col("fascia_eta"))) \
                .withColumn("eta_max", udfEtaMax(func.col("fascia_eta")))
        new = new.withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))
        print("Categorie trovate:")
        new.show()
        #new.printSchema()

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (new.count() > 0):
            new.drop("input_file","fascia_eta").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)

    else:
        print("Non ci sono categorie nella Raw Zone")

"""
Vengono lette tutti i visitatori (da file.csv) nella cartella raw/visitatori/elenco/
Struttura del nome file: ID;Nome;Cognome;Sesso;Età

Nome, Cognome e Sesso vengono messi con la prima lettera maiuscola
Viene derivata la data di creazione del file
"""
#TODO possibilità di aggiungere altre informazioni associate ad un visitatore
#TODO possibile controllo che l'età sia scritta correttamente
def visitatori_elenco(spark, sc):
    print("inizio a spostare i visitatori da Raw a Standardized")

    fileDirectory = 'raw/visitatori/elenco/'
    moveDirectory = 'raw/visitatori/elenco/processed/'
    destinationDirectory = 'standardized/visitatori/elenco/'

    if (Utilities.check_csv_files(fileDirectory)):
        # schema del csv
        schema = StructType([ \
            StructField("id", StringType(), True), \
            StructField("nome", StringType(), True), \
            StructField("cognome", StringType(), True),\
            StructField("sesso", StringType(), True),\
            StructField("eta", IntegerType(), True)])

        # legge tutti i file nella directory
        df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)
        #df.printSchema()

        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        new = df.withColumn("nome", initcap("nome")) \
                .withColumn("cognome", initcap("cognome")) \
                .withColumn("sesso", upper("sesso")) \
                .withColumn("input_file",udfFilePath(input_file_name()))
        new = new.withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))

        new.drop("input_file").show()
        #new.printSchema()

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (new.count() > 0):
            new.drop("input_file").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)

    else:
        print("Non ci sono visitatori nella Raw Zone")
"""
Vengono lette tutte le visite (da file.csv) nella cartella raw/visitatori/visite/
Struttura del nome file: ID;Visitatore ID;Opera ID;Durata;Timestamp

Il timestamp viene convertito in data
"""
#TODO possibili controlli sul formato della durata mm:ss
def visitatori_visite(spark, sc):
    print("inizio a spostare le visite da Raw a Standardized")

    fileDirectory = 'raw/visitatori/visite/'
    moveDirectory = 'raw/visitatori/visite/processed/'
    destinationDirectory = 'standardized/visitatori/visite/'

    if (Utilities.check_csv_files(fileDirectory)):
        # schema del csv
        schema = StructType([ \
            StructField("id", StringType(), True), \
            StructField("visitatore_id", StringType(), True), \
            StructField("opera_id", StringType(), True), \
            StructField("durata", StringType(), True), \
            StructField("timestamp", IntegerType(), True)])

        # legge tutti i file nella directory
        df = spark.read.schema(schema).option("delimiter", ";").csv(fileDirectory)
        #df.printSchema()

        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        new = df.withColumn("data_creazione", to_timestamp(from_unixtime("timestamp")))\
                .withColumn("input_file",udfFilePath(input_file_name()))

        new.show()
        #new.printSchema()

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (new.count() > 0):
            new.drop("input_file","timestamp").write.mode("append").option("header", "true").option("delimiter", ";").csv(destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)

    else:
        print("Non ci sono visite nella Raw Zone")

def main():
    print("Da Raw Zone a Standardized Zone")

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
                   "8) Opere (NEW)\n"
                   "0) Tutti\n")

    if (valore == '1'):
        opere_sottocartelle(spark, sc)
    elif (valore == '2'):
        descrizioni_sottocartelle(spark, sc)
    elif (valore == '3'):
        opere_autori(spark, sc)
    elif (valore == '4'):
        opere_immagini(spark, sc)
    elif (valore == "5"):
        visitatori_categorie(spark, sc)
    elif (valore == '6'):
        visitatori_elenco(spark, sc)
    elif (valore == '7'):
        visitatori_visite(spark, sc)
    elif (valore == '0'):
        print()
        # TODO implementare tutti

if __name__ == "__main__":
    main()









