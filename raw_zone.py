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
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, input_file_name, substring_index, current_timestamp, to_timestamp, upper, lit, when, date_format
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
        df = spark.read.schema(schema).option("delimiter", ",").csv(fileDirectory)

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
Viene aggiunto il secolo, gli autori sono messi con la prima lettera maiuscola, viene derivata la data dal timestamp (o dalla data di crezione del file se il timestamp è null)
Viene inserito l'header e il nuovo dataframe viene salvato nella standardized zone.


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
            .option("delimiter", ",")\
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
                .withColumn("provenienza",
                            when(func.col("provenienza").isNotNull(), initcap("provenienza"))
                            ) \
                .withColumn("tipologia",
                            when(func.col("tipologia").isNotNull(), initcap("tipologia"))
                            ) \
                .withColumn("secolo",
                            when(func.col("anno").isNotNull(), udfFunction_GetCentury(df.anno)))

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

"""

Vengono lette tutte le descrizioni (da file.txt) nella cartella raw/opere/descrizioni/sottocartells
Struttura: testo qualsiasi
Il nome del file .txt deve essere del tipo "nome-ID OPERA.txt" o "ID.txt"

Viene creato un dataframe contenente id_opera, titolo_opera, descrizione, data_creazione, source_file, fonte

I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano analizzati due volte

"""
def opere_descrizioni_new(spark, sc, fileDirectory):
    print("inizio a spostare le descrizioni da Raw a Standardized: "+fileDirectory)
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
la funzione serve ad identificare le sottocartelle (fonti) di raw/opere/descrizioni ed eseguire la funzione
opere_descrizioni_new() su ognuna delle sottocartelle trovate
"""
def descrizioni_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/opere/descrizioni")
    fileDirectory = 'raw/opere/descrizioni/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        opere_descrizioni_new(spark, sc, c)



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
        df = spark.read.schema(schema).option("delimiter", ",").csv(fileDirectory)
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
Vengono lette tutti gli autori (da file.csv) nella cartella raw/opere/autori/sottocartella

Struttura: ID;Nome e cognome;Anno di nascita, source_file, data_creazione, fonte

Il file viene letto e le colonne vengono rinominate (o create da zero) per adattarsi allo schema sopra riportato.
Il nome viene messo con le maiuscole, viene ricavata la data di creazione del file, viene ricavato anche il path del file e il nome della fonte
I file processati vengono inseriti nella sotto-cartella processed, in modo che non vengano analizzati due volte
"""
#TODO possibilità di spezzare i campi nome e cognome
def opere_autori_new(spark, sc, fileDirectory):
    print("inizio a spostare gli autori da Raw a Standardized: "+fileDirectory)
    #fileDirectory = 'raw/opere/lista/'
    moveDirectory = fileDirectory + "processed/"
    destinationDirectory = 'standardized/opere/autori/'


    if (Utilities.check_csv_files(fileDirectory)):

        #legge tutti i file nella directory
        df = spark.read\
            .option("mergeSchema", "true")\
            .option("delimiter", ",")\
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

        # Cambio Nome
        possibili_id = ["autore","nomecognome","nomeecognome","nomeautore"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "nome")
        if 'nome' not in df.columns:
            df = df.withColumn('nome', lit(None).cast("string"))

        # Cambio anno_nascita
        possibili_id = ["anno","data","nascita","annodinascita","datadinascita"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "anno_nascita")
        if 'anno_nascita' not in df.columns:
            df = df.withColumn('anno_nascita', lit(None).cast("int"))


        #cast e ordinamento colonne
        df = df.alias("df").select(
            func.col("id"),
            func.col("nome"),
            func.col("anno_nascita").cast("int"),
        )



        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfSourceFile = udf(Utilities.filePathInProcessed)
        udfFonte = udf(Utilities.filePathFonte)



        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        df = df.withColumn("input_file", udfFilePath(input_file_name())) \
                .withColumn("source_file", udfSourceFile(input_file_name()))\
                .withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file"))))) \
                .withColumn("fonte", udfFonte(input_file_name())) \
                .withColumn("nome",
                            when(func.col("nome").isNotNull(),initcap("nome"))
                            )

        df.printSchema()
        df.show(10, False)

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (df.count() > 0):
            df.drop("input_file").write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)


    else:
        print("Non ci sono opere in "+fileDirectory)


"""
la funzione serve ad identificare le sottocartelle (fonti) di raw/opere/autori ed eseguire la funzione
opere_autori_new() su ognuna delle sottocartelle trovate
"""
def autori_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/opere/autori")
    fileDirectory = 'raw/opere/autori/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        opere_autori_new(spark, sc, c)



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
Vengono lette tutte le immagini (da file.jpeg) nella cartella raw/opere/immagini/sottocartella
Struttura del nome file: "NOME-ID OPERA.jpeg" o "ID.jpeg"

Nel DataFrame viene salvato il path, l'id dell'opera associata, il titolo, la data di creazione del file, la fonte e il path del file 
"""
#TODO gestire svariati altri formati per poi convertire tutto in jpeg
def opere_immagini_new(spark, sc, fileDirectory):
    print("inizio a spostare gli autori da Raw a Standardized: "+fileDirectory)
    #fileDirectory = 'raw/opere/lista/'
    moveDirectory = fileDirectory + "processed/"
    destinationDirectory = 'standardized/opere/immagini/'


    if (Utilities.check_jpeg_files(fileDirectory)):

        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfFilePathInProcessed = udf(Utilities.filePathInProcessed)
        udfGetID = udf(Utilities.getIDFromFile)
        udfGetTitolo = udf(Utilities.getTitoloFromFile)
        udfSourceFile = udf(Utilities.filePathInProcessed)
        udfFonte = udf(Utilities.filePathFonte)

        schema = StructType([ \
            StructField("input_file", StringType(), True),
            StructField("immagine", StringType(), True)])

        # legge tutti i file nella directory
        rdd = sc.wholeTextFiles(fileDirectory)
        df1 = spark.createDataFrame(rdd, schema)
        df = df1.withColumn("input_file", udfFilePath(func.col("input_file")))
        # df.show()

        df = df.withColumn("id_opera", udfGetID(func.substring_index(func.col("input_file"), "/", -1))) \
            .withColumn("titolo_opera", initcap(udfGetTitolo(func.substring_index(func.col("input_file"), "/", -1)))) \
            .withColumn("input_file", udfFilePath(func.col("input_file"))) \
            .withColumn("source_file", udfSourceFile(func.col("input_file"))) \
            .withColumn("fonte", udfFonte(func.col("input_file"))) \
            .withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))
        df.drop("immagine").show(10, False)

        if (df.count() > 0):
            df.drop("input_file","immagine").write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)

        # salvataggio del DataFrame (solo se contiene informazioni)
        Utilities.move_input_file_from_df(moveDirectory, fileDirectory, df)



    else:
        print("Non ci sono opere in "+fileDirectory)

"""
la funzione serve ad identificare le sottocartelle (fonti) di raw/opere/immagini ed eseguire la funzione
opere_immagini_new() su ognuna delle sottocartelle trovate
"""
def immagini_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/opere/immagini")
    fileDirectory = 'raw/opere/immagini/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        opere_immagini_new(spark, sc, c)



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
Vengono lette tutte le categorie (da file.csv) nella cartella raw/visitatori/categorie/sottocartella
Struttura file: id;nome_categoria,eta_min, eta_max, source_file, data_creazione

I file vengono letti e le colonne vengono rinominate o create da zero per adattarsi alla struttura sopra riportata
La fascia di età viene spostata in due colonne distinte: eta_min e eta_max (a patto che non sia null)
Viene derivata la data di creazione
"""
#TODO le categorie ora sono solo legate alla fascia di età, si possono inserire altri criteri
#TODO servirebbe un campo per identficare la tipologia di categoria (Età, Sesso ecc.)
#TODO anche le lavorazioni nella standardized zone andrebbero modificate di conseguenza
def visitatori_categorie_new(spark, sc, fileDirectory):
    print("inizio a spostare le categorie da Raw a Standardized: "+fileDirectory)
    #fileDirectory = 'raw/opere/lista/'
    moveDirectory = fileDirectory + "processed/"
    destinationDirectory = 'standardized/visitatori/categorie/'


    if (Utilities.check_csv_files(fileDirectory)):

        #legge tutti i file nella directory
        df = spark.read\
            .option("mergeSchema", "true")\
            .option("delimiter", ",")\
            .option("inferSchema", "false") \
            .option("header", "true") \
            .csv(fileDirectory)

        #id,nome_categoria,fascia_età

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

        # Cambio Nome Categorie
        possibili_id = ["nome","categoria","nomedellacategoria","nomecategoria"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "nome_categoria")
        if 'nome_categoria' not in df.columns:
            df = df.withColumn('nome_categoria', lit(None).cast("string"))

        # Cambio fascia_eta
        possibili_id = ["eta","fascia","fasciadieta", "età"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "fascia_eta")
        if 'fascia_eta' not in df.columns:
            df = df.withColumn('fascia_eta', lit(None).cast("string"))


        #cast e ordinamento colonne
        df = df.alias("df").select(
            func.col("id"),
            func.col("nome_categoria"),
            func.col("fascia_eta")
        )



        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfSourceFile = udf(Utilities.filePathInProcessed)
        udfFonte = udf(Utilities.filePathFonte)
        udfEtaMin = udf(Utilities.getEtaMin)
        udfEtaMax = udf(Utilities.getEtaMax)



        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        df = df.withColumn("input_file", udfFilePath(input_file_name())) \
                .withColumn("source_file", udfSourceFile(input_file_name()))\
                .withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file"))))) \
                .withColumn("fonte", udfFonte(input_file_name())) \
                .withColumn("nome_categoria",
                            when(func.col("nome_categoria").isNotNull(),initcap("nome_categoria"))
                            )\
                .withColumn("eta_min",
                            when(func.col("fascia_eta").isNotNull(), udfEtaMin(func.col("fascia_eta")))
                            ) \
                .withColumn("eta_max",
                            when(func.col("fascia_eta").isNotNull(), udfEtaMax(func.col("fascia_eta")))
                            )

        df.printSchema()
        df.show(10, False)

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (df.count() > 0):
            df.drop("input_file","fascia_eta").write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        #Utilities.move_input_file(moveDirectory, fileDirectory, df)


    else:
        print("Non ci sono opere in "+fileDirectory)



"""
la funzione serve ad identificare le sottocartelle (fonti) di raw/visitatori/categorie/ ed eseguire la funzione
visitatori_categorie_new() su ognuna delle sottocartelle trovate
"""
def categorie_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/visitatori/categorie/")
    fileDirectory = 'raw/visitatori/categorie/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        visitatori_categorie_new(spark, sc, c)



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
la funzione serve ad identificare le sottocartelle (fonti) di raw/visitatori/elenco/ ed eseguire la funzione
visitatori_elenco_new() su ognuna delle sottocartelle trovate
"""
def visitatori_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/visitatori/elenco/")
    fileDirectory = 'raw/visitatori/elenco/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        visitatori_elenco_new(spark, sc, c)

"""
Vengono lette tutti i visitatori (da file.csv) nella cartella raw/visitatori/elenco/
Struttura del file: id;nome;cognome;sesso;età;source_file;data_creazione;fonte

I file vengono letti e le colonne vengono rinominate o aggiunte per adattarsi allo schema sopra riportato

Nome, Cognome e Sesso vengono messi con la prima lettera maiuscola
Viene derivata la data di creazione del file, il file sorgente e la fonte

La stringa del sesso viene convertita in M o F
"""
#TODO possibilità di aggiungere altre informazioni associate ad un visitatore
#TODO possibile controllo che l'età sia scritta correttamente
def visitatori_elenco_new(spark, sc, fileDirectory):
    print("inizio a spostare i visitatori da Raw a Standardized: "+fileDirectory)
    #fileDirectory = 'raw/opere/lista/'
    moveDirectory = fileDirectory + "processed/"
    destinationDirectory = 'standardized/visitatori/elenco/'


    if (Utilities.check_csv_files(fileDirectory)):

        #legge tutti i file nella directory
        df = spark.read\
            .option("mergeSchema", "true")\
            .option("delimiter", ",")\
            .option("inferSchema", "false") \
            .option("header", "true") \
            .csv(fileDirectory)

        #id,nome_categoria,fascia_età

        #le colonne vengono messe in minuscolo e senza spazi, / o _
        for column in df.columns:
            new_column = column.replace(' ', '').replace('/', '').replace('_', '')
            df = df.withColumnRenamed(column, new_column.lower())



        #ID;Nome;Cognome;Sesso;Età

        #Cambio ID
        possibili_id = []
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "id")
        if 'id' not in df.columns:
            df = df.withColumn('id', lit(None).cast("string"))

        # Cambio Nome
        possibili_id = ["nomevisitatore","visitatore"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "nome")
        if 'nome' not in df.columns:
            df = df.withColumn('nome', lit(None).cast("string"))

        # Cambio cognome
        possibili_id = []
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "cognome")
        if 'cognome' not in df.columns:
            df = df.withColumn('cognome', lit(None).cast("string"))

        # Cambio sesso
        possibili_id = []
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "sesso")
        if 'sesso' not in df.columns:
            df = df.withColumn('sesso', lit(None).cast("string"))

        # Cambio età
        possibili_id = ["eta","anni","età"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "eta")
        if 'eta' not in df.columns:
            df = df.withColumn('eta', lit(None).cast("int"))


        #cast e ordinamento colonne
        df = df.alias("df").select(
            func.col("id"),
            func.col("nome"),
            func.col("cognome"),
            func.col("sesso"),
            func.col("eta").cast("int")
        )



        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfSourceFile = udf(Utilities.filePathInProcessed)
        udfFonte = udf(Utilities.filePathFonte)
        udfSesso = udf(Utilities.checkSesso)



        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        df = df.withColumn("input_file", udfFilePath(input_file_name())) \
                .withColumn("source_file", udfSourceFile(input_file_name()))\
                .withColumn("data_creazione", to_timestamp(from_unixtime(udfModificationDate(func.col("input_file"))))) \
                .withColumn("fonte", udfFonte(input_file_name())) \
                .withColumn("nome",
                            when(func.col("nome").isNotNull(),initcap("nome"))
                            ) \
                .withColumn("cognome",
                            when(func.col("cognome").isNotNull(), initcap("cognome"))
                            ) \
            .withColumn("sesso",
                            when(func.col("sesso").isNotNull(), udfSesso(func.col("sesso")))
                            )

        df.printSchema()
        df.show(25, False)

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (df.count() > 0):
            df.drop("input_file","fascia_eta").write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)


    else:
        print("Non ci sono opere in "+fileDirectory)



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


"""
Vengono lette tutte le visite (da file.csv) nella cartella raw/visitatori/visite/
Struttura del nome file: id;visitatore_id;opera_id;timestamp;durata;data_visita;timestamp_visita;source_file;fonte;data_creazione

i file vengono letti, le colonne vengono rinominate o create per adattarsi alla struttura sopra riportata
Viene ricavato il file sorgente, la data di creazione del file e la fonte
Dal paramentr data_visita (se presente) viene creata una colonna di campo DataType e una colonna di tipo Timestamp (contenente quindi anche ore, minuti e secondi)
La stringa della data può essere scritta in diversi formati, la funzione la converte in un formato standard
La durata viene inoltre convertita in secondi (anche in questo caso sono accettati vari formati diversi)
"""
def visitatori_visite_new(spark, sc, fileDirectory):
    print("inizio a spostare le visite da Raw a Standardized: "+fileDirectory)
    #fileDirectory = 'raw/opere/lista/'
    moveDirectory = fileDirectory + "processed/"
    destinationDirectory = 'standardized/visitatori/visite/'


    if (Utilities.check_csv_files(fileDirectory)):

        #legge tutti i file nella directory
        df = spark.read\
            .option("mergeSchema", "true")\
            .option("delimiter", ",")\
            .option("inferSchema", "false") \
            .option("header", "true") \
            .csv(fileDirectory)

        #id,nome_categoria,fascia_età

        #le colonne vengono messe in minuscolo e senza spazi, / o _
        for column in df.columns:
            new_column = column.replace(' ', '').replace('/', '').replace('_', '')
            df = df.withColumnRenamed(column, new_column.lower())



        #ID;ID visitatore;id Opera;Durata;Timestamp

        #Cambio ID
        possibili_id = []
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "id")
        if 'id' not in df.columns:
            df = df.withColumn('id', lit(None).cast("string"))

        # Cambio visitatore_id
        possibili_id = ["visitatore","idvisitatore","visitatoreid"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "visitatore_id")
        if 'visitatore_id' not in df.columns:
            df = df.withColumn('visitatore_id', lit(None).cast("string"))

        # Cambio opera_id
        possibili_id = ["opera","idopera","operaid"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "opera_id")
        if 'opera_id' not in df.columns:
            df = df.withColumn('opera_id', lit(None).cast("string"))

        # Cambio timestamp
        possibili_id = ["time"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "timestamp")
        if 'timestamp' not in df.columns:
            df = df.withColumn('timestamp', lit(None).cast("int"))

        # Cambio durata
        possibili_id = ["tempo"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "durata")
        if 'durata' not in df.columns:
            df = df.withColumn('durata', lit(None).cast("string"))

        # Cambio data
        possibili_id = ["data"]
        for valore in possibili_id:
            if valore in df.columns:
                df = df.withColumnRenamed(valore, "data_visita")
        if 'data_visita' not in df.columns:
            df = df.withColumn('data_visita', lit(None))


        #cast e ordinamento colonne
        df = df.alias("df").select(
            func.col("id"),
            func.col("visitatore_id"),
            func.col("opera_id"),
            func.col("timestamp").cast("int"),
            func.col("durata"),
            func.col("data_visita").cast("string")
        )



        udfModificationDate = udf(Utilities.modificationDate)
        udfFilePath = udf(Utilities.filePath)
        udfSourceFile = udf(Utilities.filePathInProcessed)
        udfFonte = udf(Utilities.filePathFonte)
        udfDurataInSecondi = udf(Utilities.durataInSecondi)




        # modifica del dataframe (inserita la data, il secolo e sistemato il campo autore)
        df = df.withColumn("input_file", udfFilePath(input_file_name())) \
                .withColumn("source_file", udfSourceFile(input_file_name()))\
                .withColumn("fonte", udfFonte(input_file_name())) \
                .withColumn("data_creazione",
                            when(func.col("timestamp").isNull(),
                                 to_timestamp(from_unixtime(udfModificationDate(func.col("input_file")))))
                            .otherwise(to_timestamp(from_unixtime("timestamp")))
                            )\
                .withColumn("durata",
                            when(func.col("durata").isNotNull(), udfDurataInSecondi(func.col("durata"))
                                 )
                            )\
                .withColumn("durata",func.col("durata").cast("int")) \
            .withColumn("timestamp_visita",
                        when(func.to_timestamp(df.data_visita, "yyyy-MM-dd HH:mm:ss").isNotNull(),
                             func.to_timestamp(df.data_visita, "yyyy-MM-dd HH:mm:ss"
                                               ))
                        .when(func.to_timestamp(df.data_visita, "yyyy-MM-dd").isNotNull(),
                              func.to_timestamp(df.data_visita, "yyyy-MM-dd"
                                                ))
                        .when(func.to_timestamp(df.data_visita, "yyyy\MM\dd HH:mm:ss").isNotNull(),
                              func.to_timestamp(df.data_visita, "yyyy\MM\dd HH:mm:ss"
                                                ))
                        .when(func.to_timestamp(df.data_visita, "yyyy\MM\dd").isNotNull(),
                              func.to_timestamp(df.data_visita, "yyyy\MM\dd"
                                                ))
                        .when(func.to_timestamp(df.data_visita, "yyyy MM dd HH:mm:ss").isNotNull(),
                              func.to_timestamp(df.data_visita, "yyyy MM dd HH:mm:ss"
                                                ))
                        .when(func.to_timestamp(df.data_visita, "yyyy MM dd").isNotNull(),
                              func.to_timestamp(df.data_visita, "yyyy MM dd"
                                                ))
                        .otherwise(None)
                        ) \
            .withColumn("timestamp_visita", func.col("timestamp_visita").cast(TimestampType()))\
            .withColumn("data_visita",
                                when(func.to_date(df.data_visita, "yyyy-MM-dd").isNotNull(),
                                     func.date_format(func.to_date(df.data_visita, "yyyy-MM-dd"), "yyyy-MM-dd")
                                     )
                                .when(func.to_date(df.data_visita, "yyyy\MM\dd").isNotNull(),
                                      func.date_format(func.to_date(df.data_visita, "yyyy\MM\dd"),
                                                       "yyyy-MM-dd")
                                      )
                                .when(func.to_date(df.data_visita, "yyyy/MM/dd").isNotNull(),
                                      func.date_format(func.to_date(df.data_visita, "yyyy/MM/dd"),
                                                       "yyyy-MM-dd")
                                      )
                                .when(func.to_date(df.data_visita, "yyyy MM dd").isNotNull(),
                                     func.date_format(func.to_date(df.data_visita, "yyyy MM dd"), "yyyy-MM-dd")
                                     )
                                .otherwise(None)
                                )\
                    .withColumn("data_visita", func.col("data_visita").cast(DateType())) \

        #to_timestamp(col("input_timestamp"), "MM-dd-yyyy HH mm ss SSS")

        df.printSchema()
        df.show(20, False)

        # salvataggio del DataFrame (solo se contiene informazioni)
        os.makedirs(destinationDirectory, exist_ok=True)
        if (df.count() > 0):
            df.drop("input_file").write.mode("append").option("header", "true").option("delimiter", ";").csv(
                destinationDirectory)

        # i file letti vengono spostati nella cartella processed
        Utilities.move_input_file(moveDirectory, fileDirectory, df)


    else:
        print("Non ci sono opere in "+fileDirectory)


def visite_sottocartelle(spark, sc):
    print("Controllo le sottocartelle di raw/visitatori/visite/")
    fileDirectory = 'raw/visitatori/visite/'

    cartelle = Utilities.check_sub_folder(fileDirectory)

    for c in cartelle:
        print("Sottocartelle: "+c)
        visitatori_visite_new(spark, sc, c)


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

    #opzione per gestire il cast a data e timestamp
    spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")
    #spark.sql("set spark.sql.legacy.timeParserPolicy=CORRECTED")

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

    if (valore == '1'):
        opere_sottocartelle(spark, sc)
    elif (valore == '2'):
        descrizioni_sottocartelle(spark, sc)
    elif (valore == '3'):
        autori_sottocartelle(spark, sc)
    elif (valore == '4'):
        immagini_sottocartelle(spark, sc)
    elif (valore == "5"):
        categorie_sottocartelle(spark,sc)
    elif (valore == '6'):
        visitatori_sottocartelle(spark, sc)
    elif (valore == '7'):
        visite_sottocartelle(spark, sc)
    elif (valore == '0'):
        opere_sottocartelle(spark, sc)
        descrizioni_sottocartelle(spark, sc)
        autori_sottocartelle(spark, sc)
        immagini_sottocartelle(spark, sc)
        categorie_sottocartelle(spark, sc)
        visitatori_sottocartelle(spark, sc)

if __name__ == "__main__":
    main()









