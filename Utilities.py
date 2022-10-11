import math
import os.path, time, os
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, desc, input_file_name
import shutil
import re
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType, TimestampType, ArrayType
import os


def modificationDate(file):
    ti_c = os.path.getctime(file)
    c_ti = time.ctime(ti_c)
    return int(ti_c)

def centuryFromYear(year):
    if year is not None:
        return math.ceil(year/100)
    else:
        return None


"10-24"
def getEtaMin(nome):
    if nome == None:
        return nome
    else:
        if (nome.__contains__("-")):
            return nome.split("-")[0]
        else:
            return None

def getEtaMax(nome):
    if nome == None:
        return nome
    else:
        if (nome.__contains__("-")):
            return nome.split("-")[1]
        else:
            return None

def getTitoloFromFile(nome):
    s = nome.replace("%20", " ")
    if (s.__contains__("-")):
        return s.split("-")[0]
    else:
        return None

"nome%file-id.txt"
def getIDFromFile(nome):
    s = nome.replace("%20", " ")
    if (s.__contains__("-")):
        return s.split("-")[-1].split(".")[0]
    else:
        return s.split(".")[0]

def recreateSpace(origin):
    s = origin.replace("%20", " ")
    return s

def filePath(origin):
    osName = os.name
    s = origin.replace("%20", " ")
    if (osName == "nt"):
        s = s.replace("file:/", "")
    else:
        s = s.replace("file:", "")
    s = s.replace("//", "")
    return s


def filePathInProcessed(origin):
    # Get OS name
    osName = os.name
    s = origin.replace("%20", " ")
    if (osName == "nt"):
        s = s.replace("file:/", "")
    else:
        s = s.replace("file:", "")
    s = s.replace("//", "")
    old = "/"
    new = "/processed/"
    s = new.join(s.rsplit(old, 1))
    return s

def filePathFonte(origin):
    osName = os.name
    s = origin.replace("%20", " ")
    if (osName == "nt"):
        s = s.replace("file:/", "")
    else:
        s = s.replace("file:", "")

    s = s.replace("//", "")
    array = s.split("/")
    return array[-2]

def checkSesso(s):
    s = s.upper()
    replace_f = ["W","DONNA","WOMEN","FEMMINA"]
    replace_u = ["MASCHIO", "MEN", "UOMO"]
    for r in replace_f:
        s = s.replace(r, "F")
    for r in replace_u:
        s = s.replace(r, "M")


    if len(s)==1:
        if s=="M" or s=="F":
            return s
    return None


import datetime


def durataInSecondi(s):
    pattern_secondi=r"^\d+"
    if re.fullmatch(pattern_secondi, s)!=None:
        #print(s + "match con:"+pattern_secondi)
        #processed = '10:15:30'
        #h, m, s = processed.split(':')
        return (int(datetime.timedelta(seconds=int(s)).total_seconds()))

    #H:mm:ss
    pattern = r"^\d{1,2}:\d{2}:\d{2}"
    if re.fullmatch(pattern, s)!=None:
        h, m, se = s.split(':')
        return (int(datetime.timedelta(hours=int(h),minutes=int(m),seconds=int(se)).total_seconds()))

    # mm:ss
    pattern = r"^\d{1,2}:\d{2}"
    if re.fullmatch(pattern, s) != None:
        m, se = s.split(':')
        return (int(datetime.timedelta(minutes=int(m), seconds=int(se)).total_seconds()))


def filePathInProcessedNew(origin):
    osName = os.name
    s = origin.replace("%20", " ")
    if (osName == "nt"):
        s = s.replace("file:/", "")
    else:
        s = s.replace("file:", "")
    s = s.replace("//", "")
    array = s.split("/")
    array = array.pop()
    stringa = ""
    for el in array:
        stringa = stringa+el+"/"
    old = "/"
    new = "/processed/"
    stringa = new.join(stringa.rsplit(old, 1))
    return s

"""
Funzione per controllare che in una determinata directory sia presente un file CSV
"""
def check_csv_files(directory):
    file = False
    files = os.listdir(directory)
    for fname in files:
        if (os.path.isfile(directory + fname)):
            if (fname.split(".")[-1] == "csv"):
                file=True
    return file

"""
Funzione per restituire le cartelle con file presenti in una directory
"""
def check_sub_folder(directory):
    print("controllo quante sottocartelle piene ci sono")
    array = []
    dir = os.listdir(directory)
    for d in dir:
        if (os.path.isdir(directory + d)):
            if (d != "processed"):
                array.append(d)
    cartelle_non_vuote = []
    for cartella in array:
        if len(os.listdir(directory + cartella))>0:
            cartelle_non_vuote.append(directory + cartella+"/")
    return cartelle_non_vuote

def check_jpeg_files(directory):
    file = False
    files = os.listdir(directory)
    for fname in files:
        if (os.path.isfile(directory + fname)):
            if ((fname.split(".")[-1] == "jpeg") or (fname.split(".")[-1] == "png") or (fname.split(".")[-1] == "jpg")):
                file=True
    return file

def check_txt_files(directory):
    file = False
    files = os.listdir(directory)
    for fname in files:
        if (os.path.isfile(directory + fname)):
            if (fname.split(".")[-1] == "txt"):
                file=True
    return file

def drop_duplicates_row(df, colonna_ordine, subset):
    return df.orderBy(desc(colonna_ordine)).drop_duplicates(subset=subset)

def move_input_file(moveDirectory, fileDirectory, df):
    os.makedirs(moveDirectory, exist_ok=True)
    data = df.withColumn("input_file", input_file_name())
    lista = data.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        fname = fname.replace("%20", " ")
        shutil.move(fileDirectory + fname, moveDirectory + fname)
    files = os.listdir(fileDirectory)
    for fname in files:
        if (os.path.isfile(fileDirectory + fname)):
            os.remove(fileDirectory+fname)

def move_input_file_from_df(moveDirectory, fileDirectory, df):
    os.makedirs(moveDirectory, exist_ok=True)
    lista = df.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        fname = fname.replace("%20", " ")
        shutil.move(fileDirectory + fname, moveDirectory + fname)
    files = os.listdir(fileDirectory)
    for fname in files:
        if (os.path.isfile(fileDirectory + fname)):
            os.remove(fileDirectory + fname)

def remove_input_file(fileDirectory, df):
    data = df.withColumn("input_file", input_file_name())
    lista = data.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        os.remove(fileDirectory+fname)

def check_categorie_duplicate(id,array):
    for v in array:
        print("ID "+str(id))
        print(v.nome_categoria)
        print(v.eta_min)
    return list(array[0]) #ritorna lista
    #return {"in_type": array[0].id, "in_var": array[0].eta_min}
    data = [("James", "", "Smith", "36636", "M", 3000),
            ("Michael", "Rose", "", "40288", "M", 4000),
            ("Robert", "", "Williams", "42114", "M", 4000),
            ("Maria", "Anne", "Jones", "39192", "F", 4000),
            ("Jen", "Mary", "Brown", "", "F", -1)
            ]

    schema = StructType([ \
        StructField("firstname", StringType(), True), \
        StructField("middlename", StringType(), True), \
        StructField("lastname", StringType(), True), \
        StructField("id", StringType(), True), \
        StructField("gender", StringType(), True), \
        StructField("salary", IntegerType(), True) \
        ])
    schema.add("James", "", "Smith", "36636", "M", 3000)

    return schema

def empty_autori(spark):
    emptyRDD = spark.sparkContext.emptyRDD()
    schema = StructType([
        StructField('id', IntegerType(), True),
        StructField('nome', StringType(), True),
        StructField('anno_nascita', IntegerType(), True),
        StructField('source_file', StringType(), True),
        StructField('data_creazione', TimestampType(), True),
        StructField('fonte', StringType(), True)
    ])
    df = spark.createDataFrame(emptyRDD, schema)
    return df

def empty_descrizioni(spark):
    emptyRDD = spark.sparkContext.emptyRDD()
    schema = StructType([
        StructField('descrizione', StringType(), True),
        StructField('id_opera', StringType(), True),
        StructField('titolo_opera', StringType(), True),
        StructField('source_file', StringType(), True),
        StructField('fonte', StringType(), True),
        StructField('data_creazione', TimestampType(), True)
    ])
    df = spark.createDataFrame(emptyRDD, schema)
    return df

def empty_immagini(spark):
    emptyRDD = spark.sparkContext.emptyRDD()
    schema = StructType([
        StructField('id_opera', IntegerType(), True),
        StructField('titolo_opera', StringType(), True),
        StructField('source_file', StringType(), True),
        StructField('fonte', StringType(), True),
        StructField('data_creazione', TimestampType(), True)
    ])
    df = spark.createDataFrame(emptyRDD, schema)
    return df