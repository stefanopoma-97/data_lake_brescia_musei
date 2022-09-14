import math
import os.path, time, os
from pyspark.sql.functions import col, avg, to_date, from_unixtime, initcap, udf, desc, input_file_name
import shutil

def modificationDate(file):
    ti_c = os.path.getctime(file)
    c_ti = time.ctime(ti_c)
    return int(ti_c)

def centuryFromYear(year):
    return math.ceil(year/100)

"nome%file-id.txt"
def getIDFromFile(nome):
    s = nome.replace("%", " ")
    s = s.split("-")[-1].split(".")[0]
    return s
"10-24"
def getEtaMin(nome):
    if nome == None:
        return nome
    else:
        return nome.split("-")[0]

def getEtaMax(nome):
    if nome == None:
        return nome
    else:
        return nome.split("-")[1]

def getTitoloFromFile(nome):
    s = nome.replace("%20", " ")
    s = s.split("-")[0]
    return s

def recreateSpace(origin):
    s = origin.replace("%20", " ")
    return s

def filePath(origin):
    s = origin.replace("%20", " ")
    s = s.replace("file:/", "")
    s = s.replace("//", "")
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

def drop_duplicates_row(df, colonna_ordine, subset):
    return df.orderBy(desc(colonna_ordine)).drop_duplicates(subset=subset)

def move_input_file(moveDirectory, fileDirectory, df):
    os.makedirs(moveDirectory, exist_ok=True)
    data = df.withColumn("input_file", input_file_name())
    lista = data.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        shutil.move(fileDirectory + fname, moveDirectory + fname)

def move_input_file_from_df(moveDirectory, fileDirectory, df):
    os.makedirs(moveDirectory, exist_ok=True)
    lista = df.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        shutil.move(fileDirectory + fname, moveDirectory + fname)

def remove_input_file(fileDirectory, df):
    data = df.withColumn("input_file", input_file_name())
    lista = data.select("input_file").rdd.flatMap(lambda x: x).collect()
    for a in list(set(lista)):
        fname = a.split("/")[-1]
        os.remove(fileDirectory+fname)