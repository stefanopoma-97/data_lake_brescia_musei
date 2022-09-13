import math
import os.path, time

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