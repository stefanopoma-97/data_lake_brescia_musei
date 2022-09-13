import math

def centuryFromYear(year):
    return math.ceil(year/100)

"nome%file-id.txt"
def getIDFromFile(nome):
    s = nome.replace("%", " ")
    s = s.split("-")[-1].split(".")[0]
    return s

def getTitoloFromFile(nome):
    s = nome.replace("%", " ")
    s = s.split("-")[0]
    return s