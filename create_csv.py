
import random, string

def random_string():
    random_list = []
    for i in range(10):
        random_list.append(random.choice(string.ascii_uppercase + string.digits))
    return (''.join(random_list))

def eta_categoria():
    max = (random.randint(5, 100))
    min=(random.randint(5, max))
    return str(min)+"-"+str(max)

def create_opere(nome, r):
    out = open("Output/file/" + nome + ".csv", "w", encoding="utf-8")
    out.write("ID ,TAG ID ,TIPOLOGIA ,TITOLO,Anno,Provenienza,AUTORE ,TIMESTAMP" + "\n")

    for i in range(r):
        out.write(str(i) + ","
                  + str(random.randint(1, 100)) + ","
                  + "tipologia " + random_string() + ","
                  + "titolo " + random_string() + ","
                  + str(random.randint(1900, 2020)) + ","
                  + "citta " + random_string() + ","
                  + "Nome autore " + str(i) + ","
                  + str(1663924638)
                  + "\n")
    out.close()

def create_autori(nome, r):
    out = open("Output/file/" + nome + ".csv", "w", encoding="utf-8")
    out.write("ID, NOME, ANNO" + "\n")
    for val in range(r):
        out.write(str(val) + ","
                  + "Nome autore " + str(val) + ","
                  + str(random.randint(1900, 2020)) + ","
                  + "\n")
    out.close()

def create_visitatori(nome, r):
    out = open("Output/file/" + nome + ".csv", "w", encoding="utf-8")
    out.write("id,nome,cognome,sesso,età" + "\n")
    for val in range(r):
        out.write(str(val) + ","
                  + "Nome " + random_string() + ","
                  + "Cognome " + random_string() + ","
                  + "M" + ","
                  + str(random.randint(5, 100)) + ","
                  + "\n")
    out.close()

def create_categorie(nome, r):
    out = open("Output/file/" + nome + ".csv", "w", encoding="utf-8")
    out.write("id,nome,età" + "\n")
    for val in range(r):
        out.write(str(val) + ","
                  + "Nome " + random_string() + ","
                  + eta_categoria() + ","
                  + "\n")
    out.close()

def create_visite(nome, r):
    out = open("Output/file/" + nome + ".csv", "w", encoding="utf-8")
    out.write("id,visitatore,opera,durata,timestamp,data" + "\n")
    for val in range(r):
        out.write(str(val) + ","
                  + str(random.randint(1, r)) + ","
                  + str(random.randint(1, r)) + ","
                  + str(random.randint(1, 30))+":"+str(random.randint(10, 59)) + ","
                  +"1662811200"+","
                  +r"2022\09\21 18:40:22"+","
                  + "\n")
    out.close()


nome="curated_visite_10000"
r=10000
create_visite(nome, r)

