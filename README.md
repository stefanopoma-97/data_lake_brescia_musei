# data_lake_brescia_musei
JAVA
1) Scaricare Java 11: https://www.oracle.com/java/technologies/javase/jdk11-archive-downloads.html
2) Installare e inserire in: C:\Program Files\Java\jdk-11.0.15.1\bin
3) Configurare variabile di sistema JAVA_HOME : C:\Program Files\Java\jdk-11.0.15.1
4) Configurare variabile PATH : %JAVA_HOME%\bin (tutorial: https://www.ibm.com/docs/en/b2b-integrator/5.2?topic=installation-setting-java-variables-in-windows)
5) In C:\Program Files\Common Files\Oracle\Java Rimuovere il collegamento javapath

PYCHARM
1) Installare PyCharm: https://www.jetbrains.com/help/pycharm/installation-guide.html
2) Configrare interprete Python 3.9 (tutorial: https://www.jetbrains.com/help/pycharm/creating-virtual-environment.html)

HADOOP
1) Creare C:\hadoop
2) Su questo sito: https://archive.apache.org/dist/hadoop/common/ Scaricare una versione di Hadoop (testato con la 3.3.1)
3) Scaricare 2 file: hadoop-3.3.1-src.tar.gz e hadoop-3.3.1.tar.gz.
4) Inserire i file nella cartella create nel seguente modo:
    C:\hadoop\hadoop-3.3.1\bin
    C:\hadoop\hadoop-3.3.1-src
5) Scaricare winutils.exe dal seguente link https://github.com/kontext-tech/winutils/tree/master/hadoop-3.1.1/bin
6) Inserire il file scaricato in C:\hadoop\hadoop-3.3.1\bin\
7) Scaricare dallo stesso link il file hadoop.dll e inserirlo in C:\Windows\System32


SPARK
1) Scaricare Spark da https://spark.apache.org/downloads.html (testato con Spark 3.3.)
2) Estrarre il contenuto dell'archivio e inseriere la cartella spark-3.3.0-bin-hadoop3 in C:\
3) Impostare la variabile di sistema SPARK_HOME : C:\spark-3.3.0-bin-hadoop3
4) Impostare la variabile PATH : %SPARK_HOME%\bin
5) Impostare la variabile di sistema HADOOP_HOME : C:\hadoop\hadoop-3.3.1
6) Per testare che l'installazione sia andata a buon fine aprire il terminale e digitare: cd %SPARK_HOME%/bin
    Successivamente usare il comando "spark-shell"
   
IMPOSTARE PYSPARK
1) Installare pyspark. Su pycharm andare in: File-> Setting-> Project Name -> Python Interpreter, cliccare sul +, cercare il pacchetto "pyspark" e installarlo
2) Per testare il funzionamento eseguire il seguente script:
   
    from datetime import datetime, date
    from pyspark.sql import Row
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
   
    sc = SparkContext("local", "My App")
    spark = SparkSession.builder.getOrCreate()
    df = spark.createDataFrame([
	    Row(a=1, b=4., c='GFG1', d=date(2000, 8, 1),
		e=datetime(2000, 8, 1, 12, 0))
    ])
    df.show()
   
NEO4J
1) File-> Setting-> Project Name -> Python Interpreter, cliccare sul +
    cercare e installare "neo4j" e "py2neo"
2) nella seguente pagina trovare la versione del connettore in base alla versione di Spark e di Python installata.
    testato con Scala 2.12 e Spark 3.0+ quindi con il connettore: neo4j-connector-apache-spark_2.12-4.0.1_for_spark_3.jar
3) Sulla seguente pagina scaricare il file jar del connettore individuato: https://github.com/neo4j-contrib/neo4j-spark-connector/releases
4) Inserire il file jar in: C:\spark-3.3.0-bin-hadoop3\spark-3.3.0-bin-hadoop3\bin\neo4j-connector-apache-spark_2.12-4.1.4_for_spark_3.jar
5) Scaricare e installare Neo4j Desktop dal sito ufficiale
6) Eseguire l'applicazione e creare un nuovo progetto chiamato "CMS Brescia Musei"
7) Premere su "Add" e aggiungere un "Local DBMS"
8) Nome: "cms_brescia", Password: "neo4j_cms_brescia"
9) Confermare e attendere che il database venga creato
10) Premere su "Start"
11) Successivamente premere su "Open" per aprire da vista da Browser
1
   