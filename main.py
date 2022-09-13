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
