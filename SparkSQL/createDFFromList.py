from pyspark.sql import SparkSession

#create SparkSession

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]")\
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

data = [
    ("thanh", "Student", 2004),
    ("duc", "backend", 2006)
]

df = spark.createDataFrame(data,["name", "job", "year"])

df.show()
df.printSchema()