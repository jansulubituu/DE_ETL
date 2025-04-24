from pyspark.sql import SparkSession

#create SparkSession

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]")\
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

jsonData = spark.read.json(r"C:\Users\ADMIN\PycharmProjects\PythonProject3\data\2015-03-01-17.json")

jsonData.show(truncate=True)