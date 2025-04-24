from pyspark.sql import SparkSession

#create SparkSession

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]")\
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

textFile = spark.read.text(r"C:\Users\ADMIN\PycharmProjects\PythonProject3\data\data\data.txt")

textFile.show(truncate=False)
