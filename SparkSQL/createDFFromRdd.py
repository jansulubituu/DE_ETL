from pyspark.sql import SparkSession
import random
#create SparkSession

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]")\
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

rdd = spark.sparkContext.parallelize(range(1,11)) \
    .map(lambda x: (x, random.randint(0, 99) * x))

# print(rdd.collect())

df = spark.createDataFrame(rdd, ["key","value"])
df.show()
df.printSchema()