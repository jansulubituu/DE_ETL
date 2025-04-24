from pyspark.sql import SparkSession
import random
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField
from pyspark.sql.functions import upper
#create SparkSession

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]")\
    .config("spark.executor.memory", "2g") \
    .getOrCreate()

# tu tao schema

schema = StructType([
    StructField("id", StringType(), True),

])
jsonData = spark.read.json(r"C:\Users\ADMIN\PycharmProjects\PythonProject3\data\2015-03-01-17.json")


#withColumn(colName, column) có 2 đối số đầu vào
from pyspark.sql.functions import lit,struct

# jsonData.withColumn("id2", lit("thanhdepzai")).sellect(col("id"), col("id2")).show()

jsonData.withColumn("actor.id2", lit("thanhdepzai")).sellect(col("id"), col("id2")).show()

json_tranformation = jsonData.withColumn(
    "actor",
    struct(
        col("actor.id").alias("id"),
        col("actor.login").alias("login"),
        col("actor.gravatar_id").alias("gravatar_id"),
        col("actor.avatar_url").alias("avatar_url"),
        lit("datdeptrai").alias("id2")
    )
)
json_tranformation.select(col("actor.id"),col("actor.id2")).show()


#viet function bằng python ko lien quan den spark return id + 2

def add(data: int) -> int:
    return data + 2

from pyspark.sql.functions import udf
# user defined function ham do ngdung dinh nghia mà spark ko có
#convert ham add -> udf

addUDF = udf(add)

df = jsonData.withColumn("new_col", addUDF(col("actor.id")))
df.select(col("actor.id"), col("new_col")).show()


