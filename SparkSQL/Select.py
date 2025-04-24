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

# jsonData.select(col("id")).show(10)
# alias : doi tien cot dung nhu ham as trong sql
# jsonData.select(col("id").alias("user_id"), col("actor.id").alias("actor_id")).show()

# actor.id nhan doi và type viet hoa
# jsonData.select(col("actor.id").alias("actor_id"), col("type")).show()

jsonData.select(
    col("actor.id").alias("actor_id") * 2,
    upper(col("type").alias("Upper_type")),
    #ko dung duoc upper()
)

jsonData.select(
    col("actor.id"),
    (col("actor.id") > 9614759).alias("actor_id > 9614759"),
)

# => return true/false

#viet code bằng sql

# jsonData.selectExpr(
#     "count(distinct(id)) as user_id",
#     "count(distinct(actor.id)) as actor_id",
# )
# => so lượng mỗi cột

# from pyspark.sql.functions import length
#
# jsonData.select(col("id").alias("user_id"))\
#     .filter(length(col("id")) < 11)


# su dung distinct

# jsonData.select(col("payload.issue.id").alias("issue_id")).distinct().selectExpr("count(issue_id) as count)")

#dropDuplicate
jsonData.select(col("payload.issue.id").alias("payload.issue_id")).dropDuplicates(["id"])

#OrderBy
jsonData.sort(col("id").asc()).select(col("id")).show()

jsonData.orderBy(col("id").desc()).show()

