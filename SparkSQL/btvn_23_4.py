import string

from pyspark.sql import SparkSession
import random
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import upper
from pyspark.sql.functions import lit,struct
#create SparkSession

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]")\
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


"""
data tuong trung cho 1 cot trong database nhu sau

yeu cau moi nguoi bien dôi du lieu tu cot data thanh 3 cot: ngày, tháng, năm\



"""

data = [["23/04.2025"],
        ["~11-01//2021"],
        ["2021--/.09*30"]
        ]
df = spark.createDataFrame(data, ["data"])

def split_data(s : string):
    count = 0
    mark = 0
    d = "a"
    m = "b"
    y ="c"
    s = s+" "
    for i  in range(len(s)):
        if s[i] >="0" and s[i] <="9":
            count += 1
            if(s[i+1] < "0" or s[i+1] >"9"):
                mark += 1
                if count == 2:
                    if mark == 1 or mark == 3: d = s[i-1: i+1]
                    elif mark == 2: m = s[i-1:i+1]
                if count == 4: y = s[i-3:i+1]
                count = 0

    return d , m, y

from pyspark.sql.functions import udf
schema = StructType([
    StructField("day", StringType(), True),
    StructField("month", StringType(), True),
    StructField("year", StringType(), True),
])
split_data_udf = udf(split_data, schema)

new =  df.withColumn("data_new",split_data_udf(col("data")))
new1 = new.withColumn("month",col("data_new.month")) \
        .withColumn("year",col("data_new.year")) \
        .withColumn("day",col("data_new.day"))

new1.select(col("day"),col("month"), col("year")).show()

