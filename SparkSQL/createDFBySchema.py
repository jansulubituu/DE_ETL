

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql.types import IntegerType, DoubleType
from datetime import datetime
from sqlite3.dbapi2 import Timestamp
from xmlrpc.client import DateTime

#create SparkSession

spark = SparkSession.builder \
    .appName("DE_ETL") \
    .master("local[*]")\
    .config("spark.executor.memory", "2g") \
    .getOrCreate()
#
# data = spark.sparkContext.parallelize([
#     Row(1, "dat", 18),
#     Row(2, "phong", 22),
# ])
#
# schema = StructType([
#     StructField("id", LongType(), True),
#     StructField("name", StringType(), True),
#     StructField("age", IntegerType(), True),
# ])
#
# df = spark.createDataFrame(data, schema)
# df.show()


"""
from pyspark.sql.type import * : import cac kieu du lieu
TimestampType : bieu dien cac gia tri thoi gian
DateType : bieu dien cac gia tri ngay thang nam

ArrayType : bieu dien cac gia tri cua array list
 - elementType bieu dien cacs gia tri cuar cacs phan tu trong ArrayType(IntegerType, StringType)
 
MapType(keyType, valueType)
 - keyType 
 - valueType 
 
StructType(): bieu dien cau truc cua structField
- chuaw danh sach casc doi tuonwg structField

StructField(name, dataType, nullable<true/fale>)

 
"""

data = [
    Row(
        name = "dat",
        age = 18,
        id = 1,
        salary = 10000.0,
        bonus = 250.75,
        isActive = True,
        score = [3.56, 3.75, 3.89],
        attributes ={"dept":"engineering", "role":"Deve"},
        hireDate = datetime.strptime("2023-01-15","%Y-%m-%d"),
        last_login = datetime.strptime("2023-01-15 18:30:00","%Y-%m-%d %H:%M:%S"),

    ),
    Row(
        name="dattttt",
        age=20,
        id=2,
        salary=1000.0,
        bonus=25.75,
        isActive=True,
        score=[3.6, 3.5, 3.8],
        attributes={"dept": "engineering", "role": "Deve"},
        hireDate=datetime.strptime("2023-01-15", "%Y-%m-%d"),
        last_login=datetime.strptime("2023-02-12 17:30:00", "%Y-%m-%d %H:%M:%S"),

    )
]

schemaPeople = StructType(
    [
        StructField("name", StringType(), True),
        StructField("age", IntegerType(), True),
        StructField("id", LongType(), True),
        StructField("salary", DoubleType(), True),
        StructField("bonus", FloatType(), True),
        StructField("isActive", BooleanType(), True),
        StructField("score", ArrayType(DoubleType(), True), True),
        StructField("attributes", MapType(StringType(), StringType()), True),
        StructField("hireDate", DateType(), True),
        StructField("lastLogin", TimestampType(), True)
    ]
)

df = spark.createDataFrame(data, schemaPeople)
df.show(truncate = False)