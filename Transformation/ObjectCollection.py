

from pyspark import SparkContext


#initialize SparkContext
sc = SparkContext("local[2]","103-Spark")

#create an object collection
data = [
    {"id": 1, "name": "Vnontop"},
    {"id": 2, "name": "Dat"},
    {"id": 3, "name": "thanh"}
]

#create rdd from data
rdd = sc.parallelize(data)

print(rdd.collect())
#ham collect tra ve du lieu trong rdd duoi dang 1 list

print(rdd.count())
print(rdd.getNumPartitions())
print(rdd.first())
print(rdd.glom().collect())


