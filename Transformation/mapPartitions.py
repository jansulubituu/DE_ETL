
from pyspark import  SparkContext, SparkConf
#SparkConf: Cau hinh danh rieng cho spark
import time
from random import Random
conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

#setMaster(local[*]) => chia data thanh tat ca partition ma may co CPU 8 core => 8 parttision

sc = SparkContext(conf=conf)

data = ['Vnontop', 'Quynh', 'Dung', 'Khanh']
# Vnontop : 12, qium : 1213, sadasd : 213
rdd = sc.parallelize(data, 2)

# print(rdd.collect())
# print(rdd.glom().collect())

# def processPartitions(iterator):
#     rand = Random(int(time.time() * 1000) + Random().randint(0,1000))
#     return [f"{name} : {rand.randint(0,1000)}" for name in iterator]
#
# result = rdd.mapPartitions(processPartitions)
# print(result.collect())

result = rdd.mapPartitions(
    lambda iterator :  map(
        lambda name:  f"{name} : {Random(int(time.time() * 1000) + Random().randint(0,1000)).randint(0,1000)}",
        iterator
    )

)

print(result.collect())