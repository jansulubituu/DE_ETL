
from pyspark import  SparkContext, SparkConf
#SparkConf: Cau hinh danh rieng cho spark

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

#setMaster(local[*]) => chia data thanh tat ca partition ma may co CPU 8 core => 8 parttision

sc = SparkContext(conf=conf)

fileRdd = sc.textFile(r"C:\Users\ADMIN\PycharmProjects\PythonProject3\RDD\Transformation\data\data.txt") \
    .map(lambda x : x.lower()) \
    .flatMap(lambda x: x.split(" ")) \
    .map(lambda x: (x, 1) )\
    .reduceByKey(lambda x, y: x + y) \
    # .map(lambda x: (x[1], x[0])) \
    # .sortByKey(ascending=False)

rdd2 = sc.parallelize([("the",1)])

rdd = fileRdd.join(rdd2)
print(rdd.collect())

# print(fileRdd.collect()) #tra ve 1 list
# print(fileRdd.getNumPartitions()) # tra ve so pa
# print(fileRdd.count())
# print(fileRdd.glom().collect())
#
# for part in fileRdd.glom().collect():
#     print(part)

#cho day  so nguyen duong lon xon  => tim ra so nguyen duong nao xuat hien bao nhieu lan

# ex: 0,0 5,3,4,4,5,3,432,42,4,24,234,234,23,4,234,654,64565,6,64,64,57,5,438568,6,5,2,683456,

