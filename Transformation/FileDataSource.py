
from pyspark import  SparkContext, SparkConf
#SparkConf: Cau hinh danh rieng cho spark

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

#setMaster(local[*]) => chia data thanh tat ca partition ma may co CPU 8 core => 8 parttision

sc = SparkContext(conf=conf)

fileRdd = sc.textFile(r"C:\Users\ADMIN\PycharmProjects\PythonProject3\RDD\Transformation\data\data.txt")

print(fileRdd.collect()) #tra ve 1 list
print(fileRdd.getNumPartitions()) # tra ve so pa
print(fileRdd.count())
print(fileRdd.glom().collect())

for part in fileRdd.glom().collect():
    print(part)