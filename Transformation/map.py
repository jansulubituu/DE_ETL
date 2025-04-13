
from pyspark import  SparkContext, SparkConf
#SparkConf: Cau hinh danh rieng cho spark

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

#setMaster(local[*]) => chia data thanh tat ca partition ma may co CPU 8 core => 8 parttision

sc = SparkContext(conf=conf)

fileRdd = sc.textFile(r"C:\Users\ADMIN\PycharmProjects\PythonProject3\RDD\Transformation\data\data.txt")


allCapRdd = fileRdd.map(lambda line: line.upper() )
# ham map se ap dung tren hang du lieu(1 ban ghi du lieu)
# print(allCapRdd.collect())
for line in allCapRdd.collect():
    print(line)