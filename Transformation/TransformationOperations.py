#tao rdd moi tu rdd cu

from pyspark import  SparkContext, SparkConf
#SparkConf: Cau hinh danh rieng cho spark

conf = SparkConf().setAppName("DE").setMaster("local[*]").set("spark.executor.memory", "4g")

#setMaster(local[*]) => chia data thanh tat ca partition ma may co CPU 8 core => 8 parttision

sc = SparkContext(conf=conf)

#rdd la bat bien, muon bien doi => tao 1 rdd moi

numbers = [1,2,3,4,5,6,7,8,9,10]

numbersRdd = sc.parallelize(numbers)
# print(numbersRdd)
squareRdd = numbersRdd.map(lambda x: x ** 2)
print(squareRdd.collect())

filterRdd = numbersRdd.filter(lambda x: x > 3)
print(filterRdd.collect())

flatMapRdd = numbersRdd.map(lambda x: [x, x*2])
print(flatMapRdd.collect())