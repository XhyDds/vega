# from pyspark import SparkContext

# sc = SparkContext("local", "Simple App")
# text_file = sc.textFile("file:///usr/local/spark/README.md")
# counts = text_file.flatMap(lambda line: line.split(" ")) \
#              .map(lambda word: (word, 1)) \
#              .reduceByKey(lambda a, b: a + b)
# counts.saveAsTextFile("file:///usr/local/spark/output")
# print("hello")
import timeit
import time
import random
import os
import pyspark
from pyspark import SparkContext, SparkConf


def inside(p):
    x, y = random.random(), random.random()
    return x*x + y*y < 1

def calc():
    conf = SparkConf().setAppName("test_SamShare").setMaster("local[4]")#本地使用四个线程
    sc = SparkContext(conf=conf)

    stt=time.time()
    NUM_SAMPLES = 1000000
    count = sc.parallelize(range(0, NUM_SAMPLES)).filter(inside).count()
    pi = 4 * count / NUM_SAMPLES
    ett=time.time()

    
    print(pi)
    print("消耗时间",ett-stt)
    # sc.stop()

# print(timeit.timeit(calc,1))
pst=time.time()
calc()
pet=time.time()
print("总消耗时间",pet-pst)
"""
----------------------------------------------
                Transform算子解析
----------------------------------------------
"""
# 以下的操作由于是Transform操作，因为我们需要在最后加上一个collect算子用来触发计算。
# 1. map: 和python差不多，map转换就是对每一个元素进行一个映射
# rdd = sc.parallelize(range(1, 11), 4)
# rdd_map = rdd.map(lambda x: x*2)
# print("原始数据：", rdd.collect())
# print("扩大2倍：", rdd_map.collect())
# 原始数据： [1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# 扩大2倍： [2, 4, 6, 8, 10, 12, 14, 16, 18, 20]