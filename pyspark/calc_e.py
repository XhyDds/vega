from operator import add
import timeit
import time
import random
import os
import pyspark
from pyspark import SparkContext, SparkConf


def f(p):
    res=1.0
    for i in range(1,p):
        res/=i
    return res
def calc():
    conf = SparkConf().setAppName("test_e").setMaster("local[4]")#本地使用四个线程
    sc = SparkContext(conf=conf)

    stt=time.time()
    NUM_SAMPLES = 10000
    col = sc.parallelize(range(0, NUM_SAMPLES),3)
    coordinate_iter=col.map(f)
    res=coordinate_iter.fold(0.0,add)
    ett=time.time()


    print(res)
    print("消耗时间",ett-stt)
    # sc.stop()

pst=time.time()
calc()
pet=time.time()
print("总消耗时间",pet-pst)