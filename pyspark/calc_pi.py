from operator import add
import timeit
import time
import random
import os
import pyspark
from pyspark import SparkContext, SparkConf


def inside(p):
    x, y = random.random()*200-100, random.random()*200-100
    if x*x+y*y<=100.0*100.0:
        return 1
    else:
        return 0

def calc():
    conf = SparkConf().setAppName("test_pi").setMaster("local[4]")#本地使用四个线程
    sc = SparkContext(conf=conf)

    stt=time.time()
    NUM_SAMPLES = 1000000
    col = sc.parallelize(range(0, NUM_SAMPLES),2)
    coordinate_iter=col.map(inside)
    res=coordinate_iter.fold(0.0,add)
    pi = 4 * res/ NUM_SAMPLES
    ett=time.time()


    print(pi)
    print("消耗时间",ett-stt)
    # sc.stop()

pst=time.time()
calc()
pet=time.time()
print("总消耗时间",pet-pst)