from operator import add
import timeit
import time
import random
import os
import pyspark
from pyspark import SparkContext, SparkConf


def dot_fn(k_q):
    sum = 0.0
    for j in range(INPUT_SIZE):
        sum += k_q[0][j] * k_q[1][j]
    return sum


def calc():
    conf = SparkConf().setAppName("multihead_attention").setMaster("local[4]")#本地使用四个线程
    sc = SparkContext(conf=conf)

    
    INPUT_SIZE = 1000
    HEADS_NUM = 100
    num_slices = 2
    k_weights = []
    v = []

    for _ in range(HEADS_NUM):
        k_weights.append([random.random()*2-1 for _ in range(INPUT_SIZE)])
        v.append(random.random()*2-1)
    
    k_weights_param = sc.parallelize(k_weights, num_slices); #获得k_weight的参数
    v_param = sc.parallelize(v, num_slices); #获得v的参数

    stt=time.time()
    for _ in range(3):
        new_input = [random.random()*2-1 for _ in range(INPUT_SIZE)]

        input_ref_rdd = sc.parallelize([new_input for _ in range(HEADS_NUM)], num_slices)
        key_input_pair = k_weights_param.zip(input_ref_rdd)

        query_res = key_input_pair.map(dot_fn).collect()

        max_query_res = query_res[0]
        for i in range(HEADS_NUM):
            if query_res[i]>max_query_res:
                max_query_res = query_res[i]
        
        for i in range(HEADS_NUM):
            query_res[i] = (query_res[i] - max_query_res).exp()

        query_res_softmax = sc.parallelize(query_res, num_slices)
        v_weight_pair = v_param.zip(query_res_softmax)
        res_vec = v_weight_pair.map(lambda v_w:  v_w[0] * v_w[1] )
        res = res_vec.fold(0.0, add)

        print("result:", res)
        
    ett=time.time()

    print("消耗时间",ett-stt)
    # sc.stop()

#pst=time.time()
calc()
#pet=time.time()
#print("总消耗时间",pet-pst)