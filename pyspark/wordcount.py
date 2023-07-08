import timeit
import time
stt=time.time()
from pyspark import SparkContext

sc = SparkContext("local", "wordcount App")
text_file = sc.textFile("/home/yuri/docs/enwik8")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("./output")
print("wordcount finished")
ett=time.time()
print("time elapsed:",ett-stt)