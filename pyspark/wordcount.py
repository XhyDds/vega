from pyspark import SparkContext

sc = SparkContext("local", "Simple App")
text_file = sc.textFile("./test.csv")
counts = text_file.flatMap(lambda line: line.split(" ")) \
             .map(lambda word: (word, 1)) \
             .reduceByKey(lambda a, b: a + b)
counts.saveAsTextFile("./output.txt")
print("hello")
