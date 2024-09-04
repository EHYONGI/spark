from pyspark import SparkContext

sc = SparkContext()

file_path = '/home/ubuntu/dmf/spark/0.RDD/input.txt'
lines = sc.textFile(file_path)

words = lines.flatMap(lambda line: line.split())
# print(words.collect())

mapped_words = words.map(lambda word: (word, 1))
# print(mapped_words.collect())

reduced_words = mapped_words.reduceByKey(lambda a, b: a+b)
print(reduced_words.collect())