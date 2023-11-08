from pyspark import SparkConf, SparkContext
import re

__author__  = "Kevin Daněk"
__version__ = "1.0"

conf = SparkConf().setMaster("spark://25f56b47c366:7077").setAppName("WordCount")
# conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("/files/data/book.txt")

words = input.flatMap(lambda x: re.sub("[.,_*]/gim", "", x.lower()).split())
word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey(keyfunc= lambda k: -k)
results = word_counts_sorted.take(20)

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')

    if (word):
        print(word.decode() + ":\t\t" + str(count))