from pyspark import SparkConf, SparkContext
from pathlib import Path

import json
import re

__author__  = "Kevin Daněk"
__version__ = "1.0"

conf = SparkConf().setMaster("spark://25f56b47c366:7077").setAppName("IdnesBonus")
sc = SparkContext(conf = conf)

# Rozhodl jsem se to dát do jednoho texťáku,
# páč je to rychlejší než načítat 9,5 tisíce malých JSONů
lines = sc.textFile("/files/data/idnes.txt")
jsons = lines.map(lambda line: json.loads(line))

words = jsons.flatMap(lambda data: re.sub("[.,_*\\n]/gim", "", data["content"].lower().strip()).split()) 
word_counts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
word_counts_sorted = word_counts.map(lambda x: (x[1], x[0])).sortByKey(keyfunc= lambda k: -k)
results = word_counts_sorted.take(50)

for result in results:
    count = str(result[0])
    word = result[1]

    if (word):
        print(word + ":\t\t" + str(count))