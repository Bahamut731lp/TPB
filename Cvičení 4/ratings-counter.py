#!/usr/bin/env python
__author__  = "Ing. Lukáš Matějů, Ph.D."
__version__ = "1.0"
__maintainer__ = "Kevin Daněk"

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("spark://25f56b47c366:7077").setAppName("RatingsHistogram")
# conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

lines = sc.textFile("/files/data/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
