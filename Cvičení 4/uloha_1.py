from pyspark import SparkConf, SparkContext

__author__  = "Ing. Lukáš Matějů, Ph.D."
__version__ = "1.0"
__maintainer__ = "Kevin Daněk"

conf = SparkConf().setMaster("spark://25f56b47c366:7077").setAppName("MinTemperatures")
# conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

lines = sc.textFile("/files/data/1800.csv")
parsedLines = lines.map(parseLine)
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
results = maxTemps.collect()

for result in results:
    print(result[0] + "\t{:.2f}C".format((result[1] - 32) / 1.8))
