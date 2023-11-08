from pyspark import SparkConf, SparkContext

__author__  = "Ing. Lukáš Matějů, Ph.D."
__version__ = "1.0"
__maintainer__ = "Kevin Daněk"

conf = SparkConf().setMaster("spark://25f56b47c366:7077").setAppName("TotalSpentByCustomer")
sc = SparkContext(conf = conf)

def parse_line(line):
    fields = line.split(',')
    customer_id = fields[0]
    item_id = fields[1]
    price = float(fields[2])
    return (customer_id, item_id, price)

lines = sc.textFile("/files/data/customer-orders.csv")
entries = lines.map(parse_line)

# Postačí nám záznamu jenom o zákazníkovi a ceně - s položkama nemusíme pracovat
# Nechci to dělat v parse_line, protože tohle odstranění informace je specifické pro tuhle úlohu
# A parsovací funkce mám rád úplné, resp. nic nezatajující
entries = entries.map(lambda x: (x[0], x[2]))

# Sečtení cen
sums = entries.reduceByKey(lambda accumulator, price: accumulator + price)
sums_sorted = sums.map(lambda x: (x[1], x[0])).sortByKey(keyfunc= lambda k: -k).map(lambda x: (x[1], x[0]))
results = sums_sorted.collect()

for result in results:
    customer = str(result[0])
    price = float(result[1])

    print(customer + ":\t\t{:.2f}".format(price))
