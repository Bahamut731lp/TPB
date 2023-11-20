from pyspark.sql import SparkSession
import pyspark.sql.functions as functions
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

spark = SparkSession.builder.appName("MostPopularSuperhero").getOrCreate()

schema = StructType([ \
                     StructField("id", IntegerType(), True), \
                     StructField("name", StringType(), True)])

names = spark.read.schema(schema).option("sep", " ").csv("/files/data/marvel-names.txt")

lines = spark.read.text("/files/data/marvel-graph.txt")

# Small tweak vs. what's shown in the video: we trim each line of whitespace as that could
# throw off the counts.
connections = (
    lines.withColumn("id", functions.split(functions.trim(functions.col("value")), " ")[0])
    .withColumn("connections", functions.size(functions.split(functions.trim(functions.col("value")), " ")) - 1)
    .groupBy("id")
    .agg(functions.sum("connections").alias("connections"))
)

mininum = (
    connections
    .where("connections > 0")
    .sort("connections", ascending=[True])
    .take(1)
)[0]["connections"]

most_obsure_heroes = (
    connections
    .where(f"connections = {mininum}")
    .join(names, connections.id == names.id)
    .sort("name", ascending=[True])
    .show()
)
# mostPopular = connections.sort(functions.col("connections").desc()).first()
# mostPopularName = names.filter(functions.col("id") == mostPopular[0]).select("name").first()
# print(mostPopularName[0] + " is the most popular superhero with " + str(mostPopular[1]) + " co-appearances.")

spark.stop()



