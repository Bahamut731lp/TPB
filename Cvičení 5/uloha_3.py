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
    .join(names, connections.id == names.id)
    .where(f"connections = {mininum}")
    .select(connections.id, connections.connections, names.name)
    .distinct() 
    .sort("name", ascending=[True])
    .show()
)

spark.stop()



