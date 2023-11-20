from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

spark = SparkSession.builder.master("spark://17f2d0a5a197:7077").appName("SparkSQL").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("/files/data/fakefriends-header.csv")
    
print("Here is our inferred schema:")
people.printSchema()
(
    people
    .groupBy("age")
    .avg("friends")
    .withColumnRenamed("avg(friends)", "friends")
    .withColumn("friends", functions.format_number("friends", 2))
    .sort("age", ascending=[True]) 
    .show(50)
)

spark.stop()

