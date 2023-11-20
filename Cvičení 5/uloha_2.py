from pyspark.sql import SparkSession
import pyspark.sql.functions as functions

spark = SparkSession.builder.master("spark://17f2d0a5a197:7077").appName("SparkSQL").getOrCreate()

customers = spark.read.option("header", "false").option("inferSchema", "true").csv("/files/data/customer-orders.csv")
customers.printSchema()
(
    customers
    .withColumnRenamed("_c0", "customer_id")
    .withColumnRenamed("_c1", "item_id")
    .withColumnRenamed("_c2", "price")
    .groupBy("customer_id")
    .agg(functions.sum("price").alias("total_spent"))
    .withColumn("total_spent", functions.format_number("total_spent", 2))
    .sort("total_spent", ascending=[False])
    .show()
)

spark.stop()

