from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml import Pipeline

SPARK_URL = "spark://b4d3a732849f:7077"

spark = SparkSession.builder.master(SPARK_URL).appName("RealEstatePrediction").getOrCreate()
# No,TransactionDate,HouseAge,DistanceToMRT,NumberConvenienceStores,Latitude,Longitude,PriceOfUnitArea
data = spark.read.csv("/files/data/realestate.csv", header=True, inferSchema=True)
feature_columns = ["HouseAge", "DistanceToMRT", "NumberConvenienceStores"]
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
dt = DecisionTreeRegressor(featuresCol="features", labelCol="PriceOfUnitArea")
pipeline = Pipeline(stages=[assembler, dt])
(train_data, test_data) = data.randomSplit([0.9, 0.1], seed=42)

model = pipeline.fit(train_data)
predictions = model.transform(test_data)
predictions.select("prediction", "PriceOfUnitArea").show(50)
