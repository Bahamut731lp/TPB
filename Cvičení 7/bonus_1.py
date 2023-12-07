from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, current_timestamp, lower, regexp_replace
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

# Inicializace SparkSession
spark = SparkSession.builder.appName("WordCountTxtFileStreaming").getOrCreate()

# Definice schématu pro stream souborů
schema = StructType([StructField("value", StringType()), StructField("timestamp", TimestampType())])

# Definice schématu pro stream souborů
lines = spark.readStream.format("csv") \
    .schema(schema) \
    .option("header", "false") \
    .option("latestFirst", "true") \
    .option("maxFilesPerTrigger", 1) \
    .option("filter", "input_file_name LIKE '%.txt'") \
    .load("/files/data/*")

# Přidání sloupce timestamp
lines_with_timestamp = lines.withColumn("timestamp", current_timestamp())

# Převedení na malá písmena a odstranění interpunkce
cleaned_lines = lines_with_timestamp.withColumn("cleaned_value", regexp_replace(lower(lines_with_timestamp.value), "[^a-z\\s]", ""))

# Rozdělení řádků na slova
words = cleaned_lines.select(explode(split(cleaned_lines.cleaned_value, " ")).alias("word"), "timestamp")

# Definice schématu pro stream
word_counts = words.groupBy("word").count()

# Výpis výsledků na konzoli
query = word_counts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Čekání na ukončení streaming query
query.awaitTermination()
