from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, window, current_timestamp, col


# Inicializace SparkSession
spark = SparkSession.builder.appName("WordCountStreaming").getOrCreate()

# Definice schématu pro stream
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 9999).load()

# Přidání sloupce timestamp
lines_with_timestamp = lines.withColumn("timestamp", current_timestamp())

# Rozdělení řádků na slova
words = lines_with_timestamp.select(explode(split(lines_with_timestamp.value, " ")).alias("word"), "timestamp")

# Definice schématu pro stream s oknem
windowed_words = words.groupBy(window(col("timestamp"), "30 seconds", "15 seconds"), "word").count()

# Přidání sloupců začátku a konce okna
windowed_words = windowed_words.withColumn("window_start", col("window.start")).withColumn("window_end", col("window.end"))

# Setřídění výsledků podle začátku okna, konce okna a četnosti
sorted_windowed_words = windowed_words.orderBy("window_start", "window_end", "word", "count")

# Výpis výsledků na konzoli
query = sorted_windowed_words \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

# Čekání na ukončení streaming query
query.awaitTermination()

while True:
    spark.sql("SELECT * FROM counts").show()
    time.sleep(1)
