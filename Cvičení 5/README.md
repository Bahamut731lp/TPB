# Cvičení 5

## Úkoly
### Základní
#### Úloha 1
```py
people
.groupBy("age")
.avg("friends")
.withColumnRenamed("avg(friends)", "friends")
.withColumn("friends", functions.format_number("friends", 2))
.sort("age", ascending=[True]) 
.show(50)
```

```
+---+-------+
|age|friends|
+---+-------+
| 18| 343.38|
| 19| 213.27|
| 20| 165.00|
| 21| 350.88|
| 22| 206.43|
| 23| 246.30|
| 24| 233.80|
| 25| 197.45|
| 26| 242.06|
| 27| 228.12|
| 28| 209.10|
| 29| 215.92|
| 30| 235.82|
| 31| 267.25|
| 32| 207.91|
| 33| 325.33|
| 34| 245.50|
| 35| 211.62|
| 36| 246.60|
| 37| 249.33|
| 38| 193.53|
| 39| 169.29|
| 40| 250.82|
| 41| 268.56|
| 42| 303.50|
| 43| 230.57|
| 44| 282.17|
| 45| 309.54|
| 46| 223.69|
| 47| 233.22|
| 48| 281.40|
| 49| 184.67|
| 50| 254.60|
| 51| 302.14|
| 52| 340.64|
| 53| 222.86|
| 54| 278.08|
| 55| 295.54|
| 56| 306.67|
| 57| 258.83|
| 58| 116.55|
| 59| 220.00|
| 60| 202.71|
| 61| 256.22|
| 62| 220.77|
| 63| 384.00|
| 64| 281.33|
| 65| 298.20|
| 66| 276.44|
| 67| 214.62|
+---+-------+
only showing top 50 rows
```

#### Úloha 2

```py
customers
.withColumnRenamed("_c0", "customer_id")
.withColumnRenamed("_c1", "item_id")
.withColumnRenamed("_c2", "price")
.groupBy("customer_id")
.agg(functions.sum("price").alias("total_spent"))
.withColumn("total_spent", functions.format_number("total_spent", 2))
.sort("total_spent", ascending=[False])
.show()
```

```
+-----------+-----------+
|customer_id|total_spent|
+-----------+-----------+
|         68|   6,375.45|
|         73|   6,206.20|
|         39|   6,193.11|
|         54|   6,065.39|
|         71|   5,995.66|
|          2|   5,994.59|
|         97|   5,977.19|
|         46|   5,963.11|
|         42|   5,696.84|
|         59|   5,642.89|
|         41|   5,637.62|
|          0|   5,524.95|
|          8|   5,517.24|
|         85|   5,503.43|
|         61|   5,497.48|
|         32|   5,496.05|
|         58|   5,437.73|
|         63|   5,415.15|
|         15|   5,413.51|
|          6|   5,397.88|
+-----------+-----------+
```

#### Úloha 3
```py
connections = (
    lines.withColumn("id", functions.split(functions.trim(functions.col("value")), " ")[0])
    .withColumn("connections", functions.size(functions.split(functions.trim(functions.col("value")), " ")) - 1)
    .groupBy("id")
    .agg(functions.sum("connections").alias("connections"))
)

most_obsure_heroes = (
    connections
    .where("connections = 1")
    .join(names, connections.id == names.id)
    .sort("name", ascending=[True])
    .show()
)
```

```
+----+-----------+--------------------+
|  id|connections|                name|
+----+-----------+--------------------+
| 306|          1|              AZRAEL|
| 620|          1|          BLOWTORCH/|
| 869|          1|        CAPTAIN FATE|
| 982|          1|           CHAKRA II|
|1073|          1|   CLASS CLOWN/GLASS|
|1167|          1|        COOPER, TERI|
|1200|          1|  COVEY, LAWRENCE K.|
|1311|          1|            D'SPRYTE|
|1460|          1|             DESADIA|
|1615|          1|      DRAGO, VALERIE|
|1684|          1|              EDITOR|
|1726|          1|ELLINGTON, DR./TRAVI|
|1840|          1| FELDSTADT, DR. HANS|
|1877|          1|            FIREBOLT|
|1890|          1|          FIREFLY II|
|1945|          1|          FONG, KATY|
|2042|          1|GAMBIT DOPPELGANGER |
|2072|          1|      GARNOK REBBAHN|
|2154|          1|            GLORIOLE|
|2176|          1|            GOLEM II|
+----+-----------+--------------------+
only showing top 20 rows
```

### Bonus

#### Úloha 1
```py
.agg(functions.sum("price").alias("total_spent"))
.withColumn("total_spent", functions.format_number("total_spent", 2))
.sort("total_spent", ascending=[False])
```

#### Úloha 2
```py
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
```