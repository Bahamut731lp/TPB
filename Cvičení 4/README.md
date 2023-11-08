# Cvičení 4

## Příprava
- Ve složce cvičení stačí otevřít terminál a spustit příkaz (ujisti se, že ti běží docker služba, jinak to nepůjde)
```bash
docker-compse up -d
```
> Pozor! Soubory se mountnou do kořenového adresáře systému (`/`), ale spark je v `/opt/bitnami/spark/`, takže je s tím potřeba počítat při přístupu k souborům.

## Zkouška, že všechno funguje
- viz slidy v [zadání](zadání.pdf)

## Úkoly
### Základní
#### 1. Zjistěte teplotní maxima pro akždou meteostanici v roce 1800

```diff
lines = sc.textFile("/files/data/1800.csv")
parsedLines = lines.map(parseLine)
- minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
+ maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
- minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))
+ maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))
- results = minTemps.collect()
+ results = maxTemps.collect()

for result in results:
-    print(result[0] + "\t{:.2f}F".format(result[1])
+    print(result[0] + "\t{:.2f}C".format((result[1] - 32) / 1.8))
```



```diff
I have no name!@25f56b47c366:/opt/bitnami/spark$ spark-submit /files/uloha_1.py

ITE00100554     32.30F
EZE00100082     32.30F
```

#### 2. Zjistěte počet výskytů jednotlivých slov v textovém souboru
- K dispozici máte soubory _book.txt_ (data) a _word-count.py_ (kód)
- Vhodně rozšiřte kód z přednášky
    - Jednotlivá slova normalizujte za pomocí regulárních výrazů
        - Převod na malá písmena a odstranění interpunkce
        - V praxi by bylo možné využít např. toolkit NLTK
    - Výsledky vraťte setříděné od slov s nejvyšším počtem výskytů
    - Vypište 20 nejčastějších slov

```
I have no name!@25f56b47c366:/opt/bitnami/spark$ spark-submit /files/uloha_2.py

to:             1801
your:           1416
you:            1415
the:            1282
a:              1187
of:             960
and:            923
that:           662
in:             594
is:             549
for:            522
are:            414
if:             411
on:             401
be:             358
it:             357
as:             343
can:            340
i:              322
have:           309
```

#### 3. Zjistěte celkovou výši objednávek pro každého zákazníka
- K dispozici máte soubor `customer-orders.csv`
- Vytvořte skript vracející pro každého zákazníka celkovou utracenou částku

```py
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
```

```
I have no name!@25f56b47c366:/opt/bitnami/spark$ spark-submit /files/uloha_3.py

68:             6375.45
73:             6206.20
39:             6193.11
54:             6065.39
71:             5995.66
2:              5994.59
97:             5977.19
46:             5963.11
42:             5696.84
59:             5642.89
41:             5637.62
0:              5524.95
8:              5517.24
85:             5503.43
61:             5497.48
32:             5496.05
58:             5437.73
63:             5415.15
15:             5413.51
6:              5397.88
92:             5379.28
43:             5368.83
70:             5368.25
72:             5337.44
34:             5330.80
9:              5322.65
55:             5298.09
90:             5290.41
64:             5288.69
93:             5265.75
24:             5259.92
33:             5254.66
62:             5253.32
26:             5250.40
52:             5245.06
87:             5206.40
40:             5186.43
35:             5155.42
11:             5152.29
65:             5140.35
69:             5123.01
81:             5112.71
19:             5059.43
25:             5057.61
60:             5040.71
17:             5032.68
29:             5032.53
22:             5019.45
28:             5000.71
30:             4990.72
16:             4979.06
51:             4975.22
1:              4958.60
53:             4945.30
18:             4921.27
27:             4915.89
86:             4908.81
76:             4904.21
38:             4898.46
95:             4876.84
89:             4851.48
20:             4836.86
88:             4830.55
10:             4819.70
4:              4815.05
82:             4812.49
31:             4765.05
44:             4756.89
7:              4755.07
37:             4735.20
14:             4735.03
80:             4727.86
21:             4707.41
56:             4701.02
66:             4681.92
12:             4664.59
3:              4659.63
84:             4652.94
74:             4647.13
91:             4642.26
83:             4635.80
57:             4628.40
5:              4561.07
78:             4524.51
50:             4517.27
67:             4505.79
94:             4475.57
49:             4394.60
48:             4384.33
13:             4367.62
77:             4327.73
47:             4316.30
98:             4297.26
36:             4278.05
75:             4178.50
99:             4172.29
23:             4042.65
96:             3924.23
79:             3790.57
45:             3309.38
```

### Bonus

#### 4. Skript zjišťující počet výskytů jednotlivých slov v textovém souboru aplikujte na texty stažené z portálu iDNES.cz


#### 5. Seznam z úlohy 3 vraťte setříděny podle celkové utracené částky (Pro setřízení využijte RDD)

```py
sums_sorted = sums
.map(lambda x: (x[1], x[0]))    # Prohození klíče a hodnoty, takže z toho je (částka, zákazník)
.sortByKey(keyfunc= lambda k: -k)   # Seřazení podle klíče, keyfunc říká, že se to má řadit podle funkce k -> -k, takže sestupně
.map(lambda x: (x[1], x[0]))    # Prohození zpátky do tvaru (zákazník, částka)
```