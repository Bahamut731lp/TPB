# Cvičení 2 - Analýza dat z webu iDNES.cz

## Příprava Elasticsearch instance
Pro jednoduší zpracování dat jsme je nejdřív nasypal do Elasticsearch. Elasticsearch je společně s Kibanou ready pro docker přes `docker-compose`.

```bash
cd '/TPB/Cvičení 2'
docker-compose up
```

Poté je potřeba se dostat do Elasticsearch containeru terminálem, resetovat heslo a vygenerovat enrollment token.

```bash
docker container exec -it <id> bash
elasticsearch@0fb2d8f525a8:~$: bin/elasticsearch-reset-password -i -u elastic
elasticsearch@0fb2d8f525a8:~$: bin/elasticsearch-create-enrollment-token --scope kibana
```

Kibana běží na `http://localhost:5601`, kam musíme dohodit enrollment token vygenerovaný příkazem dříve. Necháme to automaticky nastavit a pak se přihlásíme přes username `elastic` a heslo, který jsme zadali při resetování hesla.

## Nasypání dat do Elasticsearch
O přenos dat z disku do Elasticsearch se stará skript `import2elastic.py`. Tento skript prochází jednotlivé soubory a:
- Vyčistí titulek od stop slov a rozdělí ho na pole slov
- Vyčistí obsah článku od stop slov a rozdělí ho na pole slov
- Přidá link na článek do pole `link`
- Převede počet komentářů a počet fotek na čísla (původně stringy)

Skript postupně vypisuje, kolik souborů již prošel. V mém případě se zastavil někde na 9405.

## Základní
1. Vypište počet článků
```py
cnt = es.count(index=INDEX_NAME)["count"]
table_rows.append(["Počet článků", cnt])
```

2. Vypiště počet duplicitních článků
```py
query = {
    "match_all": {}
}

aggs = {
    "title": {
        "terms": {
            "field": "link.keyword",
            "size": 500000,
            "min_doc_count": 2
        }
    }
}

duplicates = es.search(index=INDEX_NAME, query=query, aggs=aggs)["aggregations"]["title"]["buckets"]
duplicates_ctn = sum([x["doc_count"] - 1 for x in duplicates])
```

3. Vypiště datum nejstaršího článku
```py
query = {
    "match_all": {}
}

aggs = {
    "min_date": {
        "min": {
            "field": "date",
        }
    }
}

oldest = es.search(index=INDEX_NAME, query=query, aggs=aggs)
oldest_date = oldest["aggregations"]["min_date"]["value_as_string"]
```
4. Vypiště jméno článku s nejvíce komentáři
```py
aggs = {
    "doc_with_most_comments": {
        "top_hits": {
            "sort": [
                {
                    "no_of_comments": {
                        "order": "desc"
                    }
                }
            ],
            "size": 1
        }
    }
}

most_comments = es.search(index=INDEX_NAME, aggs=aggs)
most_comments_post = most_comments["aggregations"]["doc_with_most_comments"]["hits"]["hits"][0]["_source"]
title = most_comments_post["title"]
no_of_comments = most_comments_post["no_of_comments"]
```

5. Vypište nejvyšší počet přidaných fotek u článku
```py
aggs = {
    "most_photos": {
        "max": {
            "field": "no_of_photos"
        }
    }
}

most_photos = es.search(index=INDEX_NAME, aggs=aggs)
no_of_photos = most_photos["aggregations"]["most_photos"]["value"]
```

6. Vypište počty článků podle roku publikace
```py
# Konfigurace agregace
aggs = {
    "yearly_publications": {
        "date_histogram": {
            "field": "date",
            "calendar_interval": "year"
        }
    }
}

# Výsledky z databáze
result = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = result["aggregations"]
buckets = aggregations["yearly_publications"]["buckets"]

# Pole s řádky tabulky roků a počtů
rows = []
for bucket in buckets:
    year = bucket["key_as_string"].split("-")[0]
    count = bucket["doc_count"]
    rows.append([year, count])
```

7. Vypiště počet unikátních kategorií a počet článků v každé kategorii
```py
# Nalezení počtu kategorií pro nastavení limitu
aggs = {
    "pocet_kategorii": {
        "cardinality": {
            "field": "category.keyword"
        }
    }
}

query = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = query["aggregations"]
count = aggregations["pocet_kategorii"]["value"]
```

```py
# Počet článků v jednotlivých kategoriích
aggs = {
    "unikatni_kategorie": {
        "terms": {
            "field": "category.keyword",
            "size": count
        }
    }
}

query = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = query["aggregations"]
buckets = aggregations["unikatni_kategorie"]["buckets"]

rows = []
for bucket in buckets:
    category = bucket["key"]
    count = bucket["doc_count"]

    rows.append([key, count])
```


8. Vypište 5 nejčastějších slov v názvu článků z roku 2021
```py
# Vytvořte dotaz s agregací na pole "title"
aggs = {
    "top_words": {
        "terms": {
            "field": "title.keyword",
            "size": 5
        }
    }
}

query = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = query["aggregations"]
most_frequent_words = aggregations["top_words"]["buckets"]
```

9. Vypište celkový počet komentářů
```py
aggs = {
    "comments": {
        "sum": {
            "field": "no_of_comments"
        }
    }
}

query = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = query["aggregations"]
number_of_comments = aggregations["comments"]["value"]
```

10. Vypište celkový počet slov ve všech článcích
```py
aggs = {
    "words": {
        "value_count": {
            "field": "content.keyword"
        }
    }
}

query = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = query["aggregations"]
number_of_words = aggregations["words"]["value"]
```

## Bonus
11. Vypište 8 nejčastějších slov v článcích, odfiltrujte krátká slova (< 6 písmen)

```py
aggs = {
    "top_words": {
        "terms": {
            "field": "content.keyword",
            "size": 8,
            "include": ".{6,}"
        }
    }
}

query = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = query["aggregations"]
buckets = aggregations["top_words"]["buckets"]

rows = []
for bucket in buckets:
    word = bucket["key"]
    count = bucket["doc_count"]

    rows.append([word, count])
```

12. Vypište 3 články s nejvyšším počtem výskytů slova "Covid-19"

```py
query = {
    "term": {
        "content": "covid"
    }
}

scroll = es.search(
    index=INDEX_NAME,
    query=query,
    size=1000,
    scroll="1m"
)
pbar = tqdm(total=0)
covid_counter = Counter({})

while len(scroll['hits']['hits']) > 0:
    for hit in scroll['hits']['hits']:
        content = hit['_source']['content']
        link = hit["_source"]["link"]
        cnt = content.count("covid")

        if cnt > 0:
            covid_counter[link] = cnt

    scroll = es.scroll(scroll_id=scroll['_scroll_id'], scroll="1m")
    pbar.update(len(scroll['hits']['hits']))

# Uzavření zdrojů
pbar.close()
es.clear_scroll(scroll_id=scroll['_scroll_id'])

result = tabulate(covid_counter.most_common(3), tablefmt="simple")
```

13. Vypište články s nejvyšším a nejnižším počtem slov

Protože nevím, jak bych tohodle docílil přes agregace, rozhodl jsem se to vyřešit sekvenčním projitím. Abych to nemusel dělat vícekrát, rovnou jsem tuhle úlohu jsem spojil s úlohou __14, vypište průměrnou délku slova přes všechny články__.
```py
query = {
   "match_all": {}
}

scroll = es.search(
    index=INDEX_NAME,
    query=query,
    size=8000,
    scroll="1m"
)

pbar = tqdm(total=0)
word_counter = Counter({})
index = 0
average = 1

while len(scroll['hits']['hits']) > 0:
    for hit in scroll['hits']['hits']:
        content = hit['_source']['content']
        link = hit["_source"]["link"]
        word_counter[link] = len(content)

        # Rekurentní vzoreček pro průměr délky slova
        for word in content:
            index += 1
            average = ((index - 1) * average + len(word)) / index

    scroll = es.scroll(scroll_id=scroll['_scroll_id'], scroll="1m")
    pbar.update(len(scroll['hits']['hits']))

pbar.close()
es.clear_scroll(scroll_id=scroll['_scroll_id'])

longest = word_counter.most_common(1)[0]
shortest = word_counter.most_common()[-1]

stats = [
    ["Nejdelší článek", f"{longest[0]} ({longest[1]} slov)"],
    ["Nejkratší článek", f"{shortest[0]} ({shortest[1]} slov)"],
    ["Průměrný počet slov na článek", round(average, 2)]
]
```

15. Vypište měsíce s nejvíce a nejméně publikovanými články
```py
aggs = {
    "monthly_publications": {
        "date_histogram": {
            "field": "date",
            "calendar_interval": "month"
        }
    }
}

query = es.search(index=INDEX_NAME, aggs=aggs)
aggregations = query['aggregations']
buckets = ['monthly_publications']["buckets"]

maximum = max(buckets, key=lambda x: x["doc_count"])
minimum = min(buckets, key=lambda x: x["doc_count"])

stats = [
    ["Největší počet publikací", f"{maximum['key_as_string']} ({maximum['doc_count']})"],
    ["Nejmenší počet publikací", f"{minimum['key_as_string']} ({minimum['doc_count']})"]
]
```

## Výsledný report
```
Analýza dat: 100%|█████████████████████████████████████████████████████████████████████████████████████████████████████████| 14/14 [02:21<00:00, 10.11s/it]
------------------------------  -----------------------------------------------------------------------------------------------------------------------------------------------
Počet článků                    321729
Počet duplicitních článků       39
Datum nejstaršího článku        2001-05-08T13:18:45.000Z
Článek s nejvíce komentáři      Zemřel adamec komunista předal havlovi (611811)
Nejvyšší počet fotek            399.0
Publikace podle roku            2001   8932
                                2002   8813
                                2003   9030
                                2004   9461
                                2005   9918
                                2006  10163
                                2007  14584
                                2008  15360
                                2009  17627
                                2010  16411
                                2011  18009
                                2012  17327
                                2013  16711
                                2014  14414
                                2015  15615
                                2016  16121
                                2017  16149
                                2018  16201
                                2019  14634
                                2020  14987
                                2021  15060
                                2022  14583
                                2023  11619
Unikátní kategorie              domaci                    111919
                                zahranicni                 92895
                                krimi                      34203
                                praha-zpravy               11751
                                brno-zpravy                 7681
                                zpr_archiv                  6298
                                zpr_nato                    5918
                                mediahub                    5175
                                ostrava-zpravy              4920
                                plzen-zpravy                4676
                                hradec-zpravy               4425
                                usti-zpravy                 4352
                                olomouc-zpravy              4327
                                budejovice-zpravy           3762
                                pardubice-zpravy            3135
                                vary-zpravy                 3040
                                zlin-zpravy                 2744
                                jihlava-zpravy              2647
                                liberec-zpravy              2496
                                volby                       1145
                                ekonomika                    581
                                eko-zahranicni               383
                                filmvideo                    273
                                lidicky                      203
                                sto-pohledu                  201
                                lide-ceska                   134
                                sw_internet                  111
                                kolem-sveta                   99
                                zdravi                        96
                                hudba                         94
                                automoto                      89
                                volby-ep2019                  88
                                vytvarne-umeni                88
                                brno-volby                    71
                                eko-doprava                   71
                                olomouc-prilohy               71
                                recepty                       62
                                ekoakcie                      61
                                literatura                    58
                                zlin-volby                    56
                                po-cesku                      53
                                hradec-volby                  50
                                usti-volby                    48
                                praha-volby                   47
                                olomouc-volby                 45
                                pardubice-volby               40
                                liberec-volby                 38
                                leto                          37
                                ostrava-volby                 37
                                fotbal                        36
                                audio-foto-video              34
                                mob_tech                      34
                                budejovice-volby              33
                                ona-vztahy                    32
                                divadlo                       31
                                jihlava-volby                 31
                                tec_vesmir                    31
                                digitv                        28
                                tec_technika                  28
                                veda                          27
                                software                      25
                                stoletidnes                   25
                                hobby-mazlicci                24
                                plzen-volby                   23
                                architektura                  22
                                sporty                        21
                                vojenstvi                     19
                                modni-trendy                  18
                                sex                           16
                                metro-praha                   15
                                bw-recenze                    14
                                mobilni-operatori             14
                                show_aktual                   14
                                vary-volby                    14
                                fot_reprez                    11
                                hobby-domov                   11
                                hobby-zahrada                 11
                                zajimavosti                   11
                                hokej                         10
                                prilohy                       10
                                fot_pohary                     9
                                lyze                           9
                                telefony                       9
                                fot_zahranici                  8
                                viteze                         8
                                zlin-sport                     8
                                missamodelky                   7
                                osmnactiny                     7
                                sport_oh                       7
                                tec_reportaze                  7
                                test                           7
                                bw-novinky                     6
                                hokej_ms2004                   6
                                motorsport                     6
                                praha-prilohy                  6
                                stavba                         6
                                tec-kratke-zpravy              6
                                evropa                         5
                                fot_dsouteze                   5
                                podnikani                      5
                                reprezentace                   5
                                sport-golf                     5
                                xman-styl                      5
                                lyzovani                       4
                                tipy-na-vylet                  4
                                vary-sport                     4
                                xman-adrenalin                 4
                                bonusweb                       3
                                bw-magazin                     3
                                hardware                       3
                                ms-fotbal-2022                 3
                                nhl                            3
                                olympiada-tokio-2020           3
                                olympiada-turin                3
                                pardubice-sport                3
                                premium-sluzby                 3
                                sporeni                        3
                                aplikace                       2
                                auto_ojetiny                   2
                                brno-sport                     2
                                dum_osobnosti                  2
                                formule                        2
                                hok_1liga                      2
                                hokej_ms2005                   2
                                jihlava-prilohy                2
                                ms-fotbal-2010                 2
                                olympiada-peking-2022          2
                                ostrava-sport                  2
                                poj                            2
                                poslanecka-snemovna-2021       2
                                praha-sport                    2
                                pred-100-lety                  2
                                prezidentske-volby-2023        2
                                rybareni                       2
                                tenis                          2
                                usti-sport                     2
                                atletika                       1
                                bw-preview                     1
                                cyklistika                     1
                                euro-2020                      1
                                hobby-dilna                    1
                                houby                          1
                                hradec-sport                   1
                                inv                            1
                                ms-hokej-2022                  1
                                na-kolo                        1
                                olympiada-ateny                1
                                olympiada-londyn-2012          1
                                olympiada-peking               1
                                ostrava-prilohy                1
                                panelakovy-byt                 1
                                plzen-sport                    1
                                rady-na-cestu                  1
                                regionalni-zpravy              1
                                rekonstrukce                   1
                                sport-basket                   1
                                sport-mma                      1
                                vary-prilohy                   1
                                volejbal                       1
                                xman-rozhovory                 1
                                zoh-soci-2014                  1
                                zoh-vancouver-2010             1
5 nejčastějších slov v titulku  -------  -----
                                policie  12652
                                soud      9544
                                lidí      7739
                                muž       7292
                                let       7190
                                -------  -----
Celkový počet komentářů         48966612.0
Celkový počet slov              70113146
Top 8 slov > 5                  ---------  ------
                                mluvčí     106685
                                policie     86129
                                například   76099
                                několik     68554
                                uvedla      65732
                                dalších     54254
                                policisté   52084
                                policejní   51391
                                ---------  ------
Články s COVID                  -------------------------------------------------------------------------------------------------------------------------------------------  --
                                https://www.idnes.cz/zpravy/domaci/chcipl-pes-protest-demonstrace-koronavirus-covid-19-zakon.A210608_191509_domaci_klf                       16
                                https://www.idnes.cz/zpravy/domaci/ministerstvo-zdravotnictvi-jan-blatny-umrti-specialni-pracovni-skupina-lekari.A210127_121440_domaci_vank  12
                                https://www.idnes.cz/zpravy/domaci/covid-porody-matky-nemocnice-odebirani-deti-covid-pozitivnim-matkam-blatny.A210325_124203_domaci_lre      12
                                -------------------------------------------------------------------------------------------------------------------------------------------  --
Statistiky článků               -----------------------------  ------------------------------------------------------------------------------------------------------------
                                Nejdelší článek                https://www.idnes.cz/zpravy/domaci/dokument-prohlaseni-vlada-ano-cssd.A180511_070724_domaci_bur (10077 slov)
                                Nejkratší článek               https://www.idnes.cz/zpravy/archiv/test.A060306_154806_eunie_mbb (3 slov)
                                Průměrný počet slov na článek  6.86
                                -----------------------------  ------------------------------------------------------------------------------------------------------------
Měsíční publikace               ------------------------  -------------------------------
                                Největší počet publikací  2011-03-01T00:00:00.000Z (1660)
                                Nejmenší počet publikací  2023-10-01T00:00:00.000Z (542)
                                ------------------------  -------------------------------
------------------------------  -----------------------------------------------------------------------------------------------------------------------------------------------
```