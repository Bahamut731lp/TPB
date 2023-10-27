from collections import Counter
from elasticsearch import Elasticsearch
import urllib3
import json
from tabulate import tabulate
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

INDEX_NAME = 'idnes2'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "https"}],
                   basic_auth=('elastic', 'elastic'), verify_certs=False, timeout=90)

# Kontrola zda existuje index
if not es.indices.exists(index=INDEX_NAME):
    raise LookupError(f"Index {INDEX_NAME} neexistuje")

# Inicializace tabulky
table_rows = []

# Počet článků

def get_document_count():
    
    cnt = es.count(index=INDEX_NAME)["count"]
    return (["Počet článků", cnt])


# Počet duplicitních článků
def get_duplicate_count():
    

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

    duplicates = es.search(index=INDEX_NAME, query=query, aggs=aggs)[
        "aggregations"]["title"]["buckets"]
    duplicates_ctn = sum([x["doc_count"] - 1 for x in duplicates])
    return ["Počet duplicitních článků", duplicates_ctn]

# Vypiště datum nejstaršího článku


def get_oldest_date():
    

    aggs = {
        "min_date": {
            "min": {
                "field": "date",
            }
        }
    }

    oldest = es.search(index=INDEX_NAME, aggs=aggs)
    oldest_date = oldest["aggregations"]["min_date"]["value_as_string"]
    return ["Datum nejstaršího článku", oldest_date]

# Vypiště jméno článku s nejvíce komentáři


def get_most_commented_article():
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
    return ["Článek s nejvíce komentáři", f"{' '.join(title).capitalize()} ({no_of_comments})"]

# Vypište nejvyšší počet přidaných fotek u článku


def get_highest_photo_count():
    aggs = {
        "most_photos": {
            "max": {
                "field": "no_of_photos"
            }
        }
    }

    most_photos = es.search(index=INDEX_NAME, aggs=aggs)
    no_of_photos = most_photos["aggregations"]["most_photos"]["value"]
    return ["Nejvyšší počet fotek", no_of_photos]

# Vypište počty článků podle roku publikace


def get_yearly_publications():
    aggs = {
        "yearly_publications": {
            "date_histogram": {
                "field": "date",
                "calendar_interval": "year"
            }
        }
    }

    result = es.search(index=INDEX_NAME, aggs=aggs)
    aggregations = result["aggregations"]
    buckets = aggregations["yearly_publications"]["buckets"]

    # Pole s řádky tabulky roků a počtů
    rows = []
    for bucket in buckets:
        year = bucket["key_as_string"].split("-")[0]
        count = bucket["doc_count"]
        rows.append([year, count])

    return ["Publikace podle roku", tabulate(rows, tablefmt="plain")]

# Vypiště počet unikátních kategorií a počet článků v každé kategorii


def get_unique_categories():
    # Nalezení počtu kategorií pro nastavení limitu
    aggs = {
        "pocet_kategorii": {
            "cardinality": {
                "field": "category.keyword"
            }
        }
    }
    unique_cats_query = es.search(index=INDEX_NAME, aggs=aggs)
    no_of_uniq_cats = unique_cats_query["aggregations"]["pocet_kategorii"]["value"]

    # Počet článků v jednotlivých kategoriích
    aggs = {
        "unikatni_kategorie": {
            "terms": {
                "field": "category.keyword",
                "size": no_of_uniq_cats
            }
        }
    }

    categories_aggregation = es.search(
        index=INDEX_NAME, aggs=aggs)["aggregations"]

    cats = []
    for bucket in categories_aggregation["unikatni_kategorie"]["buckets"]:
        cats.append([bucket["key"], bucket["doc_count"]])

    return ["Unikátní kategorie", tabulate(cats, tablefmt="plain")]



# Vypište 5 nejčastějších slov v názvu článků z roku 2021
def get_top_five_words():
    # Vytvořte dotaz s agregací na pole "title"
    aggs = {
        "top_words": {
            "terms": {
                "field": "title.keyword",  # Použijte .keyword pro neproanalyzované názvy článků
                "size": 5  # Získat 5 nejčastějších slov
            }
        }
    }

    # Proveďte dotaz
    result = es.search(index=INDEX_NAME, aggs=aggs)

    # Získejte nejčastější slova z výsledků agregace
    top_words = result["aggregations"]["top_words"]["buckets"]

    return ["5 nejčastějších slov v titulku", tabulate(top_words, tablefmt="simple")]



# Vypište celkový počet komentářů
def get_number_of_comments_overall():
    

    aggs = {
        "comments": {
            "sum": {
                "field": "no_of_comments"
            }
        }
    }

    sum_comments_query = es.search(index=INDEX_NAME, aggs=aggs)
    return ["Celkový počet komentářů", sum_comments_query["aggregations"]["comments"]["value"]]



# Vypište celkový počet slov
def get_number_of_words_overall():
    aggs = {
        "words": {
            "value_count": {
                "field": "content.keyword"
            }
        }
    }

    sum_words_query = es.search(index=INDEX_NAME, aggs=aggs)
    return ["Celkový počet slov", sum_words_query["aggregations"]["words"]["value"]]


def get_top_eight_long_words():
    aggs = {
        "top_words": {
            "terms": {
                "field": "content.keyword",
                "size": 8,
                "include": ".{6,}"
            }
        }
    }

    top_eight_words_query = es.search(index=INDEX_NAME, aggs=aggs)
    top_words = []
    for bucket in top_eight_words_query["aggregations"]["top_words"]["buckets"]:
        top_words.append([bucket["key"], bucket["doc_count"]])

    return ["Top 8 slov > 5", tabulate(top_words)]


def get_articles_with_covid():
    query = {
        "term": {
            "content": "covid"
        }
    }

    scroll = es.search(index=INDEX_NAME, query=query, size=1000, scroll="1m")
    pbar = tqdm(total=0)
    covid_counter = Counter({})

    while len(scroll['hits']['hits']) > 0:
        for hit in scroll['hits']['hits']:
            # Zde zpracujte výsledky podle potřeby
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
    return ["Články s COVID", result]


def get_content_length_extremes():
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

    return ["Statistiky článků", tabulate(stats, tablefmt="simple")]

def get_monthly_publishing_extremes():
    aggs = {
        "monthly_publications": {
            "date_histogram": {
                "field": "date",
                "calendar_interval": "month"
            }
        }
    }

    # Proveďte dotaz
    result = es.search(index=INDEX_NAME, aggs=aggs)

    # Získání výsledků agregace
    aggregation_result = result['aggregations']['monthly_publications']["buckets"]

    maximum = max(aggregation_result, key=lambda x: x["doc_count"])
    minimum = min(aggregation_result, key=lambda x: x["doc_count"])

    stats = [
        ["Největší počet publikací", f"{maximum['key_as_string']} ({maximum['doc_count']})"],
        ["Nejmenší počet publikací", f"{minimum['key_as_string']} ({minimum['doc_count']})"]
    ]

    return ["Měsíční publikace", tabulate(stats)]

functions = [
    get_document_count,
    get_duplicate_count,
    get_oldest_date,
    get_most_commented_article,
    get_highest_photo_count,
    get_yearly_publications,
    get_unique_categories,
    get_top_five_words,
    get_number_of_comments_overall,
    get_number_of_words_overall,
    get_top_eight_long_words,
    get_articles_with_covid,
    get_content_length_extremes,
    get_monthly_publishing_extremes
]

table_rows = thread_map(
    lambda x: x(),
    functions,
    desc="Analýza dat"
)

print(tabulate(table_rows, tablefmt="simple"))
