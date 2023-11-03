from elasticsearch import Elasticsearch
from datetime import datetime
from tabulate import tabulate
import urllib3

INDEX_NAME = 'idnes2'
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "http"}],
                   basic_auth=('elastic', 'elastic'), verify_certs=False, timeout=90)

# Kontrola zda existuje index
if not es.indices.exists(index=INDEX_NAME):
    raise LookupError(f"Index {INDEX_NAME} neexistuje")

query = {
    "function_score": {
            "query" : { "match_all": {} },
            "random_score": {}
    }
}

random_link = es.search(index=INDEX_NAME, query=query)["hits"]["hits"][0]["_source"]["link"]
print(random_link)

number_of_articles = es.count(index=INDEX_NAME)["count"]
print(number_of_articles)

aggs = {
    "pocet_fotek": {
        "avg": {
            "field": "no_of_photos"
        }
    }
}

avg_photos = es.search(index=INDEX_NAME, aggs=aggs)["aggregations"]["pocet_fotek"]["value"]
print(avg_photos)

aggs = {
    "komentovany": {
        "range": {
            "field": "no_of_comments",
            "ranges": [
                { "from": 100.0 }
            ]
        }
    }
}

result = es.search(index=INDEX_NAME, aggs=aggs)["aggregations"]["komentovany"]["buckets"][0]["doc_count"]
print(result)

aggs = {
    "pocet_kategorii": {
        "cardinality": {
            "field": "category.keyword"
        }
    }
}

unique_cats_query = es.search(index=INDEX_NAME, aggs=aggs)
no_of_uniq_cats = unique_cats_query["aggregations"]["pocet_kategorii"]["value"]

aggs = {
    "unikatni_kategorie": {
        "date_range": {
            "field": "date",
            "format": "yyyy-MM-dd'T'HH:mm:ss",
            "ranges": [
                {
                    "from": "2022-01-01T00:00:00",
                    "to": "2022-12-31T00:00:00"
                }
            ]
        },
        "aggs": {
            "pocet_clanku": {
                "terms": {
                    "field": "category.keyword",
                    "size": no_of_uniq_cats
                }
            }
        }
    }
}

categories_aggregation = es.search(index=INDEX_NAME, aggs=aggs)["aggregations"]
cats = []
for bucket in categories_aggregation["unikatni_kategorie"]["buckets"][0]["pocet_clanku"]["buckets"]:
    cats.append([bucket["key"], bucket["doc_count"]])


print(tabulate(cats))