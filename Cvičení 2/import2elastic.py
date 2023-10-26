from elasticsearch import Elasticsearch, helpers
from pathlib import Path
import urllib3
import json
import os
import time
import re

INDEX_NAME = 'idnes2'
STOP_WORDS = None

def process(input_string: str):
    return [re.sub(r'[^\w]', '', x.lower()) for x in input_string.split() if x.lower() not in STOP_WORDS]

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
es = Elasticsearch([{'host': 'localhost', 'port': 9200, "scheme": "https"}],
                   basic_auth=('elastic', 'elastic'), verify_certs=False)


with open("./stopwords-cs.json", "r", encoding="utf-8") as file_handle:
    STOP_WORDS = json.load(file_handle)
    print("Načteny Stop Words")

# Kontrola zda existuje index 'person'
if not es.indices.exists(index=INDEX_NAME):
    # Vytvoření indexu
    es.indices.create(index=INDEX_NAME)
    print("Vytvořen index")

for (index, filename) in enumerate(os.listdir(Path("../Cvičení 1/data"))):
    if index < 9076:
        continue

    if filename.endswith('.json'):
        with open(Path("../Cvičení 1/data", filename), "r", encoding="utf-8") as open_file:
            content = json.load(open_file)
            transformed = []

            for key, value in content.items():
                val = value.copy()
                val["title"] = process(val["title"])
                val["content"] = process(val["content"])
                val["link"] = key
                val["no_of_photos"] = int(val["no_of_photos"])
                val["no_of_comments"] = int(val["no_of_comments"])
                

                transformed.append(val)

            print(index)
            helpers.bulk(es, transformed, index=INDEX_NAME)
            time.sleep(0.1)
