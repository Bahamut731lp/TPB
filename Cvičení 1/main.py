"""
    Cvičení 1: Scrapování dat z IDNES.cz

    - Navrhněte algoritmus, který bude postupně procházet články webového portálu IDNES.cz
    - Ke každému článku uloží:
        - název článku
        - obsah článku
        - kategorii
        - počet fotografií
        - datum publikace
        - počet komentářů
    - Informace bude ukládat do textového souboru ve formátu JSON
    - Cílem je stáhnout alespoň 250MB textových dat
    - BONUS: Stáhněte minimálně 1GB textových dat
"""
import json
import random
import re
import time
import uuid
from pathlib import Path

import requests
from bs4 import BeautifulSoup
from tqdm import tqdm
from tqdm.contrib.concurrent import thread_map

def load_visited_links():
    files = Path("./data").glob('**/*.json')
    links = []

    for file in files:
        with open(file, "r", encoding="utf-8") as file_handle:
            content = json.load(file_handle)
            links += content.keys()

    return links

def get_data_folder_size():
    return sum(f.stat().st_size for f in Path("./data").glob('**/*') if f.is_file())

def main():
    url = "https://www.idnes.cz/zpravy/archiv/"
    index = 9415
    target = 1e9 + 1e6
    visited_links = load_visited_links()
    current_size = get_data_folder_size()

    pbar = tqdm(total=target)

    while True:
        index += 1
        x = requests.get(url + str(index), timeout=5000)
        soup = BeautifulSoup(x.text, features="html.parser")

        print(f"Stránka archivu: {index}")
        articles = soup.select("#list-art-count .art .art-link")
        links = [
            x.get("href")
            for x in articles
        ]

        is_any_visited = any([
            x in visited_links
            for x in links
        ])

        if is_any_visited:
            time.sleep(0.2)
            continue

        data = thread_map(
            get_article_data,
            links,
            max_workers=32,
        )

        json_file = None
        while True:
            json_file = Path("./data", str(uuid.uuid4()) + ".json")
            if not json_file.exists():
                break

        file_content = dict([x for x in data if x is not None])

        if not file_content:
            continue

        with open(json_file, "w", encoding="utf-8") as file_handle:
            json.dump(
                file_content,
                file_handle,
                indent=4,
                ensure_ascii=False
            )

        current_size = get_data_folder_size()
        pbar.update(current_size)

    pbar.close()


def get_article_data(url: str):
    """
        Funkce pro scrape jednoho článku
    """

    time.sleep(random.randint(0, 5))
    try:
        x = requests.get(url, timeout=5000)
        soup = BeautifulSoup(x.text, features="html.parser")
    except:
        return None

    is_article = False
    try:
        is_article = soup.select_one("meta[name*='cXenseParse:qiw-typobsahu']").get("content") == "article"
    except:
        pass

    if not is_article:
        return None

    is_free = False
    try:
        is_free = soup.select_one("meta[name*='cXenseParse:qiw-content']").get("content") == "free"
    except:
        pass
    
    if not is_free:
        return None

    try:
        title = soup.select_one("h1[itemprop*=name]").text
        date = soup.select_one(".time-date").get("content")

        gallery_count = soup.select_one(".more-gallery b")
        single_images = soup.find_all(class_='opener-foto')
        no_of_photos = gallery_count.text if gallery_count else len(single_images)
        category = soup.select_one("meta[name*='cXenseParse:qiw-rubrika']").get("content")
        comment_element = soup.select_one(".community-discusion span")
        no_of_comments = re.sub("[^0-9]", "", comment_element.text) if comment_element else 0
        perex = soup.select_one(".opener").text.strip()
        content = soup.select_one("#art-text").text.strip().encode('utf-8','ignore').decode("utf-8")

        return (url, {
            "title": title,
            "category": category,
            "content": perex + content,
            "no_of_photos": no_of_photos,
            "date": date,
            "no_of_comments": no_of_comments
        })
    except:
        return None

if __name__ == "__main__":
    main()
