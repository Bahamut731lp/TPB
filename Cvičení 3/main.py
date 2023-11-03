from collections import Counter
import json
from tqdm import tqdm
from pathlib import Path
import matplotlib.pyplot as plt
from tqdm.contrib.concurrent import thread_map
from datetime import datetime
from dateutil import parser
import re
import calendar
import pytz

# Souběh?
# Never heard of it.

tz = pytz.timezone('Europe/Prague')

STOP_WORDS = []
uloha_1 = []
uloha_2 = {}
uloha_3 = {}
uloha_4 = {}
uloha_5 = []
uloha_6 = {}
uloha_7 = {}
uloha_8 = {}

with open("../Cvičení 2/stopwords-cs.json", "r", encoding="utf-8") as file_handle:
    STOP_WORDS = json.load(file_handle)
    print("Načteny Stop Words")

def process(input_string: str):
    return [re.sub(r'[^\w]', '', x.lower()) for x in input_string.split() if x.lower() not in STOP_WORDS]

def load():
    files = list(Path("../Cvičení 1/data").glob('**/*.json'))

    data = thread_map(
        analyze,
        files,
        max_workers=32,
        total=9401
    )

    return data

def analyze(link):
    with open(link, "r", encoding="utf-8") as file_handle:
        content = json.load(file_handle)

        for link, article in content.items():
            date = parser.isoparse(article["date"]).replace(tzinfo=tz)
            words = process(article["content"])
            title = process(article["title"])
            den_tydne = date.weekday()
            dam_si_gambrinus = title.count("koronavirus")
            vak_z_cinu = title.count("vakcína")

            if date.year not in uloha_2:
                uloha_2[date.year] = 0

            if date.date() not in uloha_7:
                uloha_7[date.date()] = {
                    "covid": 0,
                    "vax": 0
                }

            if article["category"] not in uloha_4:
                uloha_4[article["category"]] = 0

            if den_tydne not in uloha_8:
                uloha_8[den_tydne] = 0


            if len(words) not in uloha_3:
                uloha_3[len(words)] = int(article["no_of_comments"])
            else:
                uloha_3[len(words)] += int(article["no_of_comments"])

            uloha_1.append(date)
            uloha_2[date.year] += 1
            uloha_4[article["category"]] += 1
            uloha_5.append(len(words))
            
            uloha_7[date.date()]["covid"] += dam_si_gambrinus
            uloha_7[date.date()]["vax"] += vak_z_cinu

            uloha_8[den_tydne] += 1

            for x in words:
                length = len(x)
                if length not in uloha_6:
                    uloha_6[length] = 0

                uloha_6[length] += 1

if __name__ == "__main__":
    load()
    
    uloha_1 = {k: v for v, k in enumerate(sorted(uloha_1))}
    plt.style.use('rose-pine')

    px = 1/plt.rcParams['figure.dpi']  # pixel in inches
    plt.figure(figsize=(1280*px, 720*px))

    # Úloha 1
    plt.plot(uloha_1.keys(), uloha_1.values())
    plt.title("Úloha 1")
    plt.xlabel("Čas")
    plt.ylabel("Kumulativní počet článků")
    plt.savefig('export/uloha_1.png')
    plt.clf()

    # Úloha 2
    plt.bar(uloha_2.keys(), uloha_2.values())
    plt.title("Úloha 2")
    plt.xlabel("Roky")
    plt.ylabel("Počet článků")
    plt.savefig('export/uloha_2.png')
    plt.clf()

    # Úloha 3
    plt.scatter(uloha_3.keys(), uloha_3.values())
    plt.title("Úloha 3")
    plt.xlabel("Délka článku")
    plt.ylabel("Počet komentářů")
    plt.savefig('export/uloha_3.png')
    plt.clf()

    # Úloha 4
    plt.pie(uloha_4.values())
    plt.title("Úloha 4")
    plt.legend(uloha_4.keys(), bbox_to_anchor=(1,0), loc="lower right", bbox_transform=plt.gcf().transFigure)
    plt.savefig('export/uloha_4.png')
    plt.clf()

    # Úloha 5
    plt.hist(uloha_5)
    plt.title("Úloha 5")
    plt.xlabel("Počet slov v článcích")
    plt.ylabel("Četnost")
    plt.savefig('export/uloha_5.png')
    plt.clf()

    # Úloha 6
    plt.hist(uloha_6.values())
    plt.title("Úloha 6")
    plt.xlabel("Délka slov v článcích")
    plt.ylabel("Četnost")
    plt.savefig('export/uloha_6.png')
    plt.clf()

    # Úloha 7
    uloha_7 = dict(sorted(uloha_7.items(), key=lambda x: x[0]))

    plt.plot(uloha_7.keys(), [x["covid"] for x in uloha_7.values()], label="Výskyty slova covid")
    plt.title("Úloha 7")
    plt.xlabel("Čas")
    plt.ylabel("Počet výskytů v titulku")
    plt.plot(uloha_7.keys(), [x["vax"] for x in uloha_7.values()], label="Výskyty slova vakcína")
    plt.savefig('export/uloha_7.png')
    plt.clf()
    
    uloha_8 = dict(sorted(uloha_8.items(), key=lambda x: x[0]))
    # Úloha 8
    plt.bar([calendar.day_name[x] for x in uloha_8.keys()], uloha_8.values())
    plt.title("Úloha 8")
    plt.xlabel("Den v týdnu")
    plt.ylabel("Počet článků")
    plt.savefig('export/uloha_8.png')
    plt.clf()