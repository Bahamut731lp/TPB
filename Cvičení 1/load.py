import json
from pathlib import Path
from tqdm.contrib.concurrent import thread_map

def load():
    files = Path("./data").glob('**/*.json')

    data = thread_map(
        get_file_content,
        files,
        max_workers=32,
    )

    return data

def get_file_content(link):
    with open(link, "r", encoding="utf-8") as file_handle:
        return json.load(file_handle)

if __name__ == "__main__":
    load()