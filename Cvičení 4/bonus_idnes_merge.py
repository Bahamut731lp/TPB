from pathlib import Path
import json
from functools import reduce
from tqdm.contrib.concurrent import thread_map

def load():
    files = list(Path("../Cvičení 1/data").glob('**/*.json'))

    data = thread_map(
        get_file_content,
        files,
        max_workers=32,
    )

    with open("./data/idnes.txt", "w", encoding="utf-8") as file_handle:
        content = "\n".join(list(reduce(lambda x, y: x + y, data)))
        file_handle.write(content)

    return data

def get_file_content(link):
    with open(link, "r", encoding="utf-8") as file_handle:
        content = json.load(file_handle)
        lines = []

        for (link, data) in content.items():
            data["link"] = link
            lines.append(
                json.dumps(data, ensure_ascii=False)
            )

        return lines
    
if __name__ == "__main__":
    load()