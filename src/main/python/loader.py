import gzip
import json


def load_data(file_name: str) -> list:
    with gzip.GzipFile(file_name, 'r') as fin:  # 4. gzip
        json_bytes = fin.read()  # 3. bytes (i.e. UTF-8)
    json_str = json_bytes.decode('utf-8')  # 2. string (i.e. JSON)
    data = json.loads(json_str)  # 1. data
    return data['data']

