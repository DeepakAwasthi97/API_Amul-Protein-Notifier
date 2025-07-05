import importlib
import json
from config import SUBSTORE_LIST_FILE

def load_substore_mapping():
    spec = importlib.util.spec_from_file_location("substore_list", SUBSTORE_LIST_FILE)
    substore_list = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(substore_list)
    return substore_list.substore_info

def save_substore_mapping(substore_info):
    with open(SUBSTORE_LIST_FILE, 'w', encoding='utf-8') as f:
        f.write('substore_info = ' + json.dumps(substore_info, indent=4, ensure_ascii=False))
