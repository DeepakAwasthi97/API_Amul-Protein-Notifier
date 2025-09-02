import importlib
import json
from config import SUBSTORE_LIST_FILE
import logging

logger = logging.getLogger(__name__)


def load_substore_mapping():
    spec = importlib.util.spec_from_file_location("substore_list", SUBSTORE_LIST_FILE)
    substore_list = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(substore_list)

    for sub in substore_list.substore_info:
        # Handle _id: deduplicate whether list or comma-string
        if isinstance(sub["_id"], list):
            unique_ids = list(set(sub["_id"]))
        elif isinstance(sub["_id"], str) and "," in sub["_id"]:
            unique_ids = list(
                set(id_.strip() for id_ in sub["_id"].split(",") if id_.strip())
            )
        else:
            unique_ids = [sub["_id"]] if sub.get("_id") else []

        if len(unique_ids) > 1:
            logger.warning(
                f"Multiple unique _ids for alias {sub.get('alias')}: {unique_ids}. Using first."
            )
        sub["_id"] = unique_ids[0] if unique_ids else ""

        # Ensure pincodes is list and deduplicated
        pincodes = sub.get("pincodes", [])
        if isinstance(pincodes, str):
            pincodes = [p.strip() for p in pincodes.split(",") if p.strip()]
        sub["pincodes"] = list(set(pincodes))  # Dedup

    return substore_list.substore_info


def save_substore_mapping(substore_info):
    with open(SUBSTORE_LIST_FILE, "w", encoding="utf-8") as f:
        f.write(
            "substore_info = " + json.dumps(substore_info, indent=4, ensure_ascii=False)
        )
