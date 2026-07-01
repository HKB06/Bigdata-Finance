"""Couche Silver : transformation de `enterprise_finale` vers `enterprise_silver`.

On ne modifie jamais enterprise_finale (couche Bronze). On construit une
nouvelle collection nettoyee :

1. StartDate DD-MM-YYYY -> YYYY-MM-DD (comparaisons de dates possibles) ;
2. deduplication des activites (meme NaceCode exact + meme Classification) ;
3. adresse unique : on ne garde que TypeOfAddress = REGO ;
4. denomination principale (TypeOfDenomination = 1) placee en premier ;
5. decodage des codes -> labels FR via code.csv
   (JuridicalFormLabel, StatusLabel, activities[].NaceLabel).
"""

from __future__ import annotations

import csv
import re
from datetime import datetime, timezone
from typing import Optional

from pymongo import UpdateOne

from . import config
from . import mongo_utils

BATCH_SIZE = 2000


# --------------------------------------------------------------------------
# Table de decodage des codes (code.csv)
# --------------------------------------------------------------------------
def load_code_labels(language: str = "FR") -> dict[tuple[str, str], str]:
    """Charge {(Category, Code): Description} pour une langue donnee."""
    path = config.KBO_FILES["code"]
    labels: dict[tuple[str, str], str] = {}
    if not path.exists():
        return labels
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        for row in csv.DictReader(f):
            if (row.get("Language") or "").upper() != language.upper():
                continue
            labels[(row.get("Category"), row.get("Code"))] = row.get("Description")
    return labels


# --------------------------------------------------------------------------
# Transformations unitaires
# --------------------------------------------------------------------------
def normalize_date(value: Optional[str]) -> Optional[str]:
    """DD-MM-YYYY -> YYYY-MM-DD (renvoie la valeur telle quelle si illisible)."""
    if not value:
        return value
    match = re.match(r"^\s*(\d{2})-(\d{2})-(\d{4})\s*$", value)
    if not match:
        return value
    day, month, year = match.groups()
    return f"{year}-{month}-{day}"


def dedupe_activities(activities: list[dict]) -> list[dict]:
    """Supprime les vrais doublons (NaceCode + Classification identiques)."""
    seen: set[tuple] = set()
    result: list[dict] = []
    for activity in activities or []:
        key = (activity.get("NaceCode"), activity.get("Classification"))
        if key in seen:
            continue
        seen.add(key)
        result.append(activity)
    return result


def keep_rego_address(addresses: list[dict]) -> list[dict]:
    """Ne garde que le siege social enregistre (TypeOfAddress = REGO)."""
    return [a for a in (addresses or []) if a.get("TypeOfAddress") == "REGO"]


def order_denominations(denominations: list[dict]) -> list[dict]:
    """Place la denomination officielle (TypeOfDenomination = 1) en premier."""
    return sorted(
        denominations or [],
        key=lambda d: 0 if str(d.get("TypeOfDenomination")) == "1" else 1,
    )


def _nace_label(labels: dict, code: Optional[str]) -> Optional[str]:
    if not code:
        return None
    for category in ("Nace2008", "Nace2025", "Nace2003"):
        label = labels.get((category, code))
        if label:
            return label
    return None


def to_silver(doc: dict, labels: dict) -> dict:
    activities = dedupe_activities(doc.get("activities", []))
    for activity in activities:
        activity["NaceLabel"] = _nace_label(labels, activity.get("NaceCode"))

    return {
        "_id": doc["_id"],
        "EnterpriseNumber": doc.get("EnterpriseNumber"),
        "Status": doc.get("Status"),
        "StatusLabel": labels.get(("Status", doc.get("Status"))),
        "JuridicalForm": doc.get("JuridicalForm"),
        "JuridicalFormLabel": labels.get(("JuridicalForm", doc.get("JuridicalForm"))),
        "TypeOfEnterprise": doc.get("TypeOfEnterprise"),
        "StartDate": normalize_date(doc.get("StartDate")),
        "denominations": order_denominations(doc.get("denominations", [])),
        "address": (keep_rego_address(doc.get("addresses", [])) or [None])[0],
        "activities": activities,
        "silver_at": datetime.now(timezone.utc),
    }


def build_silver(limit: Optional[int] = None) -> int:
    """Transforme enterprise_finale -> enterprise_silver. Renvoie le nombre traite."""
    mongo_utils.silver_collection().create_index([("Status", 1)])
    labels = load_code_labels("FR")
    print(f"Labels de code charges : {len(labels):,}")

    source = mongo_utils.finale_collection()
    target = mongo_utils.silver_collection()

    cursor = source.find({})
    if limit:
        cursor = cursor.limit(limit)

    ops: list[UpdateOne] = []
    count = 0
    for doc in cursor:
        silver_doc = to_silver(doc, labels)
        ops.append(UpdateOne({"_id": silver_doc["_id"]}, {"$set": silver_doc}, upsert=True))
        count += 1
        if len(ops) >= BATCH_SIZE:
            target.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        target.bulk_write(ops, ordered=False)

    total = target.count_documents({})
    print(f"enterprise_silver : {count:,} documents transformes, {total:,} au total.")
    return count


if __name__ == "__main__":
    build_silver()
