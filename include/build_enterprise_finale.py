"""Construit la collection Bronze enterprise_finale a partir des 5 CSV KBO.

Un document par entreprise, avec ses denominations, adresses et activites.
Si les CSV ne sont pas dans data/, on charge un petit jeu de demo.
SEED_LIMIT permet de limiter le nombre d'entreprises (tests).
"""

from __future__ import annotations

import csv
import os
import re
from datetime import datetime, timezone
from typing import Iterable, Optional

from pymongo import UpdateOne

from . import config
from . import mongo_utils

BATCH_SIZE = 5000
_env_limit = os.getenv("SEED_LIMIT", "").strip()
DEFAULT_LIMIT: Optional[int] = int(_env_limit) if _env_limit.isdigit() else None


def normalize_bce(value: str) -> str:
    return re.sub(r"\D", "", value or "").zfill(10)


def _dict_rows(path) -> Iterable[dict]:
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        yield from csv.DictReader(f)


def _all_kbo_files_present() -> bool:
    return all(path.exists() for path in config.KBO_FILES.values())


# Les CSV pesent plusieurs Go : on lit en streaming et on ecrit par lots.
def _load_enterprises(limit: Optional[int]) -> tuple[int, Optional[set]]:
    """Cree un document de base par entreprise (listes vides)."""
    collection = mongo_utils.finale_collection()
    now = datetime.now(timezone.utc)
    targets: Optional[set] = set() if limit is not None else None

    ops: list[UpdateOne] = []
    count = 0
    for row in _dict_rows(config.KBO_FILES["enterprise"]):
        if limit is not None and count >= limit:
            break
        bce = normalize_bce(row.get("EnterpriseNumber"))
        if not bce:
            continue
        doc = {
            "_id": bce,
            "EnterpriseNumber": bce,
            "Status": row.get("Status"),
            "JuridicalSituation": row.get("JuridicalSituation"),
            "TypeOfEnterprise": row.get("TypeOfEnterprise"),
            "JuridicalForm": row.get("JuridicalForm"),
            "StartDate": row.get("StartDate"),
            "denominations": [],
            "addresses": [],
            "activities": [],
            "built_at": now,
        }
        ops.append(UpdateOne({"_id": bce}, {"$set": doc}, upsert=True))
        if targets is not None:
            targets.add(bce)
        count += 1
        if len(ops) >= BATCH_SIZE:
            collection.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        collection.bulk_write(ops, ordered=False)
    print(f"  enterprise.csv : {count:,} entreprises chargees")
    return count, targets


def _iter_grouped(path, key_field: str):
    """Regroupe les lignes qui se suivent et ont le meme numero d'entite."""
    current: Optional[str] = None
    bucket: list[dict] = []
    for row in _dict_rows(path):
        key = normalize_bce(row.get(key_field))
        if key != current:
            if current and bucket:
                yield current, bucket
            current, bucket = key, []
        bucket.append(row)
    if current and bucket:
        yield current, bucket


def _attach(file_key: str, dest: str, fields: list[str], targets: Optional[set]) -> None:
    """Ajoute les lignes d'un sous-fichier dans le document de l'entreprise."""
    collection = mongo_utils.finale_collection()
    ops: list[UpdateOne] = []
    groups = 0
    for bce, rows in _iter_grouped(config.KBO_FILES[file_key], "EntityNumber"):
        if not bce:
            continue
        if targets is not None and bce not in targets:
            continue
        payload = [{f: r.get(f) for f in fields} for r in rows]
        # upsert=False : on ignore les etablissements sans entreprise.
        ops.append(UpdateOne({"_id": bce}, {"$push": {dest: {"$each": payload}}}, upsert=False))
        groups += 1
        if len(ops) >= BATCH_SIZE:
            collection.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        collection.bulk_write(ops, ordered=False)
    print(f"  {file_key} : {groups:,} entites rattachees ({dest})")


def _build_from_csv(limit: Optional[int]) -> int:
    count, targets = _load_enterprises(limit)
    _attach("denomination", "denominations",
            ["Language", "TypeOfDenomination", "Denomination"], targets)
    _attach("address", "addresses",
            ["TypeOfAddress", "CountryFR", "Zipcode", "MunicipalityFR",
             "StreetFR", "HouseNumber", "Box", "DateStrikingOff"], targets)
    _attach("activity", "activities",
            ["ActivityGroup", "NaceVersion", "NaceCode", "Classification"], targets)
    return count


# Jeu de demo si les CSV KBO sont absents.
def _demo_document(bce: str, name: str, nace: Optional[str]) -> dict:
    activities = []
    if nace:
        activities = [
            {"ActivityGroup": "001", "NaceVersion": "2008", "NaceCode": nace, "Classification": "MAIN"},
            {"ActivityGroup": "001", "NaceVersion": "2008", "NaceCode": nace, "Classification": "MAIN"},
        ]
    return {
        "_id": bce,
        "EnterpriseNumber": bce,
        "Status": "AC",
        "JuridicalSituation": "000",
        "TypeOfEnterprise": "2",
        "JuridicalForm": "610",
        "StartDate": "02-01-2015",
        "denominations": [
            {"Language": "2", "TypeOfDenomination": "1", "Denomination": name},
            {"Language": "2", "TypeOfDenomination": "3", "Denomination": name + " (abrege)"},
        ],
        "addresses": [
            {"TypeOfAddress": "REGO", "CountryFR": "Belgique", "Zipcode": "1000",
             "MunicipalityFR": "Bruxelles", "StreetFR": "Rue Exemple", "HouseNumber": "1",
             "Box": "", "DateStrikingOff": ""},
            {"TypeOfAddress": "OBAD", "CountryFR": "Belgique", "Zipcode": "1050",
             "MunicipalityFR": "Ixelles", "StreetFR": "Autre Rue", "HouseNumber": "9",
             "Box": "", "DateStrikingOff": ""},
        ],
        "activities": activities,
        "built_at": datetime.now(timezone.utc),
    }


def _build_demo() -> int:
    collection = mongo_utils.finale_collection()
    docs = []
    for hotel in config.DEMO_HOTELS:
        docs.append(_demo_document(hotel["bce"], hotel["name"], nace="55100"))
    for company in config.DEMO_COMPANIES:
        docs.append(_demo_document(company["bce"], company["name"], nace="70100"))

    ops = [UpdateOne({"_id": d["_id"]}, {"$set": d}, upsert=True) for d in docs]
    collection.bulk_write(ops, ordered=False)
    return len(docs)


def build_enterprise_finale(limit: Optional[int] = DEFAULT_LIMIT) -> int:
    mongo_utils.ensure_indexes()

    if _all_kbo_files_present():
        print(f"Construction de enterprise_finale depuis les CSV KBO (limite={limit})...")
        count = _build_from_csv(limit)
    else:
        missing = [k for k, p in config.KBO_FILES.items() if not p.exists()]
        print(f"CSV KBO manquants ({', '.join(missing)}) -> jeu de demonstration.")
        count = _build_demo()

    total = mongo_utils.finale_collection().count_documents({})
    print(f"enterprise_finale : {count:,} documents traites, {total:,} au total.")
    return count


if __name__ == "__main__":
    build_enterprise_finale()
