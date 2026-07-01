"""Construction de la collection Bronze consolidee `enterprise_finale`.

Fusionne les 5 fichiers Open Data KBO en un document riche par entreprise :

    {
        "_id": "0533820890",
        "EnterpriseNumber": "0533820890",
        "Status": "AC",
        "JuridicalSituation": "000",
        "TypeOfEnterprise": "2",
        "JuridicalForm": "610",
        "StartDate": "02-01-2021",
        "denominations": [{"Language": "2", "TypeOfDenomination": "1", "Denomination": "..."}],
        "addresses":     [{"TypeOfAddress": "REGO", "Zipcode": "1020", ...}],
        "activities":    [{"NaceVersion": "2008", "NaceCode": "55100", "Classification": "MAIN"}],
    }

Sans les CSV dans data/, on charge un jeu de demonstration compose de vrais
hotels belges (DEMO_HOTELS) + des entreprises non hotelieres, afin que le
Silver et le filtre hotellerie restent demontrables.

La variable SEED_LIMIT borne le nombre d'entreprises chargees (tests).
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


# --------------------------------------------------------------------------
# Construction depuis les CSV KBO
# --------------------------------------------------------------------------
def _build_from_csv(limit: Optional[int]) -> int:
    # 1. Entreprises cibles (limitees) -> document de base.
    docs: dict[str, dict] = {}
    for i, row in enumerate(_dict_rows(config.KBO_FILES["enterprise"])):
        if limit and i >= limit:
            break
        bce = normalize_bce(row.get("EnterpriseNumber"))
        if not bce:
            continue
        docs[bce] = {
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
        }
    targets = set(docs)
    print(f"  entreprises retenues : {len(targets):,}")

    # 2. Sous-fichiers : on ne garde que les lignes des entreprises cibles.
    def attach(file_key: str, dest: str, fields: list[str]) -> None:
        path = config.KBO_FILES[file_key]
        kept = 0
        for row in _dict_rows(path):
            bce = normalize_bce(row.get("EntityNumber"))
            doc = docs.get(bce)
            if doc is None:
                continue
            doc[dest].append({field: row.get(field) for field in fields})
            kept += 1
        print(f"  {file_key}: {kept:,} lignes rattachees")

    attach("denomination", "denominations", ["Language", "TypeOfDenomination", "Denomination"])
    attach("address", "addresses", [
        "TypeOfAddress", "CountryFR", "Zipcode", "MunicipalityFR",
        "StreetFR", "HouseNumber", "Box", "DateStrikingOff",
    ])
    attach("activity", "activities", ["ActivityGroup", "NaceVersion", "NaceCode", "Classification"])

    # 3. Insertion en masse.
    collection = mongo_utils.finale_collection()
    ops: list[UpdateOne] = []
    now = datetime.now(timezone.utc)
    count = 0
    for doc in docs.values():
        doc["built_at"] = now
        ops.append(UpdateOne({"_id": doc["_id"]}, {"$set": doc}, upsert=True))
        count += 1
        if len(ops) >= BATCH_SIZE:
            collection.bulk_write(ops, ordered=False)
            ops = []
    if ops:
        collection.bulk_write(ops, ordered=False)
    return count


# --------------------------------------------------------------------------
# Jeu de demonstration (vrais hotels + entreprises non hotelieres)
# --------------------------------------------------------------------------
def _demo_document(bce: str, name: str, nace: Optional[str]) -> dict:
    activities = []
    if nace:
        activities = [
            {"ActivityGroup": "001", "NaceVersion": "2008", "NaceCode": nace, "Classification": "MAIN"},
            # doublon exact (pour illustrer la deduplication Silver)
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
    # Quelques entreprises non hotelieres (pour la demo Silver, pas de scraping).
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
        print(
            f"CSV KBO manquants ({', '.join(missing)}) -> jeu de demonstration "
            f"(hotels reels + entreprises non hotelieres)."
        )
        count = _build_demo()

    total = mongo_utils.finale_collection().count_documents({})
    print(f"enterprise_finale : {count:,} documents traites, {total:,} au total.")
    return count


if __name__ == "__main__":
    build_enterprise_finale()
