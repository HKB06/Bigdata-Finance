"""Peuplement de MongoDB avec les entreprises belges (numeros BCE).

Source primaire : le fichier `enterprise.csv` de l'Open Data BCE/KBO,
attendu dans `data/enterprise.csv`. Si le fichier est absent, on bascule
sur un petit jeu de demonstration (DEMO_COMPANIES) afin que le pipeline
reste executable pour le rendu.
"""

from __future__ import annotations

import csv
import os
import re
from datetime import datetime, timezone
from typing import Iterator, Optional

from pymongo import UpdateOne

from . import config
from . import mongo_utils

# Par defaut on charge TOUT enterprise.csv dans MongoDB (~2M entreprises),
# comme attendu pour le referentiel. Mettre une valeur entiere (ex. 200) ou
# la variable SEED_LIMIT pour borner le chargement lors de tests.
_env_limit = os.getenv("SEED_LIMIT", "").strip()
DEFAULT_SEED_LIMIT: Optional[int] = int(_env_limit) if _env_limit.isdigit() else None

# Taille des lots pour l'insertion en masse dans MongoDB.
BATCH_SIZE = 5000


def normalize_bce(value: str) -> str:
    """Normalise un numero d'entreprise sur 10 chiffres."""
    return re.sub(r"\D", "", value or "").zfill(10)


def _read_csv_rows(limit: Optional[int]) -> Iterator[dict]:
    path = config.ENTERPRISE_CSV
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if limit and i >= limit:
                break
            yield row


def _extract_number(row: dict) -> Optional[str]:
    for col in (
        "EnterpriseNumber",
        "enterprise_number",
        "enterpriseNumber",
        "Number",
        "number",
    ):
        if row.get(col):
            return normalize_bce(row[col])
    return None


def _flush(collection, operations: list) -> None:
    if operations:
        collection.bulk_write(operations, ordered=False)


def seed_companies(limit: Optional[int] = DEFAULT_SEED_LIMIT) -> int:
    """Charge les entreprises dans MongoDB (insertion en masse par lots).

    Renvoie le nombre de lignes traitees.
    """
    mongo_utils.ensure_indexes()
    collection = mongo_utils.companies_collection()

    count = 0
    if config.ENTERPRISE_CSV.exists():
        label = "tout" if limit is None else f"limite={limit}"
        print(f"Seed depuis {config.ENTERPRISE_CSV} ({label}), par lots de {BATCH_SIZE}...")

        operations: list[UpdateOne] = []
        for row in _read_csv_rows(limit):
            bce = _extract_number(row)
            if not bce:
                continue
            now = datetime.now(timezone.utc)
            operations.append(
                UpdateOne(
                    {"bce": bce},
                    {
                        "$set": {"bce": bce, "source": "kbo_open_data", "updated_at": now},
                        "$setOnInsert": {"created_at": now},
                    },
                    upsert=True,
                )
            )
            count += 1
            if len(operations) >= BATCH_SIZE:
                _flush(collection, operations)
                operations = []
                print(f"  ... {count:,} entreprises chargees")
        _flush(collection, operations)
    else:
        print(
            f"{config.ENTERPRISE_CSV} introuvable -> jeu de demonstration "
            f"({len(config.DEMO_COMPANIES)} entreprises)."
        )
        for company in config.DEMO_COMPANIES:
            mongo_utils.upsert_company(bce=company["bce"], name=company["name"], source="demo")
            count += 1

    total = mongo_utils.count_companies()
    print(f"Seed termine : {count:,} entreprises traitees, {total:,} au total dans MongoDB.")
    return count


if __name__ == "__main__":
    seed_companies()
