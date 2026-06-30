"""Peuplement de MongoDB avec les entreprises belges (numeros BCE).

Source primaire : le fichier `enterprise.csv` de l'Open Data BCE/KBO,
attendu dans `data/enterprise.csv`. Si le fichier est absent, on bascule
sur un petit jeu de demonstration (DEMO_COMPANIES) afin que le pipeline
reste executable pour le rendu.
"""

from __future__ import annotations

import csv
import re
from typing import Iterator, Optional

from . import config
from . import mongo_utils

# enterprise.csv peut etre volumineux (~2M lignes) : on borne le seed par defaut.
DEFAULT_SEED_LIMIT = 200


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


def seed_companies(limit: Optional[int] = DEFAULT_SEED_LIMIT) -> int:
    """Charge les entreprises dans MongoDB. Renvoie le nombre insere/maj."""
    mongo_utils.ensure_indexes()

    count = 0
    if config.ENTERPRISE_CSV.exists():
        print(f"Seed depuis {config.ENTERPRISE_CSV} (limite={limit})")
        for row in _read_csv_rows(limit):
            bce = _extract_number(row)
            if not bce:
                continue
            mongo_utils.upsert_company(bce=bce, source="kbo_open_data")
            count += 1
    else:
        print(
            f"{config.ENTERPRISE_CSV} introuvable -> jeu de demonstration "
            f"({len(config.DEMO_COMPANIES)} entreprises)."
        )
        for company in config.DEMO_COMPANIES:
            mongo_utils.upsert_company(bce=company["bce"], name=company["name"], source="demo")
            count += 1

    total = mongo_utils.count_companies()
    print(f"Seed termine : {count} entreprises traitees, {total} au total dans MongoDB.")
    return count


if __name__ == "__main__":
    seed_companies()
