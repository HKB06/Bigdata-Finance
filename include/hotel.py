"""Jour 2 - Part 2 : ciblage hotellerie + scraping financier NBB.

1. `filter_hotels` : filtre `enterprise_finale` (codes NACE hotellerie, statut
   actif, personne morale, classification MAIN, formes juridiques publiques
   exclues) et charge les entreprises cibles dans la State DB (status=pending).
2. `scrape_hotel` : pour une entreprise, telecharge les depots financiers NBB
   (CSV) des exercices >= 2021 vers HDFS Bronze, en s'appuyant sur la State DB
   fichiers (delta detection) et met a jour la State DB hotellerie.

Le NBB peut renvoyer des 429 (rate limit) : l'entreprise passe alors en
status=error et sera reprise au prochain run (reprise propre via la State DB).
"""

from __future__ import annotations

import time
from typing import Optional

from . import config
from . import mongo_utils
from . import sources


# --------------------------------------------------------------------------
# 1. Filtre hotellerie
# --------------------------------------------------------------------------
def _official_name(doc: dict) -> Optional[str]:
    for denomination in doc.get("denominations", []):
        if str(denomination.get("TypeOfDenomination")) == "1":
            return denomination.get("Denomination")
    denominations = doc.get("denominations")
    return denominations[0].get("Denomination") if denominations else None


def is_hotel(doc: dict) -> bool:
    """Applique tous les criteres de ciblage hotellerie."""
    if doc.get("Status") != "AC":
        return False
    if str(doc.get("TypeOfEnterprise")) != "2":
        return False
    if str(doc.get("JuridicalForm")) in config.EXCLUDED_JURIDICAL_FORMS:
        return False
    for activity in doc.get("activities", []):
        if (
            activity.get("Classification") == "MAIN"
            and activity.get("NaceCode") in config.HOTEL_NACE_CODES
        ):
            return True
    return False


def filter_hotels(limit: Optional[int] = None) -> int:
    """Charge les entreprises hotelieres dans la State DB (status=pending)."""
    mongo_utils.ensure_hotel_indexes()
    source = mongo_utils.finale_collection()

    count = 0
    for doc in source.find({}):
        if not is_hotel(doc):
            continue
        mongo_utils.hotel_upsert_pending(doc["_id"], _official_name(doc))
        count += 1
        if limit and count >= limit:
            break

    total = mongo_utils.hotel_state_collection().count_documents({})
    print(f"Hotels cibles : {count:,} ajoutes, {total:,} au total en State DB.")
    return count


# --------------------------------------------------------------------------
# 2. Scraping NBB (CSV) des hotels
# --------------------------------------------------------------------------
def scrape_hotel(bce: str) -> dict:
    """Telecharge les CSV NBB (>= 2021) d'une entreprise hoteliere."""
    bce = sources.normalize_bce(bce)
    stats = {"done": 0, "skipped": 0, "error": 0, "empty": 0}

    if mongo_utils.hotel_is_done(bce):
        print(f"[hotel] {bce} deja done, ignore.")
        return stats

    mongo_utils.hotel_set_status(bce, "in_progress")

    try:
        deposits = sources.list_nbb_deposits(bce)
    except Exception as exc:  # noqa: BLE001 (inclut les 429)
        mongo_utils.hotel_set_status(bce, "error", error=str(exc))
        print(f"[hotel] {bce} erreur listing NBB : {exc}")
        stats["error"] += 1
        return stats

    selected = sources.select_deposits(
        deposits, year_min=config.HOTEL_YEAR_MIN, year_max=2100
    )
    bin_headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*",
                   "Referer": "https://consult.cbso.nbb.be/"}

    for year, deposit in selected.items():
        deposit_id = str(deposit.get("id") or "")
        if not deposit_id:
            continue
        status = sources._download_to_bronze(
            bce=bce, source="nbb", doc_type="csv", filename=f"{year}.csv",
            url=f"{sources.CBSO_BROKER}/consult/csv/{deposit_id}",
            year=year, deposit_id=deposit_id, headers=bin_headers,
        )
        stats[status] = stats.get(status, 0) + 1
        time.sleep(0.4)

    filings = stats["done"] + stats["skipped"]
    if stats["error"] > 0:
        mongo_utils.hotel_set_status(bce, "error", filings_count=filings,
                                     error="depot(s) en erreur (rate limit ?)")
    else:
        mongo_utils.hotel_set_status(bce, "done", filings_count=filings)

    print(f"[hotel] {bce} -> {stats} (filings={filings})")
    return stats


if __name__ == "__main__":
    filter_hotels()
