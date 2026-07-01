"""Part 2 : filtre le secteur hotelier puis scrape ses comptes annuels NBB.

filter_hotels : selectionne les hotels dans enterprise_finale et les met en
State DB (pending). scrape_hotel : telecharge les CSV NBB (>= 2021) vers HDFS.
Sur un 429, l'hotel passe en error et sera repris au prochain run.
"""

from __future__ import annotations

import time
from typing import Optional

from . import config
from . import mongo_utils
from . import sources


def _official_name(doc: dict) -> Optional[str]:
    # TypeOfDenomination est zero-padde dans les donnees KBO ("001" = officiel).
    for denomination in doc.get("denominations", []):
        if str(denomination.get("TypeOfDenomination") or "").lstrip("0") == "1":
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
        time.sleep(0.15)

    filings = stats["done"] + stats["skipped"]
    if stats["error"] > 0:
        mongo_utils.hotel_set_status(bce, "error", filings_count=filings,
                                     error="depot(s) en erreur (rate limit ?)")
    else:
        mongo_utils.hotel_set_status(bce, "done", filings_count=filings)

    print(f"[hotel] {bce} -> {stats} (filings={filings})")
    return stats


def scrape_all_pending(
    limit: Optional[int] = None,
    workers: Optional[int] = None,
    renew_every: int = 200,
) -> dict:
    """Scrape en parallele tous les hotels pending/error de la State DB.

    Plusieurs threads (HOTEL_WORKERS, defaut 8) repartis sur les 3 sorties Tor.
    """
    import os
    from concurrent.futures import ThreadPoolExecutor, as_completed

    from . import config, http_client

    workers = workers or int(os.getenv("HOTEL_WORKERS", "8"))
    pending = mongo_utils.hotel_pending_bce(limit=limit)
    total = len(pending)
    print(f"[hotel] {total} entreprise(s) a scraper | workers={workers} | limite={limit}")

    if config.USE_TOR:
        try:
            http_client.renew_identity()
        except Exception:  # noqa: BLE001
            pass

    agg = {"done": 0, "skipped": 0, "error": 0, "empty": 0}
    ok_entreprises = 0
    processed = 0

    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(scrape_hotel, bce): bce for bce in pending}
        for future in as_completed(futures):
            processed += 1
            try:
                stats = future.result()
            except Exception as exc:  # noqa: BLE001
                print(f"[hotel] {futures[future]} exception : {exc}")
                stats = {"error": 1}
            for key, value in stats.items():
                agg[key] = agg.get(key, 0) + value
            if not stats.get("error"):
                ok_entreprises += 1
            if config.USE_TOR and renew_every and processed % renew_every == 0:
                try:
                    http_client.renew_identity()
                except Exception:  # noqa: BLE001
                    pass
            if processed % 50 == 0 or processed == total:
                print(f"[hotel] progression {processed}/{total} entreprises | fichiers={agg}")

    result = {"entreprises": total, "ok": ok_entreprises, **agg}
    print(f"[hotel] termine : {result}")
    return result


if __name__ == "__main__":
    filter_hotels()
