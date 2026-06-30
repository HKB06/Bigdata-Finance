"""Ingestion des sources externes vers HDFS Bronze, pilotee par la State DB.

Deux sources sont couvertes pour cette journee :

* NBB / CBSO : comptes annuels (CSV + PDF) via l'API publique consult.cbso.nbb.be
* eJustice   : publications du Moniteur belge (PDF) liees a une entreprise

Chaque telechargement suit le meme cycle :
    1. construction d'une cle State DB ;
    2. delta detection : si la cle est deja `done`, on saute ;
    3. `pending` -> telechargement -> ecriture HDFS -> `done` (ou `error`).
"""

from __future__ import annotations

import re
import time
from typing import Any, Optional
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup

from . import config
from . import hdfs_utils
from . import mongo_utils

# --- Endpoints --------------------------------------------------------------
CBSO_API = "https://consult.cbso.nbb.be/api/rs-consult/published-deposits"
CBSO_BROKER = "https://consult.cbso.nbb.be/api/external/broker/public/deposits"

EJUSTICE_ROOT = "https://www.ejustice.just.fgov.be"
EJUSTICE_LIST = f"{EJUSTICE_ROOT}/cgi_tsv/list.pl"


def normalize_bce(value: str) -> str:
    return re.sub(r"\D", "", value or "").zfill(10)


def _get(url: str, **kwargs) -> requests.Response:
    kwargs.setdefault("headers", config.HTTP_HEADERS)
    kwargs.setdefault("timeout", config.HTTP_TIMEOUT)
    response = requests.get(url, **kwargs)
    response.raise_for_status()
    return response


def _download_to_bronze(
    *,
    bce: str,
    source: str,
    doc_type: str,
    filename: str,
    url: str,
    year: Optional[int] = None,
    deposit_id: Optional[str] = None,
    headers: Optional[dict] = None,
) -> str:
    """Telecharge un fichier en respectant la State DB (delta detection).

    Renvoie l'un des statuts : "skipped", "done", "empty", "error".
    """
    key = mongo_utils.file_key(bce, source, doc_type, year=year, deposit_id=deposit_id)

    # 1. Delta detection : ne jamais re-telecharger un fichier deja present.
    if mongo_utils.is_done(key):
        return "skipped"

    hdfs_path = hdfs_utils.bronze_path(source, bce, doc_type, filename)
    mongo_utils.mark_pending(
        key, bce=bce, source=source, doc_type=doc_type,
        year=year, deposit_id=deposit_id, hdfs_path=hdfs_path,
    )

    try:
        response = _get(url, headers=headers or config.HTTP_HEADERS)
        content = response.content
        if not content:
            mongo_utils.mark_error(key, "reponse vide")
            return "empty"

        size = hdfs_utils.write_bytes(hdfs_path, content)
        mongo_utils.mark_done(key, hdfs_path=hdfs_path, size_bytes=size)
        return "done"
    except Exception as exc:  # noqa: BLE001
        mongo_utils.mark_error(key, str(exc))
        return "error"


# ==========================================================================
# NBB / CBSO : comptes annuels (CSV + PDF)
# ==========================================================================
def list_nbb_deposits(bce: str, size: int = 100) -> list[dict[str, Any]]:
    """Liste tous les depots publies pour une entreprise (pagine)."""
    numero = normalize_bce(bce)
    items: list[dict[str, Any]] = []
    page = 0
    while True:
        params = {
            "page": page,
            "size": size,
            "enterpriseNumber": numero,
            "sort": ["periodEndDate,desc", "depositDate,desc"],
        }
        response = _get(CBSO_API, params=params, headers={**config.HTTP_HEADERS, "Accept": "application/json"})
        payload = response.json()
        batch = payload.get("content", [])
        items.extend(batch)
        if payload.get("last", True) or not batch:
            break
        page += 1
        time.sleep(0.3)
    return items


def select_deposits(
    deposits: list[dict[str, Any]],
    year_min: int = config.YEAR_MIN,
    year_max: int = config.YEAR_MAX,
) -> dict[int, dict[str, Any]]:
    """Selectionne un depot non consolide par annee (prefere FR et non partiel)."""
    selected: dict[int, dict[str, Any]] = {}
    for deposit in deposits:
        model_id = str(deposit.get("modelId") or "").lower()
        model_name = str(deposit.get("modelName") or "").lower()
        if (
            model_id.startswith(("m120", "m122", "mc"))
            or "consolid" in model_name
            or "geconsolideerde" in model_name
        ):
            continue

        year = deposit.get("periodEndDateYear")
        if not isinstance(year, int) or not (year_min <= year <= year_max):
            continue

        current = selected.get(year)
        if current is None:
            selected[year] = deposit
            continue

        candidate_fr = str(deposit.get("language") or "").upper() == "FR"
        current_fr = str(current.get("language") or "").upper() == "FR"
        current_partial = str(current.get("modelId") or "").endswith("-p")
        candidate_partial = str(deposit.get("modelId") or "").endswith("-p")
        if (candidate_fr and not current_fr) or (current_partial and not candidate_partial):
            selected[year] = deposit

    return dict(sorted(selected.items(), reverse=True))


def ingest_nbb(bce: str) -> dict[str, int]:
    """Telecharge les comptes annuels NBB (CSV + PDF) vers Bronze HDFS.

    Met a jour la State DB et renvoie un compteur de statuts.
    """
    bce = normalize_bce(bce)
    stats = {"done": 0, "skipped": 0, "error": 0, "empty": 0}

    # Metadonnees brutes des depots (toujours rafraichies, utiles au debug).
    deposits = list_nbb_deposits(bce)

    bin_headers = {"User-Agent": "Mozilla/5.0", "Accept": "*/*", "Referer": "https://consult.cbso.nbb.be/"}

    for year, deposit in select_deposits(deposits).items():
        deposit_id = str(deposit.get("id") or "")
        if not deposit_id:
            continue

        # PDF du depot
        status = _download_to_bronze(
            bce=bce, source="nbb", doc_type="pdf", filename=f"{year}.pdf",
            url=f"{CBSO_BROKER}/pdf/{deposit_id}", year=year,
            deposit_id=deposit_id, headers=bin_headers,
        )
        stats[status] = stats.get(status, 0) + 1

        # CSV du depot
        status = _download_to_bronze(
            bce=bce, source="nbb", doc_type="csv", filename=f"{year}.csv",
            url=f"{CBSO_BROKER}/consult/csv/{deposit_id}", year=year,
            deposit_id=deposit_id, headers=bin_headers,
        )
        stats[status] = stats.get(status, 0) + 1

        time.sleep(0.4)

    print(f"[NBB] {bce} -> {stats}")
    return stats


# ==========================================================================
# eJustice : publications du Moniteur belge (PDF)
# ==========================================================================
_NUMAC_RE = re.compile(r"(\d{8,12})")


def _find_publication_pdfs(html: str) -> list[tuple[str, str]]:
    """Extrait les (numac, url_pdf) des publications d'une page de liste."""
    soup = BeautifulSoup(html, "html.parser")
    results: list[tuple[str, str]] = []
    seen: set[str] = set()
    for anchor in soup.find_all("a", href=True):
        href = anchor["href"]
        if ".pdf" not in href.lower():
            continue
        url = urljoin(EJUSTICE_ROOT, href)
        if url in seen:
            continue
        seen.add(url)
        match = _NUMAC_RE.search(href)
        numac = match.group(1) if match else str(len(results))
        results.append((numac, url))
    return results


def ingest_ejustice(bce: str, max_pages: int = 20) -> dict[str, int]:
    """Telecharge les publications eJustice (PDF) vers Bronze HDFS."""
    bce = normalize_bce(bce)
    stats = {"done": 0, "skipped": 0, "error": 0, "empty": 0}

    for page in range(1, max_pages + 1):
        params = {"language": "fr", "btw": bce, "page": page}
        try:
            response = _get(EJUSTICE_LIST, params=params)
        except Exception as exc:  # noqa: BLE001
            print(f"[eJustice] {bce} page {page} : {exc}")
            break

        soup = BeautifulSoup(response.text, "html.parser")
        if not soup.select("div.list-item--content"):
            break

        for numac, pdf_url in _find_publication_pdfs(response.text):
            status = _download_to_bronze(
                bce=bce, source="ejustice", doc_type="pdf",
                filename=f"{numac}.pdf", url=pdf_url, year=None, deposit_id=numac,
            )
            stats[status] = stats.get(status, 0) + 1
            time.sleep(0.2)

        time.sleep(0.3)

    print(f"[eJustice] {bce} -> {stats}")
    return stats
