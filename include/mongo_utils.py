"""Acces MongoDB : referentiel des entreprises et State DB (meta DB).

Deux collections sont gerees :

1. `companies`  : une entreprise belge par document (numero BCE + metadonnees).
2. `ingestion_state` : la State DB. Un document par fichier a telecharger.
   Elle garantit l'idempotence : on ne re-telecharge jamais un fichier deja
   marque `done` (delta detection).

Schema d'un document de State DB
--------------------------------
{
    "_id":        "<cle deterministe>",   # bce|source|deposit_id|doc_type|year
    "bce":        "0878065378",
    "source":     "nbb" | "ejustice",
    "doc_type":   "csv" | "pdf" | "html",
    "deposit_id": "123456" | None,
    "year":       2023 | None,
    "status":     "pending" | "done" | "error",
    "hdfs_path":  "/data/raw/nbb/0878065378/csv/2023.csv",
    "size_bytes": 12345,
    "error":      "message ou None",
    "created_at": <datetime UTC>,
    "updated_at": <datetime UTC>
}
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterator, Optional

from pymongo import ASCENDING, MongoClient
from pymongo.collection import Collection

from . import config

_client: Optional[MongoClient] = None


def _now() -> datetime:
    return datetime.now(timezone.utc)


def get_client() -> MongoClient:
    """Retourne un client MongoDB reutilise au sein du process."""
    global _client
    if _client is None:
        _client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
    return _client


def companies_collection() -> Collection:
    return get_client()[config.MONGO_DB][config.MONGO_COMPANIES]


def state_collection() -> Collection:
    return get_client()[config.MONGO_DB][config.MONGO_STATE]


def ensure_indexes() -> None:
    """Initialise la State DB et le referentiel (index)."""
    companies = companies_collection()
    companies.create_index([("bce", ASCENDING)], unique=True)

    state = state_collection()
    state.create_index([("bce", ASCENDING)])
    state.create_index([("status", ASCENDING)])
    state.create_index([("source", ASCENDING), ("status", ASCENDING)])


# --------------------------------------------------------------------------
# Referentiel entreprises
# --------------------------------------------------------------------------
def upsert_company(bce: str, name: Optional[str] = None, **extra) -> None:
    """Insere ou met a jour une entreprise (cle = numero BCE normalise)."""
    doc = {"bce": bce, "updated_at": _now()}
    if name:
        doc["name"] = name
    doc.update(extra)

    companies_collection().update_one(
        {"bce": bce},
        {"$set": doc, "$setOnInsert": {"created_at": _now()}},
        upsert=True,
    )


def count_companies() -> int:
    return companies_collection().count_documents({})


def iter_company_bce(limit: Optional[int] = None) -> list[str]:
    """Liste les numeros BCE des entreprises a traiter."""
    cursor = companies_collection().find({}, {"bce": 1, "_id": 0}).sort("bce", ASCENDING)
    if limit:
        cursor = cursor.limit(limit)
    return [doc["bce"] for doc in cursor]


# --------------------------------------------------------------------------
# State DB : delta detection + suivi
# --------------------------------------------------------------------------
def file_key(
    bce: str,
    source: str,
    doc_type: str,
    year: Optional[int] = None,
    deposit_id: Optional[str] = None,
) -> str:
    """Construit la cle deterministe d'un fichier dans la State DB."""
    return "|".join(
        [bce, source, str(deposit_id or "-"), doc_type, str(year or "-")]
    )


def is_done(key: str) -> bool:
    """Delta detection : True si le fichier est deja telecharge avec succes."""
    doc = state_collection().find_one({"_id": key}, {"status": 1})
    return bool(doc and doc.get("status") == "done")


def mark_pending(
    key: str,
    bce: str,
    source: str,
    doc_type: str,
    year: Optional[int] = None,
    deposit_id: Optional[str] = None,
    hdfs_path: Optional[str] = None,
) -> None:
    state_collection().update_one(
        {"_id": key},
        {
            "$set": {
                "bce": bce,
                "source": source,
                "doc_type": doc_type,
                "year": year,
                "deposit_id": deposit_id,
                "hdfs_path": hdfs_path,
                "status": "pending",
                "error": None,
                "updated_at": _now(),
            },
            "$setOnInsert": {"created_at": _now()},
        },
        upsert=True,
    )


def mark_done(key: str, hdfs_path: str, size_bytes: int) -> None:
    state_collection().update_one(
        {"_id": key},
        {
            "$set": {
                "status": "done",
                "hdfs_path": hdfs_path,
                "size_bytes": size_bytes,
                "error": None,
                "updated_at": _now(),
            }
        },
    )


def mark_error(key: str, error: str) -> None:
    state_collection().update_one(
        {"_id": key},
        {"$set": {"status": "error", "error": str(error)[:2000], "updated_at": _now()}},
    )


def state_summary() -> dict[str, int]:
    """Renvoie le nombre de fichiers par statut (pour le monitoring)."""
    pipeline = [{"$group": {"_id": "$status", "n": {"$sum": 1}}}]
    return {row["_id"]: row["n"] for row in state_collection().aggregate(pipeline)}
