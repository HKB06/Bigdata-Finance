"""Client HDFS (WebHDFS) et ecriture des fichiers bruts dans Bronze.

Arborescence Bronze :

    {BRONZE_ROOT}/{source}/{bce}/{doc_type}/{nom_de_fichier}

Exemple :

    /data/raw/nbb/0878065378/csv/2023.csv
    /data/raw/nbb/0878065378/pdf/2023.pdf
    /data/raw/ejustice/0878065378/pdf/2023-0123456.pdf
"""

from __future__ import annotations

from typing import Optional

from hdfs import InsecureClient

from . import config

_hdfs: Optional[InsecureClient] = None


def get_hdfs() -> InsecureClient:
    global _hdfs
    if _hdfs is None:
        _hdfs = InsecureClient(config.HDFS_URL, user=config.HDFS_USER)
    return _hdfs


def bronze_path(source: str, bce: str, doc_type: str, filename: str) -> str:
    """Construit le chemin HDFS Bronze d'un fichier."""
    return f"{config.BRONZE_ROOT}/{source}/{bce}/{doc_type}/{filename}"


def write_bytes(hdfs_path: str, content: bytes) -> int:
    """Ecrit un contenu binaire dans HDFS et renvoie sa taille en octets."""
    client = get_hdfs()
    parent = hdfs_path.rsplit("/", 1)[0]
    client.makedirs(parent)
    client.write(hdfs_path, data=content, overwrite=True)
    return len(content)


def check_connection() -> None:
    """Verifie que le NameNode/WebHDFS repond."""
    status = get_hdfs().status("/", strict=False)
    if status is None:
        raise RuntimeError(f"HDFS injoignable sur {config.HDFS_URL}")
