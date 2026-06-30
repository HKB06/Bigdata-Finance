"""Client HTTP avec rotation Tor (anti-blocage du scraping).

Les sources publiques (NBB/CBSO, eJustice, stapor) limitent fortement les
requetes par IP. Pour eviter les blocages, on route le trafic a travers
plusieurs proxies Tor (`tor1`, `tor2`, `tor3`) et on alterne d'IP :

* round-robin : chaque requete part par un proxy different ;
* retry : en cas d'echec, on rejoue la requete par le proxy suivant ;
* NEWNYM : on peut demander un nouveau circuit Tor (nouvelle IP de sortie)
  via le port de controle.

Si `USE_TOR` est faux ou si aucun proxy n'est configure, les requetes
partent en direct (utile en local sans Tor).
"""

from __future__ import annotations

import itertools
import socket
import time
from typing import Optional

import requests

from . import config

# Cycle round-robin sur les proxies Tor disponibles.
_proxy_cycle = itertools.cycle(config.TOR_PROXIES) if config.TOR_PROXIES else None


def _proxies(proxy_url: str) -> dict[str, str]:
    return {"http": proxy_url, "https": proxy_url}


def get(
    url: str,
    *,
    use_tor: Optional[bool] = None,
    retries: Optional[int] = None,
    **kwargs,
) -> requests.Response:
    """GET avec rotation Tor optionnelle.

    `use_tor=None` suit la configuration globale (`config.USE_TOR`).
    """
    kwargs.setdefault("headers", config.HTTP_HEADERS)
    kwargs.setdefault("timeout", config.HTTP_TIMEOUT)

    tor_enabled = config.USE_TOR if use_tor is None else use_tor
    if not tor_enabled or not _proxy_cycle:
        response = requests.get(url, **kwargs)
        response.raise_for_status()
        return response

    attempts = retries or (len(config.TOR_PROXIES) + 1)
    last_error: Optional[Exception] = None
    for _ in range(attempts):
        proxy_url = next(_proxy_cycle)
        try:
            response = requests.get(url, proxies=_proxies(proxy_url), **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:  # noqa: BLE001
            last_error = exc
            time.sleep(0.5)

    raise last_error  # type: ignore[misc]


def renew_identity() -> int:
    """Demande un nouveau circuit (nouvelle IP) sur chaque noeud Tor.

    Best-effort : renvoie le nombre de noeuds ayant accepte le NEWNYM.
    """
    renewed = 0
    for host in config.TOR_CONTROL_HOSTS:
        try:
            with socket.create_connection((host, config.TOR_CONTROL_PORT), timeout=10) as sock:
                sock.sendall(f'AUTHENTICATE "{config.TOR_CONTROL_PASSWORD}"\r\n'.encode())
                if b"250" not in sock.recv(1024):
                    continue
                sock.sendall(b"SIGNAL NEWNYM\r\n")
                if b"250" in sock.recv(1024):
                    renewed += 1
        except Exception:
            continue
    return renewed


def public_ip(use_tor: Optional[bool] = None) -> str:
    """Renvoie l'IP publique vue par la sortie (debug de la rotation)."""
    return get("https://api.ipify.org", use_tor=use_tor, timeout=30).text.strip()
