"""Gestion du cookie anti-bot de stapor (statuts.notaire.be).

statuts.notaire.be est protege par un challenge JavaScript : l'API ne
repond en JSON que si la requete porte les cookies poses par le navigateur.
On obtient ces cookies via Playwright (Chromium), puis on les met en cache
dans HDFS pour les partager entre les taches Airflow et eviter de relancer
un navigateur a chaque entreprise.

Ordre de resolution du cookie :
1. variable d'environnement COOKIE_NOTAIRE (si fournie) ;
2. cache HDFS (si encore valide) ;
3. acquisition via Playwright, puis mise en cache HDFS.
"""

from __future__ import annotations

from typing import Optional

import requests

from . import config

SEED = config.NOTAIRE_SEED_BCE
STATUTES_URL = "https://statuts.notaire.be/stapor_v1/api/enterprises/{num}/statutes"


def _is_valid(cookie: Optional[str]) -> bool:
    if not cookie:
        return False
    try:
        response = requests.get(
            STATUTES_URL.format(num=SEED),
            params={"offset": 0, "limit": 1},
            headers={**config.HTTP_HEADERS, "Accept": "application/json", "Cookie": cookie},
            timeout=15,
        )
        return "application/json" in response.headers.get("content-type", "").lower()
    except Exception:
        return False


def _read_hdfs_cookie() -> Optional[str]:
    try:
        from . import hdfs_utils

        with hdfs_utils.get_hdfs().read(config.COOKIE_HDFS_PATH, encoding="utf-8") as reader:
            return reader.read().strip()
    except Exception:
        return None


def _write_hdfs_cookie(cookie: str) -> None:
    try:
        from . import hdfs_utils

        client = hdfs_utils.get_hdfs()
        client.makedirs(config.COOKIE_HDFS_PATH.rsplit("/", 1)[0])
        client.write(config.COOKIE_HDFS_PATH, data=cookie, overwrite=True)
    except Exception:
        pass


def _acquire_with_playwright() -> Optional[str]:
    """Lance Chromium pour resoudre le challenge et recuperer les cookies."""
    try:
        from playwright.sync_api import sync_playwright
    except Exception:
        return None

    display = None
    try:
        from pyvirtualdisplay import Display

        display = Display(visible=False, size=(1400, 1000))
        display.start()
    except Exception:
        display = None

    try:
        seed_url = (
            f"https://statuts.notaire.be/stapor_v1/enterprise/{SEED}/statutes"
            f"?enterpriseNumber={SEED}&statuteStart=0&statuteCount=5"
        )
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(
                headless=False,
                args=["--no-sandbox", "--disable-dev-shm-usage"],
            )
            context = browser.new_context(locale="fr-BE")
            page = context.new_page()
            page.add_init_script(
                "Object.defineProperty(navigator,'webdriver',{get:()=>undefined})"
            )
            page.goto("https://statuts.notaire.be/", wait_until="load", timeout=25000)
            page.wait_for_timeout(2000)
            page.goto(seed_url, wait_until="load", timeout=35000)
            page.wait_for_timeout(3000)
            cookies = context.cookies()
            browser.close()
        return "; ".join(f"{c['name']}={c['value']}" for c in cookies) or None
    except Exception:
        return None
    finally:
        if display is not None:
            try:
                display.stop()
            except Exception:
                pass


def get_cookie(force: bool = False) -> Optional[str]:
    """Renvoie un cookie notaire valide, ou None si indisponible."""
    if config.COOKIE_NOTAIRE and _is_valid(config.COOKIE_NOTAIRE):
        return config.COOKIE_NOTAIRE

    if not force:
        cached = _read_hdfs_cookie()
        if _is_valid(cached):
            return cached

    cookie = _acquire_with_playwright()
    if cookie and _is_valid(cookie):
        _write_hdfs_cookie(cookie)
        return cookie

    return None
