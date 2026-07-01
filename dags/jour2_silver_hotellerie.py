"""DAG Jour 2 : consolidation Bronze -> Silver -> ciblage & scraping hotellerie.

Flux :
1. build_finale   : fusionne les 5 CSV KBO (ou le jeu de demo) -> enterprise_finale ;
2. build_silver   : nettoie / enrichit                          -> enterprise_silver ;
3. filter_hotels  : filtre le secteur hotelier -> State DB hotellerie (pending) ;
4. list_pending   : liste les entreprises hotelieres a scraper ;
5. scrape_hotel   : telecharge les CSV NBB (>= 2021) vers HDFS Bronze
                    (delta detection + reprise sur 429 via la State DB) ;
6. report         : resume des collections et de la State DB.

Variables d'environnement utiles :
- SEED_LIMIT  : borne le nombre d'entreprises chargees dans enterprise_finale ;
- HOTEL_LIMIT : borne le nombre d'hotels scrapes par run.
"""

from datetime import datetime, timedelta
import os
import sys

from airflow.decorators import dag, task

sys.path.append("/opt/airflow")

default_args = {
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


@dag(
    dag_id="jour2_silver_hotellerie",
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    max_active_tasks=3,
    tags=["bce", "silver", "hotellerie", "nbb", "transformation"],
)
def jour2_silver_hotellerie():

    @task
    def build_finale() -> int:
        from include import build_enterprise_finale

        return build_enterprise_finale.build_enterprise_finale()

    @task
    def build_silver(_upstream: int) -> int:
        from include import silver

        return silver.build_silver()

    @task
    def filter_hotels(_upstream: int) -> int:
        from include import hotel

        return hotel.filter_hotels()

    @task
    def list_pending(_upstream: int) -> list[str]:
        from include import mongo_utils

        limit = int(os.getenv("HOTEL_LIMIT", "0")) or None
        pending = mongo_utils.hotel_pending_bce(limit=limit)
        print(f"{len(pending)} hotel(s) a scraper (limite={limit}).")
        return pending

    @task(retries=3, retry_delay=timedelta(minutes=3))
    def scrape_hotel(bce: str) -> dict:
        from include import hotel

        return hotel.scrape_hotel(bce)

    @task(trigger_rule="all_done")
    def report() -> dict:
        from include import mongo_utils

        summary = {
            "enterprise_finale": mongo_utils.finale_collection().count_documents({}),
            "enterprise_silver": mongo_utils.silver_collection().count_documents({}),
            "hotel_state": mongo_utils.hotel_summary(),
            "files": mongo_utils.state_summary(),
        }
        print(f"Resume Jour 2 : {summary}")
        return summary

    finale = build_finale()
    silver_done = build_silver(finale)
    hotels_found = filter_hotels(silver_done)
    pending = list_pending(hotels_found)

    scraped = scrape_hotel.expand(bce=pending)
    scraped >> report()


jour2_silver_hotellerie()
