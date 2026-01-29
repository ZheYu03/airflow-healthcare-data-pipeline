"""
Clinic Enrichment DAG (import-light)

Important: keep DAG module import fast (< 30s), otherwise Airflow will mark it as
"Broken DAG: DagBag import timeout". Heavy imports (Playwright, Supabase) and
Airflow DB calls should happen inside tasks, not at module import time.
"""

import asyncio
import logging
import os
import random
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _get_cfg() -> Dict[str, Any]:
    """
    Resolve runtime config. Prefer env vars (fast) and only hit Airflow Variables
    at task runtime.
    """
    def _v_int(key: str, default: int) -> int:
        try:
            return int(os.environ.get(key) or Variable.get(key, default_var=str(default)))
        except Exception:
            return default

    def _v_bool(key: str, default: bool) -> bool:
        raw = os.environ.get(key)
        if raw is None:
            try:
                raw = Variable.get(key, default_var=str(default).lower())
            except Exception:
                raw = str(default).lower()
        return str(raw).lower() == "true"

    return {
        "batch_size": _v_int("ENRICHMENT_BATCH_SIZE", 50),
        "min_delay": _v_int("ENRICHMENT_MIN_DELAY", 8),
        "max_delay": _v_int("ENRICHMENT_MAX_DELAY", 15),
        "headless": _v_bool("ENRICHMENT_HEADLESS", True),
    }


def check_unenriched_clinics(**_context) -> bool:
    from helpers.supabase_client import SupabaseClient  # lazy import

    supabase = SupabaseClient()
    count = supabase.get_unenriched_count()
    logger.info("Found %s unenriched clinics remaining", count)
    return count > 0


def get_clinics_to_enrich(**context) -> List[Dict[str, Any]]:
    from helpers.supabase_client import SupabaseClient  # lazy import

    cfg = _get_cfg()
    supabase = SupabaseClient()
    clinics = supabase.get_unenriched_clinics(limit=int(cfg["batch_size"]))
    logger.info("Retrieved %s clinics for enrichment", len(clinics))
    context["ti"].xcom_push(key="clinics_to_enrich", value=clinics)
    return clinics


async def _scrape_batch(clinics: List[Dict[str, Any]], cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    from helpers.maps_scraper import GoogleMapsScraper  # lazy import (Playwright)

    results: List[Dict[str, Any]] = []
    async with GoogleMapsScraper(headless=bool(cfg["headless"])) as scraper:
        for i, clinic in enumerate(clinics):
            clinic_id = clinic.get("id")
            name = clinic.get("name") or ""
            address = clinic.get("address") or ""
            city = clinic.get("city") or ""
            state = clinic.get("state") or ""

            logger.info("Scraping %s/%s: %s", i + 1, len(clinics), name)
            try:
                enrichment = await scraper.scrape_clinic(name, address, city, state)
                enrichment["clinic_id"] = clinic_id
                enrichment["name"] = name
                results.append(enrichment)
            except Exception as e:
                logger.exception("Failed to scrape %s", name)
                results.append(
                    {
                        "clinic_id": clinic_id,
                        "name": name,
                        "scrape_success": False,
                        "scrape_error": str(e),
                    }
                )

            if i < len(clinics) - 1:
                await asyncio.sleep(random.uniform(float(cfg["min_delay"]), float(cfg["max_delay"])))

    return results


def scrape_clinics(**context) -> Dict[str, Any]:
    cfg = _get_cfg()
    ti = context["ti"]
    clinics = ti.xcom_pull(key="clinics_to_enrich", task_ids="get_clinics_to_enrich") or []
    if not clinics:
        logger.info("No clinics to scrape")
        return {"total": 0, "success": 0, "failed": 0}

    results = asyncio.run(_scrape_batch(clinics, cfg))
    ti.xcom_push(key="scrape_results", value=results)

    success_count = sum(1 for r in results if r.get("scrape_success"))
    failed_count = len(results) - success_count
    logger.info("Scraping complete: %s success, %s failed", success_count, failed_count)
    return {"total": len(results), "success": success_count, "failed": failed_count}


def update_supabase(**context) -> Dict[str, Any]:
    from helpers.supabase_client import SupabaseClient  # lazy import

    ti = context["ti"]
    results = ti.xcom_pull(key="scrape_results", task_ids="scrape_clinics") or []
    if not results:
        logger.info("No scrape results to update")
        return {"updated": 0, "failed": 0, "remaining": None}

    supabase = SupabaseClient()
    updated = 0
    failed = 0

    for result in results:
        clinic_id = result.get("clinic_id")
        if not clinic_id:
            failed += 1
            continue

        if result.get("scrape_success"):
            if supabase.update_enrichment(clinic_id, result):
                updated += 1
            else:
                failed += 1
        else:
            supabase.mark_enrichment_failed(clinic_id)
            failed += 1

    remaining = supabase.get_unenriched_count()
    logger.info("Supabase update complete: %s updated, %s failed, %s remaining", updated, failed, remaining)
    return {"updated": updated, "failed": failed, "remaining": remaining}


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
}


with DAG(
    dag_id="clinic_enrichment",
    default_args=default_args,
    description="Enrich clinic data by scraping Google Maps hourly",
    schedule_interval="0 * * * *",
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["clinic", "enrichment", "scraping", "google-maps"],
    max_active_runs=1,
) as dag:
    check_task = ShortCircuitOperator(
        task_id="check_unenriched",
        python_callable=check_unenriched_clinics,
    )

    get_clinics_task = PythonOperator(
        task_id="get_clinics_to_enrich",
        python_callable=get_clinics_to_enrich,
    )

    scrape_task = PythonOperator(
        task_id="scrape_clinics",
        python_callable=scrape_clinics,
        execution_timeout=timedelta(hours=2),
    )

    update_task = PythonOperator(
        task_id="update_supabase",
        python_callable=update_supabase,
    )

    check_task >> get_clinics_task >> scrape_task >> update_task

