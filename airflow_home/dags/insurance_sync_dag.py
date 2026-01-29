"""
Insurance Plans Sync DAG (import-light)

Scrapes medical/health insurance plans from Malaysian insurance companies:
- AIA Malaysia
- Prudential BSN
- Allianz Malaysia
- Great Eastern
- Etiqa Insurance

Important: keep DAG module import fast (< 30s), otherwise Airflow will mark it as
"Broken DAG: DagBag import timeout". Heavy imports (Playwright, Supabase) and
Airflow DB calls should happen inside tasks, not at module import time.
"""

import asyncio
import logging
import os
from datetime import datetime, timedelta
from typing import Any, Dict, List

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable

logger = logging.getLogger(__name__)


def _get_cfg() -> Dict[str, Any]:
    """
    Resolve runtime config. Prefer env vars (fast) and only hit Airflow Variables
    at task runtime.
    """
    def _v_bool(key: str, default: bool) -> bool:
        raw = os.environ.get(key)
        if raw is None:
            try:
                raw = Variable.get(key, default_var=str(default).lower())
            except Exception:
                raw = str(default).lower()
        return str(raw).lower() == "true"

    return {
        "headless": _v_bool("INSURANCE_SCRAPER_HEADLESS", True),
    }


def scrape_aia(**context) -> List[Dict[str, Any]]:
    """Scrape AIA Malaysia insurance plans."""
    from helpers.insurance_scraper import InsuranceScraper  # lazy import
    
    cfg = _get_cfg()
    logger.info("Starting AIA scrape...")
    
    async def _scrape():
        async with InsuranceScraper(headless=bool(cfg["headless"])) as scraper:
            return await scraper.scrape_aia()
    
    plans = asyncio.run(_scrape())
    logger.info(f"Scraped {len(plans)} plans from AIA")
    context["ti"].xcom_push(key="aia_plans", value=plans)
    return plans


def scrape_prudential(**context) -> List[Dict[str, Any]]:
    """Scrape Prudential BSN insurance plans."""
    from helpers.insurance_scraper import InsuranceScraper  # lazy import
    
    cfg = _get_cfg()
    logger.info("Starting Prudential scrape...")
    
    async def _scrape():
        async with InsuranceScraper(headless=bool(cfg["headless"])) as scraper:
            return await scraper.scrape_prudential()
    
    plans = asyncio.run(_scrape())
    logger.info(f"Scraped {len(plans)} plans from Prudential")
    context["ti"].xcom_push(key="prudential_plans", value=plans)
    return plans


def scrape_allianz(**context) -> List[Dict[str, Any]]:
    """Scrape Allianz Malaysia insurance plans."""
    from helpers.insurance_scraper import InsuranceScraper  # lazy import
    
    cfg = _get_cfg()
    logger.info("Starting Allianz scrape...")
    
    async def _scrape():
        async with InsuranceScraper(headless=bool(cfg["headless"])) as scraper:
            return await scraper.scrape_allianz()
    
    plans = asyncio.run(_scrape())
    logger.info(f"Scraped {len(plans)} plans from Allianz")
    context["ti"].xcom_push(key="allianz_plans", value=plans)
    return plans


def scrape_great_eastern(**context) -> List[Dict[str, Any]]:
    """Scrape Great Eastern insurance plans."""
    from helpers.insurance_scraper import InsuranceScraper  # lazy import
    
    cfg = _get_cfg()
    logger.info("Starting Great Eastern scrape...")
    
    async def _scrape():
        async with InsuranceScraper(headless=bool(cfg["headless"])) as scraper:
            return await scraper.scrape_great_eastern()
    
    plans = asyncio.run(_scrape())
    logger.info(f"Scraped {len(plans)} plans from Great Eastern")
    context["ti"].xcom_push(key="great_eastern_plans", value=plans)
    return plans


def scrape_etiqa(**context) -> List[Dict[str, Any]]:
    """Scrape Etiqa insurance plans."""
    from helpers.insurance_scraper import InsuranceScraper  # lazy import
    
    cfg = _get_cfg()
    logger.info("Starting Etiqa scrape...")
    
    async def _scrape():
        async with InsuranceScraper(headless=bool(cfg["headless"])) as scraper:
            return await scraper.scrape_etiqa()
    
    plans = asyncio.run(_scrape())
    logger.info(f"Scraped {len(plans)} plans from Etiqa")
    context["ti"].xcom_push(key="etiqa_plans", value=plans)
    return plans


def update_supabase(**context) -> Dict[str, Any]:
    """Update Supabase with all scraped insurance plans using smart sync."""
    from helpers.supabase_client import SupabaseClient  # lazy import
    
    ti = context["ti"]
    
    # Collect all plans from XCom
    all_plans = []
    provider_stats = {}
    
    xcom_keys = [
        ("aia_plans", "scrape_aia", "AIA"),
        ("prudential_plans", "scrape_prudential", "Prudential"),
        ("allianz_plans", "scrape_allianz", "Allianz"),
        ("great_eastern_plans", "scrape_great_eastern", "GreatEastern"),
        ("etiqa_plans", "scrape_etiqa", "Etiqa"),
    ]
    
    for xcom_key, task_id, provider_key in xcom_keys:
        plans = ti.xcom_pull(key=xcom_key, task_ids=task_id) or []
        all_plans.extend(plans)
        provider_stats[provider_key] = len(plans)
        logger.info(f"Retrieved {len(plans)} scraped plans from {provider_key}")
    
    if not all_plans:
        logger.warning("No insurance plans scraped - nothing to sync")
        return {
            "total_scraped": 0,
            "new": 0,
            "updated": 0,
            "unchanged": 0,
            "errors": 0,
            "provider_stats": provider_stats,
        }
    
    logger.info(f"Total plans scraped: {len(all_plans)}")
    
    # Smart sync to Supabase (only insert new / update changed)
    supabase = SupabaseClient()
    result = supabase.upsert_insurance_plans(all_plans)
    
    # Log detailed sync statistics
    logger.info("=" * 60)
    logger.info("INSURANCE SYNC SUMMARY")
    logger.info("=" * 60)
    logger.info(f"Total scraped:  {len(all_plans)}")
    logger.info(f"New plans:      {result.get('new', 0)}")
    logger.info(f"Updated plans:  {result.get('updated', 0)}")
    logger.info(f"Unchanged:      {result.get('unchanged', 0)}")
    logger.info(f"Errors:         {result.get('errors', 0)}")
    logger.info("-" * 60)
    logger.info("Plans scraped per provider:")
    for provider, count in provider_stats.items():
        logger.info(f"  {provider}: {count}")
    logger.info("=" * 60)
    
    return {
        "total_scraped": len(all_plans),
        "new": result.get("new", 0),
        "updated": result.get("updated", 0),
        "unchanged": result.get("unchanged", 0),
        "errors": result.get("errors", 0),
        "provider_stats": provider_stats,
    }


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=15),
}


with DAG(
    dag_id="insurance_sync",
    default_args=default_args,
    description="Sync insurance plans from Malaysian insurance company websites daily",
    schedule_interval="0 3 * * *",  # Run at 3 AM daily
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["insurance", "scraping", "sync"],
    max_active_runs=1,
) as dag:
    
    # Scrape tasks for each provider
    scrape_aia_task = PythonOperator(
        task_id="scrape_aia",
        python_callable=scrape_aia,
        execution_timeout=timedelta(minutes=30),
    )
    
    scrape_prudential_task = PythonOperator(
        task_id="scrape_prudential",
        python_callable=scrape_prudential,
        execution_timeout=timedelta(minutes=30),
    )
    
    scrape_allianz_task = PythonOperator(
        task_id="scrape_allianz",
        python_callable=scrape_allianz,
        execution_timeout=timedelta(minutes=30),
    )
    
    scrape_great_eastern_task = PythonOperator(
        task_id="scrape_great_eastern",
        python_callable=scrape_great_eastern,
        execution_timeout=timedelta(minutes=30),
    )
    
    scrape_etiqa_task = PythonOperator(
        task_id="scrape_etiqa",
        python_callable=scrape_etiqa,
        execution_timeout=timedelta(minutes=30),
    )
    
    # Update database task
    update_supabase_task = PythonOperator(
        task_id="update_supabase",
        python_callable=update_supabase,
        execution_timeout=timedelta(minutes=15),
    )
    
    # Sequential execution to avoid rate limiting
    (
        scrape_aia_task
        >> scrape_prudential_task
        >> scrape_allianz_task
        >> scrape_great_eastern_task
        >> scrape_etiqa_task
        >> update_supabase_task
    )

