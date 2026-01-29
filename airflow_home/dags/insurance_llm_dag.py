"""Insurance LLM-Powered Scraper DAG.

This DAG scrapes health insurance products from all Malaysian insurance providers
using GPT-4o to analyze PDF brochures and extract structured data.

Providers:
- AIA Malaysia
- Allianz Malaysia
- Great Eastern Life
- Etiqa Insurance
- Prudential Malaysia

Schedule: Weekly (Sunday at 2 AM)
- More expensive than regular scraping due to LLM costs
- Provides detailed, accurate data extraction from PDFs
- Estimated cost per run: ~$0.50-$1.00

Required Environment Variables:
- OPENAI_API_KEY: OpenAI API key for GPT-4o
- SUPABASE_URL: Supabase project URL
- SUPABASE_SERVICE_ROLE_KEY: Supabase service role key
"""

import logging
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

logger = logging.getLogger(__name__)

# DAG default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=15),
}


def scrape_all_providers_task(**context):
    """
    Scrape all insurance providers using LLM-powered PDF analysis.
    
    Providers: AIA, Allianz, Great Eastern, Etiqa, Prudential
    Returns scraped plans via XCom for downstream processing.
    """
    from helpers.insurance_scraper import scrape_all_providers_with_llm_sync
    
    logger.info("Starting LLM-powered scraping for all insurance providers...")
    
    # Check for API key
    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise ValueError("OPENAI_API_KEY environment variable is not set")
    
    # Get headless setting from environment (default True)
    headless = os.environ.get("ENRICHMENT_HEADLESS", "true").lower() == "true"
    
    # Scrape all providers with LLM analysis
    plans = scrape_all_providers_with_llm_sync(headless=headless, openai_api_key=api_key)
    
    logger.info(f"Scraped {len(plans)} total plans from all providers with LLM analysis")
    
    # Push to XCom for downstream tasks
    context["ti"].xcom_push(key="all_plans", value=plans)
    
    return len(plans)


def update_supabase_task(**context):
    """
    Update Supabase with scraped insurance plans.
    
    Uses smart sync to only update changed/new plans.
    """
    from helpers.supabase_client import SupabaseClient
    
    # Get plans from XCom
    ti = context["ti"]
    plans = ti.xcom_pull(task_ids="scrape_all_providers", key="all_plans") or []
    
    if not plans:
        logger.warning("No plans to update in Supabase")
        return {"new": 0, "updated": 0, "unchanged": 0, "errors": 0}
    
    logger.info(f"Updating Supabase with {len(plans)} plans...")
    
    # Initialize client
    client = SupabaseClient()
    
    # Use smart upsert to handle changes
    result = client.smart_upsert_insurance_plans(plans)
    
    logger.info(
        f"Supabase update complete: "
        f"{result['new']} new, {result['updated']} updated, "
        f"{result['unchanged']} unchanged, {result['errors']} errors"
    )
    
    return result


def log_summary_task(**context):
    """Log a summary of the scraping run."""
    ti = context["ti"]
    
    plans_count = ti.xcom_pull(task_ids="scrape_all_providers")
    db_result = ti.xcom_pull(task_ids="update_supabase")
    
    logger.info("=" * 60)
    logger.info("Insurance LLM Scraper Summary")
    logger.info("=" * 60)
    logger.info("Providers: AIA, Allianz, Great Eastern, Etiqa, Prudential")
    logger.info(f"Total plans scraped: {plans_count or 0}")
    
    if db_result:
        logger.info(f"New plans added: {db_result.get('new', 0)}")
        logger.info(f"Plans updated: {db_result.get('updated', 0)}")
        logger.info(f"Plans unchanged: {db_result.get('unchanged', 0)}")
        logger.info(f"Errors: {db_result.get('errors', 0)}")
    
    logger.info("=" * 60)


# Create the DAG
with DAG(
    dag_id="insurance_scraper",
    default_args=default_args,
    description="Scrape all Malaysian insurance providers using LLM-powered PDF analysis",
    schedule_interval="0 2 * * 0",  # Weekly on Sunday at 2 AM
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["insurance", "scraping", "llm", "health", "medical"],
    doc_md=__doc__,
) as dag:
    
    # Task 1: Scrape all providers with LLM
    scrape_task = PythonOperator(
        task_id="scrape_all_providers",
        python_callable=scrape_all_providers_task,
        execution_timeout=timedelta(minutes=180),  # 3 hours for all providers
    )
    
    # Task 2: Update Supabase
    update_task = PythonOperator(
        task_id="update_supabase",
        python_callable=update_supabase_task,
        execution_timeout=timedelta(minutes=15),
    )
    
    # Task 3: Log summary
    summary_task = PythonOperator(
        task_id="log_summary",
        python_callable=log_summary_task,
    )
    
    # Set dependencies
    scrape_task >> update_task >> summary_task

