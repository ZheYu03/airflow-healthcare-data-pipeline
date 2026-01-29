"""
Clinic Facilities Sync DAG

This DAG checks if the Google Sheet has been modified, and if so,
reads clinic data and upserts the basic data to Supabase.
Enrichment (lat/lng, phone, rating, etc.) is handled separately
by the clinic_enrichment_dag which runs hourly.

Schedule: Daily at 2:00 AM UTC
"""

import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from airflow.models import Variable

# Import helper modules
from helpers.sheets_client import SheetsClient
from helpers.supabase_client import SupabaseClient
from helpers.drive_client import DriveClient

logger = logging.getLogger(__name__)

# Configuration - can be overridden via Airflow Variables
SPREADSHEET_ID = Variable.get(
    "CLINIC_SPREADSHEET_ID",
    default_var="1juukIEirv0BytdVrQYpGfjdhr4rhJnEAho0KmbwKy8A"
)
WORKSHEET_NAME = Variable.get(
    "CLINIC_WORKSHEET_NAME",
    default_var="KLINIK PERUBATAN SWASTA"
)

# Default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


def check_sheet_modified(**context) -> bool:
    """
    Check if the Google Sheet has been modified since last sync.
    Uses Google Drive API to compare modifiedTime.

    Returns:
        True if sheet has changed and should sync, False to skip.
    """
    logger.info(f"Checking if spreadsheet {SPREADSHEET_ID} has been modified")

    # Get last known modified time from Airflow Variable
    last_modified = Variable.get("SHEET_LAST_MODIFIED", default_var=None)
    logger.info(f"Last known modified time: {last_modified}")

    try:
        drive_client = DriveClient()
        has_changed = drive_client.has_file_changed(SPREADSHEET_ID, last_modified)

        if has_changed:
            # Get and store the new modified time
            new_modified = drive_client.get_file_modified_time(SPREADSHEET_ID)
            if new_modified:
                context["ti"].xcom_push(key="new_modified_time", value=new_modified)

            logger.info("Sheet has changed - proceeding with sync")
            return True
        else:
            logger.info("Sheet has not changed - skipping sync")
            return False

    except Exception as e:
        logger.error(f"Error checking sheet modification: {e}")
        # On error, proceed with sync to be safe
        logger.info("Proceeding with sync due to check error")
        return True


def extract_from_sheets(**context) -> List[Dict[str, Any]]:
    """
    Extract clinic data from Google Sheets.

    Returns:
        List of clinic dicts with raw data from the sheet.
    """
    logger.info("Starting extraction from Google Sheets")

    sheets_client = SheetsClient()
    clinics = sheets_client.read_clinics(
        spreadsheet_id=SPREADSHEET_ID,
        worksheet_name=WORKSHEET_NAME,
        skip_rows=2,
    )

    logger.info(f"Extracted {len(clinics)} clinics from sheet")

    # Push to XCom for next task
    context["ti"].xcom_push(key="raw_clinics", value=clinics)

    return clinics


def transform_data(**context) -> List[Dict[str, Any]]:
    """
    Transform raw clinic data to match Supabase schema.

    Note: Enrichment (lat/lng, phone, etc.) is handled by the separate
    clinic_enrichment_dag which scrapes Google Maps hourly.

    Returns:
        List of transformed clinic dicts.
    """
    # Pull raw data from previous task
    ti = context["ti"]
    raw_clinics = ti.xcom_pull(key="raw_clinics", task_ids="extract_from_sheets")

    if not raw_clinics:
        logger.warning("No raw clinics to transform")
        return []

    logger.info(f"Transforming {len(raw_clinics)} clinics")

    # Map sheet columns to our schema
    transformed = []
    for clinic in raw_clinics:
        transformed_clinic = {
            "name": clinic.get("nama_penuh_fasiliti", ""),
            "facility_type": clinic.get("jenis_fasiliti", ""),
            "address": clinic.get("alamat", ""),
            "postcode": clinic.get("poskod", ""),
            "city": clinic.get("bandar", ""),
            "state": clinic.get("negeri", ""),
            # IMPORTANT: do NOT include enrichment fields here.
            # If we upsert NULLs for these, we will wipe scraped values.
        }
        transformed.append(transformed_clinic)

    logger.info(f"Transformed {len(transformed)} clinics")

    # Push to XCom
    ti.xcom_push(key="transformed_clinics", value=transformed)

    return transformed


def upsert_to_supabase(**context) -> Dict[str, Any]:
    """
    Upsert clinic data to Supabase.

    Uses upsert with ON CONFLICT to update existing records or insert new ones.
    Enrichment fields (lat/lng, phone, etc.) are preserved if already set.

    Returns:
        Dict with upsert statistics.
    """
    # Pull transformed data from previous task
    ti = context["ti"]
    transformed_clinics = ti.xcom_pull(
        key="transformed_clinics", task_ids="transform_data"
    )

    if not transformed_clinics:
        logger.warning("No clinics to upsert")
        return {"processed": 0, "success": 0, "errors": 0}

    logger.info(f"Upserting {len(transformed_clinics)} clinics to Supabase")

    supabase_client = SupabaseClient()
    result = supabase_client.upsert_clinics(transformed_clinics)

    # Log count of unenriched clinics for visibility
    unenriched = supabase_client.get_unenriched_count()
    logger.info(f"Upsert complete: {result}")
    logger.info(f"Clinics awaiting enrichment: {unenriched}")

    return result


def update_last_modified(**context) -> None:
    """
    Update the SHEET_LAST_MODIFIED variable after successful sync.
    """
    ti = context["ti"]
    new_modified = ti.xcom_pull(key="new_modified_time", task_ids="check_sheet_modified")

    if new_modified:
        Variable.set("SHEET_LAST_MODIFIED", new_modified)
        logger.info(f"Updated SHEET_LAST_MODIFIED to {new_modified}")
    else:
        logger.warning("No new modified time to update")


# Define the DAG
with DAG(
    dag_id="clinic_facilities_sync",
    default_args=default_args,
    description="Sync clinic facilities from Google Sheets to Supabase (with change detection)",
    schedule_interval="0 2 * * *",  # Daily at 2:00 AM UTC
    start_date=datetime(2025, 12, 1),
    catchup=False,
    tags=["clinic", "sync", "supabase", "google-sheets"],
) as dag:

    # Task 1: Check if sheet has been modified (short-circuit if not)
    check_task = ShortCircuitOperator(
        task_id="check_sheet_modified",
        python_callable=check_sheet_modified,
        provide_context=True,
    )

    # Task 2: Extract data from Google Sheets
    extract_task = PythonOperator(
        task_id="extract_from_sheets",
        python_callable=extract_from_sheets,
        provide_context=True,
    )

    # Task 3: Transform data to match schema
    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True,
    )

    # Task 4: Upsert to Supabase
    upsert_task = PythonOperator(
        task_id="upsert_to_supabase",
        python_callable=upsert_to_supabase,
        provide_context=True,
    )

    # Task 5: Update last modified timestamp
    update_modified_task = PythonOperator(
        task_id="update_last_modified",
        python_callable=update_last_modified,
        provide_context=True,
    )

    # Define task dependencies
    check_task >> extract_task >> transform_task >> upsert_task >> update_modified_task
