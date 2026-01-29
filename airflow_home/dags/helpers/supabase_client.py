"""Supabase client for upserting clinic and insurance data."""

import os
import hashlib
import logging
from datetime import datetime, timezone
from typing import List, Dict, Any

from supabase import create_client, Client

logger = logging.getLogger(__name__)


class SupabaseClient:
    """Client for upserting clinic and insurance data to Supabase."""

    TABLE_NAME = "Clinic Facilities"
    INSURANCE_TABLE_NAME = "Insurance Plans"

    def __init__(self, url: str = None, key: str = None):
        """
        Initialize the Supabase client.

        Args:
            url: Supabase project URL. If None, uses SUPABASE_URL env var.
            key: Supabase service role key. If None, uses SUPABASE_SERVICE_ROLE_KEY env var.
        """
        if url is None:
            url = os.environ.get("SUPABASE_URL")
        if key is None:
            key = os.environ.get("SUPABASE_SERVICE_ROLE_KEY")

        if not url or not key:
            raise ValueError(
                "url and key must be provided or "
                "SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY env vars must be set"
            )

        self.client: Client = create_client(url, key)

    @staticmethod
    def generate_deterministic_id(name: str, address: str) -> str:
        """
        Generate a deterministic UUID-like ID from name and address.

        This ensures the same clinic always gets the same ID for upsert.

        Args:
            name: Clinic name.
            address: Clinic address.

        Returns:
            UUID string derived from the hash.
        """
        # Create a deterministic hash
        content = f"{name.lower().strip()}|{address.lower().strip()}"
        hash_bytes = hashlib.sha256(content.encode()).digest()

        # Format as UUID (8-4-4-4-12)
        hex_str = hash_bytes.hex()[:32]
        uuid_str = f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"

        return uuid_str

    def transform_for_insert(self, clinic: Dict[str, Any]) -> Dict[str, Any]:
        """
        Transform enriched clinic data to match Supabase table schema.

        Args:
            clinic: Enriched clinic dict from the pipeline.

        Returns:
            Dict matching the Supabase table schema.
        """
        name = clinic.get("name", "")
        address = clinic.get("address", "")

        # Generate deterministic ID for upsert
        clinic_id = self.generate_deterministic_id(name, address)

        # Check for 24 hours in name
        is_24_hours = "24 JAM" in name.upper() or "24JAM" in name.upper()

        now = datetime.now(timezone.utc).isoformat()

        record: Dict[str, Any] = {
            "id": clinic_id,
            "updated_at": now,
            "name": name,
            "facility_type": clinic.get("facility_type", ""),
            "address": address or None,
            "city": clinic.get("city") or None,
            "state": clinic.get("state") or None,
            "postcode": clinic.get("postcode") or None,
            "email": None,  # Not available in source data
            "is_24_hours": is_24_hours,
            "has_emergency": None,  # Not available in source data
            "is_government": False,  # These are private clinics
            "is_active": True,
        }

        # IMPORTANT:
        # Only include enrichment fields in upserts when they are explicitly present
        # in the input dict. Otherwise, a daily sync run would overwrite existing
        # scraped values with NULL.
        enrichment_keys = [
            "latitude",
            "longitude",
            "phone",
            "website",
            "operating_hours",
            "services",
            "specialties",
            "accepted_insurance",
            "has_emergency",
            "google_place_id",
            "google_rating",
            "email",
        ]
        for k in enrichment_keys:
            if k in clinic:
                record[k] = clinic.get(k)

        return record

    def upsert_clinics(self, clinics: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Upsert clinic records to Supabase.

        Args:
            clinics: List of enriched clinic dicts.

        Returns:
            Dict with upsert statistics.
        """
        if not clinics:
            logger.warning("No clinics to upsert")
            return {"inserted": 0, "updated": 0, "errors": 0}

        # Transform all clinics
        records = [self.transform_for_insert(c) for c in clinics]

        # Deduplicate by id (keep first occurrence)
        seen_ids = set()
        unique_records = []
        for record in records:
            if record["id"] not in seen_ids:
                seen_ids.add(record["id"])
                unique_records.append(record)
            else:
                logger.debug(f"Skipping duplicate: {record['name']}")

        records = unique_records
        logger.info(f"Upserting {len(records)} unique clinic records to Supabase (removed {len(clinics) - len(records)} duplicates)")

        # Batch upsert to avoid payload size limits
        BATCH_SIZE = 500
        total_success = 0
        total_errors = 0

        for i in range(0, len(records), BATCH_SIZE):
            batch = records[i : i + BATCH_SIZE]
            batch_num = i // BATCH_SIZE + 1
            total_batches = (len(records) + BATCH_SIZE - 1) // BATCH_SIZE

            try:
                logger.info(f"Upserting batch {batch_num}/{total_batches} ({len(batch)} records)")
                response = (
                    self.client.table(self.TABLE_NAME)
                    .upsert(batch, on_conflict="id")
                    .execute()
                )
                batch_success = len(response.data) if response.data else 0
                total_success += batch_success

            except Exception as e:
                logger.error(f"Batch {batch_num} failed: {e}")
                total_errors += len(batch)

        logger.info(f"Upsert complete: {total_success} success, {total_errors} errors")

        return {
            "processed": len(records),
            "success": total_success,
            "errors": total_errors,
        }

    def upsert_single(self, clinic: Dict[str, Any]) -> bool:
        """
        Upsert a single clinic record.

        Args:
            clinic: Enriched clinic dict.

        Returns:
            True if successful, False otherwise.
        """
        try:
            record = self.transform_for_insert(clinic)
            self.client.table(self.TABLE_NAME).upsert(
                record, on_conflict="id"
            ).execute()
            return True
        except Exception as e:
            logger.error(f"Failed to upsert clinic {clinic.get('name')}: {e}")
            return False

    def get_unenriched_clinics(self, limit: int = 50) -> List[Dict[str, Any]]:
        """
        Get clinics that haven't been enriched yet (latitude IS NULL).

        Args:
            limit: Maximum number of clinics to return.

        Returns:
            List of clinic dicts with basic info.
        """
        try:
            response = (
                self.client.table(self.TABLE_NAME)
                .select("id, name, address, city, state, postcode, facility_type")
                .is_("latitude", "null")
                .eq("is_active", True)
                .limit(limit)
                .execute()
            )
            clinics = response.data if response.data else []
            logger.info(f"Found {len(clinics)} unenriched clinics")
            return clinics
        except Exception as e:
            logger.error(f"Failed to get unenriched clinics: {e}")
            return []

    def get_unenriched_count(self) -> int:
        """
        Get count of clinics that haven't been enriched yet.

        Returns:
            Count of unenriched clinics.
        """
        try:
            response = (
                self.client.table(self.TABLE_NAME)
                .select("id", count="exact")
                .is_("latitude", "null")
                .eq("is_active", True)
                .execute()
            )
            return response.count if response.count else 0
        except Exception as e:
            logger.error(f"Failed to get unenriched count: {e}")
            return 0

    def update_enrichment(self, clinic_id: str, enrichment_data: Dict[str, Any]) -> bool:
        """
        Update a clinic with enrichment data from scraping.

        Args:
            clinic_id: The clinic's UUID.
            enrichment_data: Dict with enrichment fields (latitude, longitude, phone, etc.)

        Returns:
            True if successful, False otherwise.
        """
        try:
            now = datetime.now(timezone.utc).isoformat()

            update_data = {
                "updated_at": now,
                "latitude": enrichment_data.get("latitude"),
                "longitude": enrichment_data.get("longitude"),
                "phone": enrichment_data.get("phone"),
                "website": enrichment_data.get("website"),
                "operating_hours": enrichment_data.get("operating_hours"),
                "google_place_id": enrichment_data.get("google_place_id"),
                "google_rating": enrichment_data.get("google_rating"),
            }

            # Add services and specialties if available
            if enrichment_data.get("services"):
                update_data["services"] = enrichment_data["services"]
            if enrichment_data.get("specialties"):
                update_data["specialties"] = enrichment_data["specialties"]

            # Remove None values to avoid overwriting existing data with nulls
            # But keep latitude/longitude even if None (to mark as "attempted")
            update_data = {
                k: v for k, v in update_data.items()
                if v is not None or k in ("latitude", "longitude")
            }

            self.client.table(self.TABLE_NAME).update(update_data).eq("id", clinic_id).execute()
            logger.debug(f"Updated enrichment for clinic {clinic_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to update enrichment for {clinic_id}: {e}")
            return False

    def mark_enrichment_failed(self, clinic_id: str) -> bool:
        """
        Mark a clinic as having failed enrichment (set latitude to 0 as marker).

        This prevents re-attempting failed clinics immediately.

        Args:
            clinic_id: The clinic's UUID.

        Returns:
            True if successful, False otherwise.
        """
        try:
            now = datetime.now(timezone.utc).isoformat()
            self.client.table(self.TABLE_NAME).update({
                "updated_at": now,
                "latitude": 0.0,  # Marker for "attempted but failed"
                "longitude": 0.0,
            }).eq("id", clinic_id).execute()
            return True
        except Exception as e:
            logger.error(f"Failed to mark enrichment failed for {clinic_id}: {e}")
            return False

    # ==================== Insurance Plans Methods ====================

    # Fields to compare for change detection
    INSURANCE_COMPARE_FIELDS = [
        "plan_name", "plan_type", "coverage_type",
        "annual_limit", "lifetime_limit", "room_board_limit",
        "outpatient_covered", "maternity_covered", "dental_covered",
        "optical_covered", "mental_health_covered",
        "monthly_premium_min", "monthly_premium_max", "deductible",
        "co_payment_percentage", "min_age", "max_age",
        "panel_hospitals", "covered_conditions", "excluded_conditions",
        "claim_process", "contact_phone", "website",
    ]

    def get_existing_insurance_plans_by_provider(self, provider_name: str) -> Dict[str, Dict[str, Any]]:
        """
        Get existing insurance plans for a provider, indexed by ID.

        Args:
            provider_name: The provider name to filter by.

        Returns:
            Dict mapping plan ID to plan data.
        """
        try:
            response = (
                self.client.table(self.INSURANCE_TABLE_NAME)
                .select("*")
                .eq("provider_name", provider_name)
                .eq("is_active", True)
                .execute()
            )
            plans = response.data if response.data else []
            return {p["id"]: p for p in plans}
        except Exception as e:
            logger.error(f"Failed to get existing plans for {provider_name}: {e}")
            return {}

    def plan_has_changed(self, existing_plan: Dict[str, Any], new_plan: Dict[str, Any]) -> bool:
        """
        Compare two plans to determine if there are meaningful changes.

        A plan is considered changed if any non-null scraped field differs from existing.

        Args:
            existing_plan: The plan currently in Supabase.
            new_plan: The newly scraped plan data.

        Returns:
            True if the plan has changed, False otherwise.
        """
        for field in self.INSURANCE_COMPARE_FIELDS:
            new_value = new_plan.get(field)
            existing_value = existing_plan.get(field)
            
            # Only compare if new value is not None (we don't overwrite with nulls)
            if new_value is not None:
                # Handle list/dict comparisons
                if isinstance(new_value, (list, dict)):
                    if new_value != existing_value:
                        logger.debug(f"Plan changed: {field} differs")
                        return True
                # Handle numeric comparisons with tolerance
                elif isinstance(new_value, float) and isinstance(existing_value, (int, float)):
                    if abs(new_value - float(existing_value)) > 0.01:
                        logger.debug(f"Plan changed: {field} differs ({existing_value} -> {new_value})")
                        return True
                # Standard comparison
                elif new_value != existing_value:
                    logger.debug(f"Plan changed: {field} differs ({existing_value} -> {new_value})")
                    return True
        
        return False

    def smart_upsert_insurance_plans(self, plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Smart upsert that only inserts new plans or updates changed plans.

        Args:
            plans: List of normalized insurance plan dicts.

        Returns:
            Dict with detailed sync statistics.
        """
        if not plans:
            logger.warning("No insurance plans to sync")
            return {"new": 0, "updated": 0, "unchanged": 0, "errors": 0}

        # Deduplicate by id (keep first occurrence)
        seen_ids = set()
        unique_plans = []
        for plan in plans:
            plan_id = plan.get("id")
            if plan_id and plan_id not in seen_ids:
                seen_ids.add(plan_id)
                unique_plans.append(plan)
            elif not plan_id:
                logger.warning(f"Plan missing id: {plan.get('plan_name')}")

        logger.info(f"Processing {len(unique_plans)} unique scraped plans")

        # Group plans by provider
        plans_by_provider: Dict[str, List[Dict[str, Any]]] = {}
        for plan in unique_plans:
            provider = plan.get("provider_name", "Unknown")
            if provider not in plans_by_provider:
                plans_by_provider[provider] = []
            plans_by_provider[provider].append(plan)

        stats = {"new": 0, "updated": 0, "unchanged": 0, "errors": 0}
        now = datetime.now(timezone.utc).isoformat()

        for provider_name, provider_plans in plans_by_provider.items():
            logger.info(f"Syncing {len(provider_plans)} plans for {provider_name}")
            
            # Get existing plans for this provider
            existing_plans = self.get_existing_insurance_plans_by_provider(provider_name)
            logger.info(f"Found {len(existing_plans)} existing plans in Supabase for {provider_name}")

            plans_to_insert = []
            plans_to_update = []

            for plan in provider_plans:
                plan_id = plan.get("id")
                existing = existing_plans.get(plan_id)

                if existing is None:
                    # New plan - needs insert
                    plan["created_at"] = now
                    plan["updated_at"] = now
                    plans_to_insert.append(plan)
                elif self.plan_has_changed(existing, plan):
                    # Existing plan with changes - needs update
                    plan["updated_at"] = now
                    # Preserve created_at from existing record
                    plan["created_at"] = existing.get("created_at", now)
                    plans_to_update.append(plan)
                else:
                    # No changes - skip
                    stats["unchanged"] += 1

            # Batch insert new plans
            if plans_to_insert:
                logger.info(f"Inserting {len(plans_to_insert)} new plans for {provider_name}")
                try:
                    response = (
                        self.client.table(self.INSURANCE_TABLE_NAME)
                        .insert(plans_to_insert)
                        .execute()
                    )
                    inserted = len(response.data) if response.data else 0
                    stats["new"] += inserted
                    if inserted < len(plans_to_insert):
                        stats["errors"] += len(plans_to_insert) - inserted
                except Exception as e:
                    logger.error(f"Failed to insert new plans for {provider_name}: {e}")
                    stats["errors"] += len(plans_to_insert)

            # Batch update changed plans
            if plans_to_update:
                logger.info(f"Updating {len(plans_to_update)} changed plans for {provider_name}")
                try:
                    response = (
                        self.client.table(self.INSURANCE_TABLE_NAME)
                        .upsert(plans_to_update, on_conflict="id")
                        .execute()
                    )
                    updated = len(response.data) if response.data else 0
                    stats["updated"] += updated
                    if updated < len(plans_to_update):
                        stats["errors"] += len(plans_to_update) - updated
                except Exception as e:
                    logger.error(f"Failed to update plans for {provider_name}: {e}")
                    stats["errors"] += len(plans_to_update)

        logger.info(
            f"Insurance sync complete: {stats['new']} new, {stats['updated']} updated, "
            f"{stats['unchanged']} unchanged, {stats['errors']} errors"
        )
        return stats

    def upsert_insurance_plans(self, plans: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Upsert insurance plan records using smart sync (only update changed plans).

        Args:
            plans: List of normalized insurance plan dicts.

        Returns:
            Dict with sync statistics.
        """
        # Use smart sync for better efficiency
        result = self.smart_upsert_insurance_plans(plans)
        
        # Return in legacy format for backwards compatibility
        return {
            "processed": result["new"] + result["updated"] + result["unchanged"],
            "success": result["new"] + result["updated"],
            "errors": result["errors"],
            "new": result["new"],
            "updated": result["updated"],
            "unchanged": result["unchanged"],
        }

    def get_insurance_plan_count(self, provider_name: str = None) -> int:
        """
        Get count of active insurance plans.

        Args:
            provider_name: Optional filter by provider name.

        Returns:
            Count of insurance plans.
        """
        try:
            query = (
                self.client.table(self.INSURANCE_TABLE_NAME)
                .select("id", count="exact")
                .eq("is_active", True)
            )
            if provider_name:
                query = query.eq("provider_name", provider_name)
            
            response = query.execute()
            return response.count if response.count else 0
        except Exception as e:
            logger.error(f"Failed to get insurance plan count: {e}")
            return 0

    def get_all_insurance_plans(self, provider_name: str = None) -> List[Dict[str, Any]]:
        """
        Get all active insurance plans.

        Args:
            provider_name: Optional filter by provider name.

        Returns:
            List of insurance plan dicts.
        """
        try:
            query = (
                self.client.table(self.INSURANCE_TABLE_NAME)
                .select("*")
                .eq("is_active", True)
            )
            if provider_name:
                query = query.eq("provider_name", provider_name)
            
            response = query.execute()
            return response.data if response.data else []
        except Exception as e:
            logger.error(f"Failed to get insurance plans: {e}")
            return []

    def deactivate_old_plans(self, provider_name: str, active_plan_ids: List[str]) -> int:
        """
        Deactivate plans that are no longer found on the provider's website.

        This soft-deletes plans by setting is_active=False for plans not in
        the active_plan_ids list.

        Args:
            provider_name: The provider name to filter by.
            active_plan_ids: List of plan IDs that are still active.

        Returns:
            Number of plans deactivated.
        """
        try:
            # Get current active plans for this provider
            response = (
                self.client.table(self.INSURANCE_TABLE_NAME)
                .select("id")
                .eq("provider_name", provider_name)
                .eq("is_active", True)
                .execute()
            )
            
            current_plans = response.data if response.data else []
            current_ids = {p["id"] for p in current_plans}
            
            # Find plans to deactivate
            ids_to_deactivate = current_ids - set(active_plan_ids)
            
            if not ids_to_deactivate:
                logger.info(f"No plans to deactivate for {provider_name}")
                return 0

            # Deactivate in batches
            now = datetime.now(timezone.utc).isoformat()
            deactivated = 0
            
            for plan_id in ids_to_deactivate:
                try:
                    self.client.table(self.INSURANCE_TABLE_NAME).update({
                        "is_active": False,
                        "updated_at": now,
                    }).eq("id", plan_id).execute()
                    deactivated += 1
                except Exception as e:
                    logger.warning(f"Failed to deactivate plan {plan_id}: {e}")

            logger.info(f"Deactivated {deactivated} old plans for {provider_name}")
            return deactivated

        except Exception as e:
            logger.error(f"Failed to deactivate old plans for {provider_name}: {e}")
            return 0

    def upsert_single_insurance_plan(self, plan: Dict[str, Any]) -> bool:
        """
        Upsert a single insurance plan record.

        Args:
            plan: Normalized insurance plan dict.

        Returns:
            True if successful, False otherwise.
        """
        try:
            now = datetime.now(timezone.utc).isoformat()
            plan["updated_at"] = now
            if "created_at" not in plan:
                plan["created_at"] = now

            self.client.table(self.INSURANCE_TABLE_NAME).upsert(
                plan, on_conflict="id"
            ).execute()
            return True
        except Exception as e:
            logger.error(f"Failed to upsert insurance plan {plan.get('plan_name')}: {e}")
            return False

