"""Google Places API enricher for clinic data."""

import os
import time
import logging
from typing import Dict, Any, Optional

import googlemaps

logger = logging.getLogger(__name__)

# Rate limiting: Google Places allows 10 QPS for most endpoints
RATE_LIMIT_DELAY = 0.15  # 150ms between requests to stay under 10 QPS


class PlacesEnricher:
    """Enriches clinic data with Google Places API information."""

    def __init__(self, api_key: str = None):
        """
        Initialize the Places API client.

        Args:
            api_key: Google Places API key. If None, uses GOOGLE_PLACES_API_KEY env var.
        """
        if api_key is None:
            api_key = os.environ.get("GOOGLE_PLACES_API_KEY")

        if not api_key:
            raise ValueError(
                "api_key must be provided or GOOGLE_PLACES_API_KEY env var must be set"
            )

        self.client = googlemaps.Client(key=api_key)
        self._last_request_time = 0

    def _rate_limit(self):
        """Apply rate limiting between API requests."""
        elapsed = time.time() - self._last_request_time
        if elapsed < RATE_LIMIT_DELAY:
            time.sleep(RATE_LIMIT_DELAY - elapsed)
        self._last_request_time = time.time()

    def search_place(self, clinic: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Search for a clinic in Google Places.

        Args:
            clinic: Dict with clinic info (name, address, city, state).

        Returns:
            Place result dict or None if not found.
        """
        name = clinic.get("name", "")
        address = clinic.get("address", "")
        city = clinic.get("city", "")
        state = clinic.get("state", "")

        # Build search query
        query_parts = [name]
        if address:
            query_parts.append(address)
        if city:
            query_parts.append(city)
        if state:
            query_parts.append(state)
        query_parts.append("Malaysia")

        query = ", ".join(filter(None, query_parts))

        try:
            self._rate_limit()
            logger.debug(f"Searching Places API for: {query}")

            # Use text search to find the place
            results = self.client.places(query=query, language="en")

            if results.get("results"):
                return results["results"][0]
            else:
                logger.debug(f"No results found for: {name}")
                return None

        except Exception as e:
            logger.warning(f"Places search failed for {name}: {e}")
            return None

    def get_place_details(self, place_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information for a place.

        Args:
            place_id: Google Place ID.

        Returns:
            Place details dict or None on error.
        """
        try:
            self._rate_limit()
            logger.debug(f"Fetching place details for: {place_id}")

            # Request specific fields to minimize cost
            fields = [
                "formatted_phone_number",
                "international_phone_number",
                "website",
                "opening_hours",
                "geometry",
                "rating",
                "user_ratings_total",
                "url",
            ]

            result = self.client.place(place_id=place_id, fields=fields)

            if result.get("result"):
                return result["result"]
            return None

        except Exception as e:
            logger.warning(f"Place details failed for {place_id}: {e}")
            return None

    def enrich_clinic(self, clinic: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich a clinic record with Google Places data.

        Args:
            clinic: Dict with basic clinic info.

        Returns:
            Enriched clinic dict with additional fields.
        """
        enriched = clinic.copy()

        # Initialize enriched fields with None
        enriched.update({
            "latitude": None,
            "longitude": None,
            "phone": None,
            "website": None,
            "operating_hours": None,
            "google_place_id": None,
            "google_rating": None,
            "google_reviews_count": None,
        })

        # Search for the place
        place = self.search_place(clinic)
        if not place:
            return enriched

        # Extract basic info from search result
        place_id = place.get("place_id")
        if place_id:
            enriched["google_place_id"] = place_id

        # Get location from search result
        geometry = place.get("geometry", {})
        location = geometry.get("location", {})
        if location:
            enriched["latitude"] = location.get("lat")
            enriched["longitude"] = location.get("lng")

        # Get rating from search result
        enriched["google_rating"] = place.get("rating")
        enriched["google_reviews_count"] = place.get("user_ratings_total")

        # Get detailed info if we have a place_id
        if place_id:
            details = self.get_place_details(place_id)
            if details:
                # Phone number
                enriched["phone"] = (
                    details.get("formatted_phone_number")
                    or details.get("international_phone_number")
                )

                # Website
                enriched["website"] = details.get("website")

                # Operating hours
                opening_hours = details.get("opening_hours")
                if opening_hours:
                    # Convert to JSON-serializable format
                    enriched["operating_hours"] = {
                        "weekday_text": opening_hours.get("weekday_text", []),
                        "periods": opening_hours.get("periods", []),
                    }

        return enriched



