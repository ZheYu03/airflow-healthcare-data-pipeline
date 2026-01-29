"""Google Maps scraper using Playwright for clinic enrichment.

Compulsory fields to extract:
- latitude, longitude, phone, operating_hours, services, specialties, google_place_id, google_rating

Optional fields:
- website, email, has_emergency
"""

import asyncio
import random
import re
import logging
from typing import Dict, Any, Optional, List, Tuple

from playwright.async_api import async_playwright, Page, Browser

logger = logging.getLogger(__name__)

# User agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]


class GoogleMapsScraper:
    """Scraper for extracting clinic information from Google Maps."""

    def __init__(self, headless: bool = True):
        """
        Initialize the scraper.

        Args:
            headless: Run browser in headless mode (default True).
        """
        self.headless = headless
        self._browser: Optional[Browser] = None
        self._playwright = None

    async def __aenter__(self):
        """Async context manager entry."""
        self._playwright = await async_playwright().start()
        self._browser = await self._playwright.chromium.launch(
            headless=self.headless,
            args=[
                "--disable-blink-features=AutomationControlled",
                "--disable-dev-shm-usage",
                "--no-sandbox",
                "--disable-gpu",
            ],
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if self._browser:
            await self._browser.close()
        if self._playwright:
            await self._playwright.stop()

    async def _random_delay(self, min_sec: float = 1.0, max_sec: float = 3.0):
        """Add random delay to simulate human behavior."""
        delay = random.uniform(min_sec, max_sec)
        await asyncio.sleep(delay)

    async def _get_text(self, page: Page, selector: str) -> Optional[str]:
        """Safely get text from an element."""
        try:
            el = await page.query_selector(selector)
            if el:
                text = await el.inner_text()
                return text.strip() if text else None
        except Exception:
            pass
        return None

    async def _get_all_text(self, page: Page, selector: str) -> List[str]:
        """Safely get text from all matching elements."""
        results = []
        try:
            elements = await page.query_selector_all(selector)
            for el in elements:
                text = await el.inner_text()
                if text and text.strip():
                    results.append(text.strip())
        except Exception:
            pass
        return results

    async def _get_attribute(self, page: Page, selector: str, attr: str) -> Optional[str]:
        """Safely get attribute from an element."""
        try:
            el = await page.query_selector(selector)
            if el:
                return await el.get_attribute(attr)
        except Exception:
            pass
        return None

    def _parse_coordinates_from_url(self, url: str) -> Tuple[Optional[float], Optional[float]]:
        """Extract latitude and longitude from Google Maps URL."""
        patterns = [
            r"@(-?\d+\.?\d*),(-?\d+\.?\d*)",  # @lat,lng format
            r"!3d(-?\d+\.?\d*)!4d(-?\d+\.?\d*)",  # !3dlat!4dlng format
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                try:
                    lat = float(match.group(1))
                    lng = float(match.group(2))
                    if -90 <= lat <= 90 and -180 <= lng <= 180:
                        return lat, lng
                except (ValueError, IndexError):
                    pass
        return None, None

    def _parse_place_id_from_url(self, url: str) -> Optional[str]:
        """Extract place ID from Google Maps URL."""
        # Try multiple patterns
        patterns = [
            r"!1s(0x[a-f0-9]+:[a-f0-9]+)",  # hex format
            r"(ChIJ[a-zA-Z0-9_-]+)",  # ChIJ format
            r"place_id[=:]([a-zA-Z0-9_-]+)",  # place_id param
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None

    def _parse_rating(self, text: Optional[str]) -> Optional[float]:
        """Parse rating from text like '4.9'."""
        if not text:
            return None
        match = re.search(r"(\d+[.,]?\d*)", text)
        if match:
            try:
                return float(match.group(1).replace(",", "."))
            except ValueError:
                pass
        return None

    def _parse_review_count(self, text: Optional[str]) -> Optional[int]:
        """Parse review count from text like '(287)' or '287 reviews'."""
        if not text:
            return None
        text = text.replace(",", "").replace(".", "").replace(" ", "")
        match = re.search(r"(\d+)", text)
        if match:
            try:
                return int(match.group(1))
            except ValueError:
                pass
        return None

    def _clean_phone(self, phone: Optional[str]) -> Optional[str]:
        """Clean phone number."""
        if not phone:
            return None
        # Remove common prefixes and clean
        phone = phone.replace("phone:tel:", "").replace("Phone:", "").strip()
        # Keep only digits, +, -, spaces, parentheses
        cleaned = re.sub(r"[^\d+\-\s()]+", "", phone)
        return cleaned if cleaned else None

    async def _extract_phone(self, page: Page) -> Optional[str]:
        """Extract phone number using multiple methods."""
        phone = None

        # Method 1: data-item-id attribute
        phone_el = await page.query_selector('button[data-item-id^="phone:tel"]')
        if phone_el:
            phone_attr = await phone_el.get_attribute("data-item-id")
            if phone_attr:
                phone = self._clean_phone(phone_attr)
                if phone:
                    logger.debug(f"Phone found via data-item-id: {phone}")
                    return phone

        # Method 2: aria-label containing phone
        phone_el = await page.query_selector('button[aria-label*="phone" i], button[aria-label*="Phone" i]')
        if phone_el:
            phone_text = await phone_el.get_attribute("aria-label")
            if phone_text:
                # Extract phone from aria-label like "Phone: 03-1234 5678"
                match = re.search(r"[\d\s+\-()]{8,}", phone_text)
                if match:
                    phone = self._clean_phone(match.group(0))
                    if phone:
                        logger.debug(f"Phone found via aria-label: {phone}")
                        return phone

        # Method 3: Look for phone icon button and get sibling text
        phone_buttons = await page.query_selector_all('button[data-tooltip*="phone" i], button[data-tooltip*="Copy phone"]')
        for btn in phone_buttons:
            text = await btn.inner_text()
            if text and re.search(r"\d{2,}", text):
                phone = self._clean_phone(text)
                if phone:
                    logger.debug(f"Phone found via tooltip button: {phone}")
                    return phone

        # Method 4: Search in info panel for phone patterns
        info_items = await page.query_selector_all('div[role="region"] button, div.rogA2c')
        for item in info_items:
            text = await item.inner_text()
            if text and re.match(r"^[\d\s+\-()]{8,}$", text.strip()):
                phone = self._clean_phone(text)
                if phone:
                    logger.debug(f"Phone found via info panel: {phone}")
                    return phone

        logger.debug("Phone not found")
        return None

    async def _extract_operating_hours(self, page: Page) -> Optional[Dict[str, Any]]:
        """Extract operating hours from the info panel."""
        hours_data: Dict[str, Any] = {"weekday_text": []}

        try:
            # Method 1: Click hours button to expand
            hours_selectors = [
                'button[data-item-id="oh"]',
                'button[aria-label*="hours" i]',
                'button[aria-label*="Hours" i]',
                '[data-hide-tooltip-on-mouse-move="true"]',
            ]
            
            for selector in hours_selectors:
                hours_button = await page.query_selector(selector)
                if hours_button:
                    try:
                        await hours_button.click()
                        await self._random_delay(0.5, 1.0)
                        break
                    except Exception:
                        continue

            # Method 2: Look for hours table/list
            hours_selectors = [
                'table.eK4R0e tr',
                'table.WgFkxc tr', 
                'div.OqCZI',
                'div.t39EBf',
                'tr.y0skZc',
            ]
            
            for selector in hours_selectors:
                elements = await page.query_selector_all(selector)
                if elements:
                    for el in elements:
                        text = await el.inner_text()
                        if text and text.strip():
                            cleaned = text.strip().replace("\t", " ").replace("\n", " ")
                            if cleaned and len(cleaned) > 3:
                                hours_data["weekday_text"].append(cleaned)
                    if hours_data["weekday_text"]:
                        break

            # Method 3: Try aria-label on hours section
            if not hours_data["weekday_text"]:
                hours_section = await page.query_selector('[aria-label*="hour" i]')
                if hours_section:
                    aria_label = await hours_section.get_attribute("aria-label")
                    if aria_label and len(aria_label) > 10:
                        hours_data["weekday_text"].append(aria_label)

            if hours_data["weekday_text"]:
                logger.debug(f"Operating hours found: {len(hours_data['weekday_text'])} entries")
                return hours_data

        except Exception as e:
            logger.debug(f"Failed to parse hours: {e}")

        logger.debug("Operating hours not found")
        return None

    async def _extract_services_specialties(self, page: Page) -> Tuple[List[str], List[str]]:
        """Extract services and specialties from About section or category."""
        services: List[str] = []
        specialties: List[str] = []

        try:
            # Get category/type from header area
            category_selectors = [
                'button[jsaction*="category"]',
                'span.DkEaL',
                'div.LBgpqf button',
                'span.mgr77e',
            ]
            for selector in category_selectors:
                category = await self._get_text(page, selector)
                if category and len(category) < 100:
                    specialties.append(category)
                    logger.debug(f"Specialty found: {category}")
                    break

            # Try to find and click About tab
            about_selectors = [
                'button[aria-label*="About"]',
                'button[data-tab-id="overview"]',
                'button:has-text("About")',
            ]
            for selector in about_selectors:
                about_button = await page.query_selector(selector)
                if about_button:
                    try:
                        await about_button.click()
                        await self._random_delay(0.5, 1.0)
                        break
                    except Exception:
                        continue

            # Extract service items from About section
            service_selectors = [
                'div[data-attrid] span',
                'div.iP2t7d span',
                'div.LBgpqf div',
                'li.hfpxzc',
            ]
            for selector in service_selectors:
                elements = await page.query_selector_all(selector)
                for el in elements:
                    text = await el.inner_text()
                    if text and 3 < len(text.strip()) < 100:
                        item = text.strip()
                        if item not in services:
                            services.append(item)
                if services:
                    break

            # Look for health services keywords
            health_keywords = ["clinic", "medical", "health", "doctor", "hospital", "dental", "pharmacy"]
            page_text = await page.content()
            for keyword in health_keywords:
                if keyword.lower() in page_text.lower() and keyword not in [s.lower() for s in specialties]:
                    # Don't add generic keywords, just log
                    pass

        except Exception as e:
            logger.debug(f"Failed to extract services: {e}")

        logger.debug(f"Services found: {len(services)}, Specialties found: {len(specialties)}")
        return services[:10], specialties[:5]

    async def _extract_website(self, page: Page) -> Optional[str]:
        """Extract website URL."""
        website = None

        # Method 1: authority link
        website_el = await page.query_selector('a[data-item-id^="authority"]')
        if website_el:
            website = await website_el.get_attribute("href")
            if website:
                logger.debug(f"Website found via authority: {website}")
                return website

        # Method 2: aria-label
        website_el = await page.query_selector('a[aria-label*="website" i], a[aria-label*="Website" i]')
        if website_el:
            website = await website_el.get_attribute("href")
            if website:
                logger.debug(f"Website found via aria-label: {website}")
                return website

        # Method 3: data-tooltip
        website_el = await page.query_selector('a[data-tooltip*="website" i]')
        if website_el:
            website = await website_el.get_attribute("href")
            if website:
                logger.debug(f"Website found via tooltip: {website}")
                return website

        logger.debug("Website not found")
        return None

    async def scrape_clinic(self, name: str, address: str, city: str, state: str) -> Dict[str, Any]:
        """
        Scrape a clinic's information from Google Maps.

        Args:
            name: Clinic name.
            address: Clinic address.
            city: City name.
            state: State name.

        Returns:
            Dict with enriched clinic data including:
            - COMPULSORY: latitude, longitude, phone, operating_hours, services, specialties, google_place_id, google_rating
            - OPTIONAL: website, email, has_emergency
        """
        result: Dict[str, Any] = {
            # Compulsory fields
            "latitude": None,
            "longitude": None,
            "phone": None,
            "operating_hours": None,
            "services": None,
            "specialties": None,
            "google_place_id": None,
            "google_rating": None,
            # Optional fields
            "website": None,
            "email": None,
            "has_emergency": None,
            # Meta
            "scrape_success": False,
            "scrape_error": None,
        }

        if not self._browser:
            result["scrape_error"] = "Browser not initialized"
            return result

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

        page = None
        context = None
        try:
            # Create new page with random user agent
            context = await self._browser.new_context(
                user_agent=random.choice(USER_AGENTS),
                viewport={"width": 1280, "height": 900},
                locale="en-US",
            )
            page = await context.new_page()

            # Navigate to Google Maps
            logger.info(f"Searching for: {name}")
            await page.goto("https://www.google.com/maps", wait_until="domcontentloaded")
            await self._random_delay(1.5, 2.5)

            # Fill search box and search
            search_box = await page.wait_for_selector('input#searchboxinput', timeout=10000)
            await search_box.fill(query)
            await self._random_delay(0.3, 0.7)
            await page.keyboard.press("Enter")
            await self._random_delay(3.0, 5.0)

            # Check for CAPTCHA or unusual traffic page
            page_content = await page.content()
            if "unusual traffic" in page_content.lower() or "captcha" in page_content.lower():
                result["scrape_error"] = "CAPTCHA detected"
                logger.warning("CAPTCHA detected - need to pause scraping")
                return result

            # Try to click the first result if we're on a list view
            try:
                first_result = await page.query_selector('div[role="article"]')
                if first_result:
                    await first_result.click()
                    await self._random_delay(2.0, 3.0)
            except Exception:
                pass

            # Wait for info panel to load
            await self._random_delay(2.0, 3.0)

            # === EXTRACT COMPULSORY FIELDS ===

            # 1. Coordinates from URL
            current_url = page.url
            lat, lng = self._parse_coordinates_from_url(current_url)
            result["latitude"] = lat
            result["longitude"] = lng
            logger.debug(f"Coordinates: {lat}, {lng}")

            # 2. Google Place ID from URL
            result["google_place_id"] = self._parse_place_id_from_url(current_url)
            logger.debug(f"Place ID: {result['google_place_id']}")

            # 3. Rating
            rating_selectors = [
                "span.F7nice span",
                "div.F7nice span",
                "span.ceNzKf",
                "span.ZkP5Je",
            ]
            for selector in rating_selectors:
                rating_text = await self._get_text(page, selector)
                if rating_text:
                    result["google_rating"] = self._parse_rating(rating_text)
                    if result["google_rating"]:
                        logger.debug(f"Rating: {result['google_rating']}")
                        break

            # 4. Phone
            result["phone"] = await self._extract_phone(page)

            # 5. Operating hours
            result["operating_hours"] = await self._extract_operating_hours(page)

            # 6. Services and Specialties
            services, specialties = await self._extract_services_specialties(page)
            result["services"] = services if services else None
            result["specialties"] = specialties if specialties else None

            # === EXTRACT OPTIONAL FIELDS ===

            # Website (optional)
            result["website"] = await self._extract_website(page)

            # Check for emergency/24h keywords (optional)
            page_text_lower = page_content.lower()
            if "24 hour" in page_text_lower or "24-hour" in page_text_lower or "emergency" in page_text_lower:
                result["has_emergency"] = True

            # Mark success
            result["scrape_success"] = True

            # Log summary
            filled_compulsory = sum(1 for k in ["latitude", "longitude", "phone", "operating_hours", "services", "specialties", "google_place_id", "google_rating"] if result.get(k))
            logger.info(
                f"Successfully scraped: {name} | "
                f"rating={result['google_rating']}, coords={lat},{lng}, "
                f"phone={result['phone']}, place_id={result['google_place_id']}, "
                f"hours={'Yes' if result['operating_hours'] else 'No'}, "
                f"services={len(services)}, specialties={len(specialties)} | "
                f"Compulsory fields: {filled_compulsory}/8"
            )

        except Exception as e:
            result["scrape_error"] = str(e)
            logger.error(f"Scrape failed for {name}: {e}")

        finally:
            if page:
                try:
                    await page.close()
                except Exception:
                    pass
            if context:
                try:
                    await context.close()
                except Exception:
                    pass

        return result


async def scrape_single_clinic(
    name: str,
    address: str,
    city: str,
    state: str,
    headless: bool = True
) -> Dict[str, Any]:
    """
    Convenience function to scrape a single clinic.

    Args:
        name: Clinic name.
        address: Clinic address.
        city: City name.
        state: State name.
        headless: Run browser in headless mode.

    Returns:
        Dict with enriched clinic data.
    """
    async with GoogleMapsScraper(headless=headless) as scraper:
        return await scraper.scrape_clinic(name, address, city, state)


def scrape_clinic_sync(
    name: str,
    address: str,
    city: str,
    state: str,
    headless: bool = True
) -> Dict[str, Any]:
    """
    Synchronous wrapper for scraping a single clinic.

    Args:
        name: Clinic name.
        address: Clinic address.
        city: City name.
        state: State name.
        headless: Run browser in headless mode.

    Returns:
        Dict with enriched clinic data.
    """
    return asyncio.run(scrape_single_clinic(name, address, city, state, headless))

