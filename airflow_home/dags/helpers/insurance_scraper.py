"""Insurance plan scraper for Malaysian insurance companies.

Scrapes medical/health insurance plans from:
- AIA Malaysia
- Prudential BSN
- Allianz Malaysia
- Great Eastern
- Etiqa Insurance

Uses Playwright for HTML scraping, pdfplumber for PDF parsing,
and OpenAI GPT-4o for intelligent data extraction from brochures.
"""

import asyncio
import hashlib
import io
import logging
import os
import random
import re
import tempfile
from typing import Any, Dict, List, Optional, Tuple

from playwright.async_api import Browser, Page, async_playwright

logger = logging.getLogger(__name__)

# User agents to rotate
USER_AGENTS = [
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:122.0) Gecko/20100101 Firefox/122.0",
]

# Insurance provider configurations (web scraping only - no hardcoded plans)
PROVIDERS = {
    "AIA": {
        "name": "AIA Malaysia",
        "base_url": "https://www.aia.com.my",
        "products_url": "https://www.aia.com.my/en/our-products/health-protection/medical-protection.html",
        "contact_phone": "1300-88-1318",
        "website": "https://www.aia.com.my",
    },
    "Prudential": {
        "name": "Prudential Malaysia",
        "base_url": "https://www.prudential.com.my",
        "products_url": "https://www.prudential.com.my/en/products-health-insurance/medical-plans/",
        "contact_phone": "1300-88-7288",
        "website": "https://www.prudential.com.my",
    },
    "Allianz": {
        "name": "Allianz Malaysia",
        "base_url": "https://www.allianz.com.my",
        "products_url": "https://www.allianz.com.my/personal/life-health-and-savings/medical-and-hospitalisation.html",
        "contact_phone": "1300-22-5542",
        "website": "https://www.allianz.com.my",
    },
    "GreatEastern": {
        "name": "Great Eastern Life",
        "base_url": "https://www.greateasternlife.com",
        "products_url": "https://www.greateasternlife.com/my/en/personal-insurance/our-products.html?category=corp-site%3Amy%2Fproduct-category%2Flife-and-health%2Fhealth-insurance&online=&gift=&keyword=",
        "contact_phone": "1300-13-8338",
        "website": "https://www.greateasternlife.com/my",
    },
    "Etiqa": {
        "name": "Etiqa Insurance",
        "base_url": "https://www.etiqa.com.my",
        "products_url": "https://www.etiqa.com.my/health",
        "contact_phone": "1300-13-8888",
        "website": "https://www.etiqa.com.my",
    },
}


def is_valid_plan_name(name: str) -> bool:
    """Check if a string looks like a valid insurance plan name."""
    if not name or len(name) < 4 or len(name) > 80:
        return False
    
    name_lower = name.lower().strip()
    
    # Filter out generic/navigation text
    invalid_patterns = [
        "health protection", "medical protection", "life protection",
        "click", "read more", "learn more", "find out", "explore",
        "contact us", "about us", "about ", "home", "menu", "login", "register",
        "cookie", "privacy", "terms", "copyright", "footer", "header",
        "navigation", "search", "loading", "error", "submit", "subscribe",
        "today", "logo", "provider", "service company", "welcome",
        "vitality", "customer support", "road ranger", "auto assist",
        "cares", " has ", " we ", "support", "assist", "children &",
        "wellness", "how ", "what ", "why ", "when ", "where ",
        # Additional filters for common false positives
        "knowledge hub", "insurance type", "insurance needs", "sign up",
        "sign in", "log in", "get quote", "view more", "view all",
        "download", "brochure", "pdf", "contact", "email", "phone",
        "follow us", "social", "facebook", "twitter", "instagram",
        "linkedin", "youtube", "sdn bhd", "berhad", "holdings",
        "services", "programme", "program", "healthiest", "campaign",
        "news", "article", "blog", "event", "promotion", "offer",
    ]
    
    for pattern in invalid_patterns:
        if pattern in name_lower:
            return False
    
    # Must start with a letter
    if not name[0].isalpha():
        return False
    
    # Should contain some letters (not just symbols/numbers)
    letter_count = sum(1 for c in name if c.isalpha())
    if letter_count < 3:
        return False
    
    # Known good prefixes for insurance products
    good_prefixes = [
        "pru", "aia ", "aia med", "allianz", "great ", "etiqa", "takaful", "medisafe",
        "supreme", "smart", "a-plus", "a-life", "critical", "hospital",
        "i-medik", "medical ez", "mediplus", "med insure", "med ",
        "onemedical", "one medical", "i-med", "ezy", "healthassured",
    ]
    
    # Boost confidence for names with known prefixes
    has_good_prefix = any(name_lower.startswith(prefix) for prefix in good_prefixes)
    
    # If short name without good prefix, likely not valid
    if len(name) < 12 and not has_good_prefix:
        return False
    
    return True


def generate_plan_id(provider_name: str, plan_name: str) -> str:
    """Generate a deterministic UUID-like ID from provider and plan name."""
    content = f"{provider_name.lower().strip()}|{plan_name.lower().strip()}"
    hash_bytes = hashlib.sha256(content.encode()).digest()
    hex_str = hash_bytes.hex()[:32]
    return f"{hex_str[:8]}-{hex_str[8:12]}-{hex_str[12:16]}-{hex_str[16:20]}-{hex_str[20:32]}"


def normalize_plan_data(raw_data: Dict[str, Any], provider_key: str) -> Dict[str, Any]:
    """
    Normalize scraped plan data to match the Insurance Plans table schema.
    
    Args:
        raw_data: Raw scraped data dict.
        provider_key: Provider key (e.g., "AIA", "Prudential").
    
    Returns:
        Normalized dict matching table schema.
    """
    provider_info = PROVIDERS.get(provider_key, {})
    plan_name = raw_data.get("plan_name", "Unknown Plan")
    provider_name = provider_info.get("name", provider_key)
    
    return {
        "id": generate_plan_id(provider_name, plan_name),
        "provider_name": provider_name,
        "plan_name": plan_name,
        "plan_type": raw_data.get("plan_type", "Medical"),
        "coverage_type": raw_data.get("coverage_type"),
        "annual_limit": _parse_money(raw_data.get("annual_limit")),
        "lifetime_limit": _parse_money(raw_data.get("lifetime_limit")),
        "room_board_limit": _parse_money(raw_data.get("room_board_limit")),
        "outpatient_covered": raw_data.get("outpatient_covered"),
        "maternity_covered": raw_data.get("maternity_covered"),
        "dental_covered": raw_data.get("dental_covered"),
        "optical_covered": raw_data.get("optical_covered"),
        "mental_health_covered": raw_data.get("mental_health_covered"),
        "covered_conditions": raw_data.get("covered_conditions"),
        "excluded_conditions": raw_data.get("excluded_conditions"),
        "panel_hospitals": raw_data.get("panel_hospitals"),
        "monthly_premium_min": _parse_money(raw_data.get("monthly_premium_min")),
        "monthly_premium_max": _parse_money(raw_data.get("monthly_premium_max")),
        "deductible": _parse_money(raw_data.get("deductible")),
        "co_payment_percentage": _parse_percentage(raw_data.get("co_payment_percentage")),
        "min_age": _parse_int(raw_data.get("min_age")),
        "max_age": _parse_int(raw_data.get("max_age")),
        "claim_process": raw_data.get("claim_process"),
        "contact_phone": provider_info.get("contact_phone"),
        "website": raw_data.get("website") or provider_info.get("website"),
        "is_active": True,
    }


def _parse_money(value: Any) -> Optional[float]:
    """Parse monetary value from various formats."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Remove currency symbols, commas, spaces
        cleaned = re.sub(r"[RM$,\s]", "", value, flags=re.IGNORECASE)
        # Handle "million" suffix
        if "million" in value.lower():
            cleaned = re.sub(r"[^0-9.]", "", cleaned)
            try:
                return float(cleaned) * 1_000_000
            except ValueError:
                pass
        try:
            return float(cleaned)
        except ValueError:
            pass
    return None


def _parse_percentage(value: Any) -> Optional[float]:
    """Parse percentage value."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        match = re.search(r"(\d+(?:\.\d+)?)", value)
        if match:
            return float(match.group(1))
    return None


def _parse_int(value: Any) -> Optional[int]:
    """Parse integer value."""
    if value is None:
        return None
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    if isinstance(value, str):
        match = re.search(r"(\d+)", value)
        if match:
            return int(match.group(1))
    return None


class InsuranceScraper:
    """Scraper for Malaysian insurance company websites."""

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

    async def _create_page(self) -> Tuple[Page, Any]:
        """Create a new browser page with random user agent."""
        context = await self._browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1280, "height": 900},
            locale="en-MY",
        )
        page = await context.new_page()
        return page, context

    async def _safe_get_text(self, page: Page, selector: str) -> Optional[str]:
        """Safely get text from an element."""
        try:
            el = await page.query_selector(selector)
            if el:
                text = await el.inner_text()
                return text.strip() if text else None
        except Exception:
            pass
        return None

    async def _safe_get_all_text(self, page: Page, selector: str) -> List[str]:
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

    async def _safe_get_attribute(self, page: Page, selector: str, attr: str) -> Optional[str]:
        """Safely get attribute from an element."""
        try:
            el = await page.query_selector(selector)
            if el:
                return await el.get_attribute(attr)
        except Exception:
            pass
        return None

    async def _download_pdf(self, page: Page, pdf_url: str) -> Optional[bytes]:
        """Download a PDF file."""
        try:
            response = await page.request.get(pdf_url)
            if response.ok:
                return await response.body()
        except Exception as e:
            logger.warning(f"Failed to download PDF {pdf_url}: {e}")
        return None

    async def _click_load_more_until_gone(
        self, page: Page, button_selector: str, max_clicks: int = 20
    ) -> int:
        """
        Click a 'Load More' or 'Show More' button until it disappears.
        
        Args:
            page: Playwright page object.
            button_selector: CSS selector or text selector for the button.
            max_clicks: Maximum number of clicks to prevent infinite loops.
        
        Returns:
            Number of times the button was clicked.
        """
        clicks = 0
        while clicks < max_clicks:
            try:
                # Wait a bit for any animations to complete
                await self._random_delay(1, 2)
                
                # Try to find the button
                btn = await page.query_selector(button_selector)
                if not btn:
                    # Also try with text-based selector
                    btn = await page.query_selector(f"button:has-text('{button_selector}')")
                
                if not btn:
                    logger.info(f"Button '{button_selector}' not found, stopping after {clicks} clicks")
                    break
                
                # Check if button is visible
                is_visible = await btn.is_visible()
                if not is_visible:
                    logger.info(f"Button '{button_selector}' not visible, stopping after {clicks} clicks")
                    break
                
                # Scroll the button into view first
                await btn.scroll_into_view_if_needed()
                await self._random_delay(0.5, 1)
                
                # Try clicking with JavaScript if normal click fails
                try:
                    await btn.click(timeout=5000)
                except Exception:
                    # Fallback to JavaScript click
                    logger.debug(f"Normal click failed, trying JavaScript click")
                    await page.evaluate("(el) => el.click()", btn)
                
                clicks += 1
                logger.info(f"Clicked '{button_selector}' button ({clicks} times)")
                
                # Wait for content to load after click
                await self._random_delay(2, 4)
                
                # Wait for network to settle
                try:
                    await page.wait_for_load_state("networkidle", timeout=5000)
                except Exception:
                    pass  # Timeout is okay, continue
                
            except Exception as e:
                logger.warning(f"Error clicking button '{button_selector}': {e}")
                break
        
        return clicks

    async def _classify_plan_type_with_llm(
        self, plan_name: str, description: str, api_key: str = None
    ) -> bool:
        """
        Use LLM to classify if a plan is Medical Insurance (True) or Critical Illness (False).
        
        Medical Insurance = hospitalization, medical card, surgical coverage, daily hospital benefits
        Critical Illness = lump sum payout on diagnosis of cancer, heart attack, stroke, etc.
        
        Args:
            plan_name: Name of the insurance plan
            description: Description or key benefits of the plan
            api_key: OpenAI API key (uses env var if not provided)
        
        Returns:
            True if Medical Insurance, False if Critical Illness or other
        """
        import openai
        
        api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.warning("No OpenAI API key available for plan classification")
            # Default to True (include) if no API key - will be filtered by other means
            return True
        
        client = openai.OpenAI(api_key=api_key)
        
        prompt = f"""Classify this insurance plan as either "MEDICAL" or "CRITICAL_ILLNESS":

MEDICAL Insurance (hospitalization coverage):
- Pays HOSPITAL BILLS directly
- Medical card / cashless hospital admission
- Room & board / daily hospital benefits
- Surgical, ICU, outpatient coverage
- Plan names usually contain: MediCard, MediValue, MediShield, Medical, Health Protector, Health Direct, Hospital, Baby Shield

CRITICAL_ILLNESS Insurance (lump sum payout):
- Pays LUMP SUM CASH on diagnosis (NOT hospital bills)
- Triggered by diagnosis of: cancer, heart attack, stroke, kidney failure, critical diseases
- Plan names usually contain: Critical Care, Critical Relief, Critical Illness, Early Payout, Multi Cancer

Plan Name: {plan_name}
Description: {description}

CLASSIFICATION RULE:
- If plan name contains "Critical Care" or "Critical Relief" or "Critical Illness" -> CRITICAL_ILLNESS
- If plan name contains "Medical" or "MediCard" or "MediValue" or "Health Direct" -> MEDICAL
- If uncertain, check if plan pays hospital bills (MEDICAL) or lump sum on diagnosis (CRITICAL_ILLNESS)

Reply with ONLY one word: MEDICAL or CRITICAL_ILLNESS"""

        try:
            response = client.chat.completions.create(
                model="gpt-4o-mini",  # Use mini for fast, cheap classification
                messages=[
                    {"role": "system", "content": "You are an insurance plan classifier. Reply with only MEDICAL or CRITICAL_ILLNESS."},
                    {"role": "user", "content": prompt}
                ],
                max_tokens=20,
                temperature=0
            )
            
            result = response.choices[0].message.content.strip().upper()
            is_medical = "MEDICAL" in result and "CRITICAL" not in result
            
            logger.info(f"LLM classified '{plan_name}' as: {result} -> {'Medical Insurance' if is_medical else 'Critical Illness'}")
            return is_medical
            
        except Exception as e:
            logger.warning(f"LLM classification failed for '{plan_name}': {e}")
            # Default to True (include) on error - will be filtered by PDF analysis
            return True

    def _parse_pdf_text(self, pdf_bytes: bytes) -> str:
        """Parse text content from PDF bytes."""
        try:
            import pdfplumber
            
            with io.BytesIO(pdf_bytes) as pdf_file:
                with pdfplumber.open(pdf_file) as pdf:
                    text_parts = []
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            text_parts.append(text)
                    return "\n".join(text_parts)
        except Exception as e:
            logger.warning(f"Failed to parse PDF: {e}")
            return ""

    def _extract_coverage_from_text(self, text: str) -> Dict[str, Any]:
        """Extract coverage information from text content."""
        text_lower = text.lower()
        
        coverage = {
            "outpatient_covered": None,
            "maternity_covered": None,
            "dental_covered": None,
            "optical_covered": None,
            "mental_health_covered": None,
        }
        
        # Check for coverage keywords
        if any(kw in text_lower for kw in ["outpatient", "out-patient", "clinic visit"]):
            coverage["outpatient_covered"] = "not covered" not in text_lower or "outpatient" in text_lower
        
        if any(kw in text_lower for kw in ["maternity", "pregnancy", "childbirth"]):
            coverage["maternity_covered"] = True
        
        if any(kw in text_lower for kw in ["dental", "teeth", "orthodontic"]):
            coverage["dental_covered"] = True
        
        if any(kw in text_lower for kw in ["optical", "eye", "vision", "spectacles"]):
            coverage["optical_covered"] = True
        
        if any(kw in text_lower for kw in ["mental health", "psychiatric", "psychology"]):
            coverage["mental_health_covered"] = True
        
        return coverage

    def _extract_limits_from_text(self, text: str) -> Dict[str, Any]:
        """Extract limit amounts from text content."""
        limits = {
            "annual_limit": None,
            "lifetime_limit": None,
            "room_board_limit": None,
        }
        
        # Common patterns for limits
        annual_patterns = [
            r"annual\s+limit[:\s]+(?:rm\s*)?([0-9,.]+(?:\s*million)?)",
            r"yearly\s+limit[:\s]+(?:rm\s*)?([0-9,.]+(?:\s*million)?)",
            r"per\s+year[:\s]+(?:rm\s*)?([0-9,.]+(?:\s*million)?)",
        ]
        
        lifetime_patterns = [
            r"lifetime\s+limit[:\s]+(?:rm\s*)?([0-9,.]+(?:\s*million)?)",
            r"overall\s+limit[:\s]+(?:rm\s*)?([0-9,.]+(?:\s*million)?)",
        ]
        
        room_patterns = [
            r"room\s*(?:&|and)?\s*board[:\s]+(?:rm\s*)?([0-9,.]+)",
            r"daily\s+room[:\s]+(?:rm\s*)?([0-9,.]+)",
        ]
        
        text_lower = text.lower()
        
        for pattern in annual_patterns:
            match = re.search(pattern, text_lower)
            if match:
                limits["annual_limit"] = match.group(1)
                break
        
        for pattern in lifetime_patterns:
            match = re.search(pattern, text_lower)
            if match:
                limits["lifetime_limit"] = match.group(1)
                break
        
        for pattern in room_patterns:
            match = re.search(pattern, text_lower)
            if match:
                limits["room_board_limit"] = match.group(1)
                break
        
        return limits

    # ========== Provider-specific scrapers ==========

    async def _extract_product_links(self, page: Page, base_url: str) -> List[str]:
        """Extract product page links from a listing page."""
        links = []
        selectors = [
            "a[href*='product']",
            "a[href*='health']",
            "a[href*='medical']",
            ".product-card a",
            ".product-item a",
            "article a",
        ]
        
        for selector in selectors:
            try:
                elements = await page.query_selector_all(selector)
                for el in elements:
                    href = await el.get_attribute("href")
                    if href:
                        if not href.startswith("http"):
                            href = base_url + href if href.startswith("/") else base_url + "/" + href
                        if href not in links:
                            links.append(href)
            except Exception:
                continue
        
        return links[:15]  # Limit to avoid too many requests

    async def scrape_aia(self) -> List[Dict[str, Any]]:
        """Scrape AIA Malaysia health insurance products."""
        provider_key = "AIA"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        logger.info(f"Scraping {config['name']} products from website...")
        
        page, context = await self._create_page()
        try:
            await page.goto(config["products_url"], wait_until="domcontentloaded", timeout=30000)
            await self._random_delay(2, 4)
            
            # Multiple selector strategies to find product names
            selectors = [
                # Product cards and titles
                ".product-card h2", ".product-card h3", ".product-card .title",
                ".product-item h2", ".product-item h3", ".product-item .title",
                "article h2", "article h3",
                # Links with health/medical content
                "a[href*='health'] span", "a[href*='medical'] span",
                # Generic product name patterns
                "[class*='product'] h2", "[class*='product'] h3",
                "h2.title", "h3.title",
                # Grid/list items
                ".grid-item h3", ".list-item h3",
            ]
            
            for selector in selectors:
                names = await self._safe_get_all_text(page, selector)
                for name in names:
                    name = name.strip()
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Also search for AIA-specific product patterns in page content
            page_content = await page.content()
            aia_patterns = [
                r"A-(?:Plus|Life)\s+[A-Za-z]+(?:\s+[A-Za-z]+)?",
                r"AIA\s+(?:Medical|Health|Critical|Voluntary)[A-Za-z\s]*",
            ]
            
            for pattern in aia_patterns:
                matches = re.findall(pattern, page_content)
                for match in matches:
                    name = match.strip()
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Try to scrape individual product pages for more details
            product_links = await self._extract_product_links(page, config["base_url"])
            for link in product_links[:5]:  # Limit to first 5 product pages
                await self._random_delay(1, 2)
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=30000)
                    await self._random_delay(1, 2)
                    
                    # Get product name from h1
                    h1_text = await self._safe_get_text(page, "h1")
                    if h1_text and is_valid_plan_name(h1_text) and h1_text.lower() not in seen_names:
                        seen_names.add(h1_text.lower())
                        
                        # Extract additional details
                        page_text = await page.content()
                        coverage = self._extract_coverage_from_text(page_text)
                        limits = self._extract_limits_from_text(page_text)
                        
                        plan_data = {
                            "plan_name": h1_text,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": link,
                            **coverage,
                            **limits,
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
                except Exception as e:
                    logger.debug(f"Failed to scrape product page {link}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to scrape AIA: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']}")
        return plans

    async def scrape_prudential(self) -> List[Dict[str, Any]]:
        """Scrape Prudential BSN health insurance products."""
        provider_key = "Prudential"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        logger.info(f"Scraping {config['name']} products from website...")
        
        page, context = await self._create_page()
        try:
            await page.goto(config["products_url"], wait_until="domcontentloaded", timeout=30000)
            await self._random_delay(2, 4)
            
            # Multiple selector strategies
            selectors = [
                ".product-card h2", ".product-card h3", ".product-card .title",
                ".product-item h2", ".product-item h3",
                "article h2", "article h3",
                "[class*='product'] h2", "[class*='product'] h3",
                ".card h3", ".card h2", ".card-title",
            ]
            
            for selector in selectors:
                names = await self._safe_get_all_text(page, selector)
                for name in names:
                    name = name.strip()
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Search for PRU-prefixed products in page content
            page_content = await page.content()
            pru_patterns = [
                r"PRU[A-Z][a-zA-Z]+(?:\s+[A-Z][a-z]+)?(?:\s+[A-Z][a-z]+)?",
                r"Prudential\s+(?:BSN\s+)?[A-Z][a-zA-Z\s]+",
            ]
            
            for pattern in pru_patterns:
                matches = re.findall(pattern, page_content)
                for match in matches:
                    name = match.strip()
                    # Filter out common false positives
                    if "Prudential plc" in name or "Prudential BSN Takaful" in name:
                        continue
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Try to scrape individual product pages
            product_links = await self._extract_product_links(page, config["base_url"])
            for link in product_links[:5]:
                await self._random_delay(1, 2)
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=30000)
                    await self._random_delay(1, 2)
                    
                    h1_text = await self._safe_get_text(page, "h1")
                    if h1_text and is_valid_plan_name(h1_text) and h1_text.lower() not in seen_names:
                        seen_names.add(h1_text.lower())
                        
                        page_text = await page.content()
                        coverage = self._extract_coverage_from_text(page_text)
                        limits = self._extract_limits_from_text(page_text)
                        
                        plan_data = {
                            "plan_name": h1_text,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": link,
                            **coverage,
                            **limits,
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
                except Exception as e:
                    logger.debug(f"Failed to scrape product page {link}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to scrape Prudential: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']}")
        return plans

    async def scrape_allianz(self) -> List[Dict[str, Any]]:
        """Scrape Allianz Malaysia health insurance products."""
        provider_key = "Allianz"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        logger.info(f"Scraping {config['name']} products from website...")
        
        page, context = await self._create_page()
        try:
            await page.goto(config["products_url"], wait_until="domcontentloaded", timeout=30000)
            await self._random_delay(2, 4)
            
            # Multiple selector strategies
            selectors = [
                ".product-card h2", ".product-card h3", ".product-card .title",
                ".product-item h2", ".product-item h3",
                "article h2", "article h3",
                "[class*='product'] h2", "[class*='product'] h3",
                ".card h3", ".card h2", ".card-title",
                "[data-product] h3", "[data-product] h2",
            ]
            
            for selector in selectors:
                names = await self._safe_get_all_text(page, selector)
                for name in names:
                    name = name.strip()
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Search for Allianz-specific products in page content
            page_content = await page.content()
            allianz_patterns = [
                r"Allianz\s+(?:Care|Health|Medi|Criti)[A-Za-z]*(?:\s+[A-Za-z]+)?",
                r"MediSafe\s*(?:Infinite|Plus|Basic)?",
                r"Hospital\s*(?:&|and)?\s*Surgical(?:\s+[A-Za-z]+)?",
            ]
            
            for pattern in allianz_patterns:
                matches = re.findall(pattern, page_content, re.IGNORECASE)
                for match in matches:
                    name = match.strip()
                    # Filter out false positives
                    if "Allianz Malaysia" in name or "Allianz Customer" in name:
                        continue
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Try to scrape individual product pages
            product_links = await self._extract_product_links(page, config["base_url"])
            for link in product_links[:5]:
                await self._random_delay(1, 2)
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=30000)
                    await self._random_delay(1, 2)
                    
                    h1_text = await self._safe_get_text(page, "h1")
                    if h1_text and is_valid_plan_name(h1_text) and h1_text.lower() not in seen_names:
                        seen_names.add(h1_text.lower())
                        
                        page_text = await page.content()
                        coverage = self._extract_coverage_from_text(page_text)
                        limits = self._extract_limits_from_text(page_text)
                        
                        plan_data = {
                            "plan_name": h1_text,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": link,
                            **coverage,
                            **limits,
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
                except Exception as e:
                    logger.debug(f"Failed to scrape product page {link}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to scrape Allianz: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']}")
        return plans

    async def scrape_great_eastern(self) -> List[Dict[str, Any]]:
        """Scrape Great Eastern health insurance products."""
        provider_key = "GreatEastern"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        logger.info(f"Scraping {config['name']} products from website...")
        
        page, context = await self._create_page()
        try:
            await page.goto(config["products_url"], wait_until="domcontentloaded", timeout=30000)
            await self._random_delay(2, 4)
            
            # Multiple selector strategies
            selectors = [
                ".product-card h2", ".product-card h3", ".product-card .title",
                ".product-item h2", ".product-item h3",
                "article h2", "article h3",
                "[class*='product'] h2", "[class*='product'] h3",
                ".card h3", ".card h2", ".card-title",
            ]
            
            for selector in selectors:
                names = await self._safe_get_all_text(page, selector)
                for name in names:
                    name = name.strip()
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Search for Great Eastern specific products in page content
            page_content = await page.content()
            ge_patterns = [
                r"GREAT\s+(?:HealthCare|MediCash|Critical|Total|Life)[A-Za-z]*(?:\s+[A-Za-z]+)?",
                r"Supreme\s+Health(?:\s+[A-Za-z]+)?",
                r"SmartMedic(?:\s+[A-Za-z]+)?",
                r"Great\s+(?:Care|Shield|Protect)[A-Za-z]*",
            ]
            
            for pattern in ge_patterns:
                matches = re.findall(pattern, page_content, re.IGNORECASE)
                for match in matches:
                    name = match.strip()
                    # Filter out false positives
                    if "Great Eastern" in name and len(name) < 20:
                        continue
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Try to scrape individual product pages
            product_links = await self._extract_product_links(page, config["base_url"])
            for link in product_links[:5]:
                await self._random_delay(1, 2)
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=30000)
                    await self._random_delay(1, 2)
                    
                    h1_text = await self._safe_get_text(page, "h1")
                    if h1_text and is_valid_plan_name(h1_text) and h1_text.lower() not in seen_names:
                        seen_names.add(h1_text.lower())
                        
                        page_text = await page.content()
                        coverage = self._extract_coverage_from_text(page_text)
                        limits = self._extract_limits_from_text(page_text)
                        
                        plan_data = {
                            "plan_name": h1_text,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": link,
                            **coverage,
                            **limits,
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
                except Exception as e:
                    logger.debug(f"Failed to scrape product page {link}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to scrape Great Eastern: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']}")
        return plans

    async def scrape_etiqa(self) -> List[Dict[str, Any]]:
        """Scrape Etiqa health insurance products."""
        provider_key = "Etiqa"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        logger.info(f"Scraping {config['name']} products from website...")
        
        page, context = await self._create_page()
        try:
            await page.goto(config["products_url"], wait_until="domcontentloaded", timeout=30000)
            await self._random_delay(2, 4)
            
            # Multiple selector strategies
            selectors = [
                ".product-card h2", ".product-card h3", ".product-card .title",
                ".product-item h2", ".product-item h3",
                "article h2", "article h3",
                "[class*='product'] h2", "[class*='product'] h3",
                ".card h3", ".card h2", ".card-title",
            ]
            
            for selector in selectors:
                names = await self._safe_get_all_text(page, selector)
                for name in names:
                    name = name.strip()
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Search for Etiqa specific products in page content
            page_content = await page.content()
            etiqa_patterns = [
                r"Medical\s+EZ(?:\s+[A-Za-z]+)?",
                r"Takaful\s+Medi[A-Za-z]+(?:\s+[A-Za-z]+)?",
                r"Etiqa\s+(?:Health|Family|Critical|Medi)[A-Za-z]*(?:\s+[A-Za-z]+)?",
                r"i-Medik(?:\s+[A-Za-z]+)?",
            ]
            
            for pattern in etiqa_patterns:
                matches = re.findall(pattern, page_content, re.IGNORECASE)
                for match in matches:
                    name = match.strip()
                    # Filter out false positives
                    if "Etiqa Insurance" in name or "Etiqa today" in name:
                        continue
                    if is_valid_plan_name(name) and name.lower() not in seen_names:
                        seen_names.add(name.lower())
                        plan_data = {
                            "plan_name": name,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": config["products_url"],
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
            
            # Try to scrape individual product pages
            product_links = await self._extract_product_links(page, config["base_url"])
            for link in product_links[:5]:
                await self._random_delay(1, 2)
                try:
                    await page.goto(link, wait_until="domcontentloaded", timeout=30000)
                    await self._random_delay(1, 2)
                    
                    h1_text = await self._safe_get_text(page, "h1")
                    if h1_text and is_valid_plan_name(h1_text) and h1_text.lower() not in seen_names:
                        seen_names.add(h1_text.lower())
                        
                        page_text = await page.content()
                        coverage = self._extract_coverage_from_text(page_text)
                        limits = self._extract_limits_from_text(page_text)
                        
                        plan_data = {
                            "plan_name": h1_text,
                            "plan_type": "Medical",
                            "coverage_type": "Individual",
                            "website": link,
                            **coverage,
                            **limits,
                        }
                        plans.append(normalize_plan_data(plan_data, provider_key))
                except Exception as e:
                    logger.debug(f"Failed to scrape product page {link}: {e}")
                    continue
                    
        except Exception as e:
            logger.error(f"Failed to scrape Etiqa: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']}")
        return plans

    async def scrape_aia_with_llm(self, openai_api_key: str = None) -> List[Dict[str, Any]]:
        """
        Scrape AIA Malaysia health insurance products using LLM for PDF analysis.
        
        This method navigates to each product page, downloads the PDF brochure,
        and uses GPT-4o to extract structured data.
        
        Args:
            openai_api_key: OpenAI API key. Uses env var if not provided.
        
        Returns:
            List of normalized plan dicts with detailed information.
        """
        from helpers.llm_analyzer import InsurancePDFAnalyzer
        
        provider_key = "AIA"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        # Initialize LLM analyzer
        api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("OpenAI API key not provided. Cannot use LLM analysis.")
            return []
        
        analyzer = InsurancePDFAnalyzer(api_key=api_key)
        
        logger.info(f"Scraping {config['name']} products with LLM analysis...")
        
        page, context = await self._create_page()
        try:
            # Navigate to health protection product listing
            await page.goto(
                config["products_url"],
                wait_until="domcontentloaded",
                timeout=60000
            )
            await self._random_delay(5, 7)  # Wait for dynamic content
            
            # Accept cookies if present
            try:
                cookie_btn = await page.query_selector("button:has-text('Accept')")
                if cookie_btn:
                    await cookie_btn.click()
                    await self._random_delay(1, 2)
            except Exception:
                pass
            
            # Click "Show More" button until all products are loaded
            # AIA uses a <div> element with class "cmp-productfilterlist__more"
            show_more_clicks = await self._click_load_more_until_gone(
                page, "div.cmp-productfilterlist__more", max_clicks=15
            )
            logger.info(f"Clicked 'Show More' {show_more_clicks} times")
            await self._random_delay(2, 3)
            
            # Extract all product card links on the listing page
            product_links = []
            
            # Skip these category/overview pages
            skip_pages = [
                "overview.html", "life-protection.html", "medical-protection.html",
                "critical-illness-protection.html", "lady-protection.html", 
                "savings-and-investment.html", "retirement-protection.html",
                "health-protection.html", "employee-benefits.html",
            ]
            
            # Look for links in the page content via regex
            page_content = await page.content()
            link_patterns = [
                r'href="(/en/our-products/health-protection/[^"]+\.html)"',
                r'href="(/en/our-products/life-protection/[^"]+\.html)"',
                r'href="(/en/our-products/medical-protection/[^"]+\.html)"',
                r'href="(/en/our-products/critical-illness-protection/[^"]+\.html)"',
            ]
            
            for pattern in link_patterns:
                matches = re.findall(pattern, page_content)
                for match in matches:
                    # Skip category and overview pages
                    if any(skip in match for skip in skip_pages):
                        continue
                    if "?" in match:  # Skip filter URLs
                        continue
                    full_url = config["base_url"] + match
                    if full_url not in product_links:
                        product_links.append(full_url)
            
            # Deduplicate and limit
            product_links = list(set(product_links))[:20]
            logger.info(f"Found {len(product_links)} potential product pages to scrape")
            
            # Scrape each product page
            for link in product_links:
                await self._random_delay(2, 4)
                try:
                    logger.info(f"Scraping product page: {link}")
                    await page.goto(link, wait_until="domcontentloaded", timeout=45000)
                    await self._random_delay(2, 3)
                    
                    # Extract plan name - try multiple selectors
                    plan_name = None
                    name_selectors = [
                        "h1.cmp-title__text",  # AIA specific
                        ".product-title h1",
                        ".hero-title h1",
                        "h1",
                        ".product-name",
                        "[class*='product-title']",
                    ]
                    
                    for selector in name_selectors:
                        candidate = await self._safe_get_text(page, selector)
                        if candidate and is_valid_plan_name(candidate):
                            plan_name = candidate
                            break
                    
                    # Fallback: extract plan name from URL
                    if not plan_name or not is_valid_plan_name(plan_name):
                        # Extract from URL like /medical-protection/med-insure.html -> Med Insure
                        url_parts = link.rstrip("/").split("/")
                        if url_parts:
                            last_part = url_parts[-1].replace(".html", "").replace("-", " ")
                            # Capitalize each word
                            url_plan_name = " ".join(word.capitalize() for word in last_part.split())
                            if url_plan_name and len(url_plan_name) >= 4:
                                plan_name = url_plan_name
                                logger.info(f"Using plan name from URL: {plan_name}")
                    
                    if not plan_name:
                        logger.debug(f"Could not extract plan name on {link}")
                        continue
                    
                    if plan_name.lower() in seen_names:
                        logger.debug(f"Already scraped plan: {plan_name}")
                        continue
                    
                    seen_names.add(plan_name.lower())
                    
                    # Get description
                    description = None
                    desc_selectors = [
                        ".product-description p",
                        ".hero-description",
                        "article p:first-of-type",
                        ".product-intro p",
                        "main p:first-of-type",
                    ]
                    for selector in desc_selectors:
                        description = await self._safe_get_text(page, selector)
                        if description and len(description) > 30:
                            break
                    
                    # Get eligible age info
                    eligible_age = None
                    age_selectors = [
                        "[class*='age']",
                        ":has-text('Eligible Age') + *",
                        ":has-text('Entry Age') + *",
                    ]
                    page_text = await page.inner_text("body")
                    age_patterns = [
                        r"Eligible\s+Age[:\s]+([^\n]+)",
                        r"Entry\s+Age[:\s]+([^\n]+)",
                        r"(\d+\s*(?:days?|months?|years?)\s*[-–]\s*\d+\s*(?:days?|months?|years?)\s*old)",
                    ]
                    for pattern in age_patterns:
                        match = re.search(pattern, page_text, re.IGNORECASE)
                        if match:
                            eligible_age = match.group(1).strip()
                            break
                    
                    # Find PDF brochure link
                    pdf_url = None
                    pdf_selectors = [
                        "a[href*='.pdf']",
                        "a[href*='brochure']",
                        "a[href*='product-brochure']",
                        "a:has-text('Product brochure')",
                        "a:has-text('Download brochure')",
                        "a:has-text('PDF')",
                    ]
                    
                    for selector in pdf_selectors:
                        try:
                            pdf_el = await page.query_selector(selector)
                            if pdf_el:
                                pdf_href = await pdf_el.get_attribute("href")
                                if pdf_href:
                                    if pdf_href.startswith("/"):
                                        pdf_url = config["base_url"] + pdf_href
                                    elif not pdf_href.startswith("http"):
                                        pdf_url = config["base_url"] + "/" + pdf_href
                                    else:
                                        pdf_url = pdf_href
                                    break
                        except Exception:
                            continue
                    
                    # Also search in page content for AIA-specific PDF links
                    if not pdf_url:
                        page_html = await page.content()
                        # Look for AIA product brochure PDFs (NOT from pidm.gov.my or other providers)
                        pdf_patterns = [
                            r'href="(/content/dam/my[^"]*product-brochure[^"]*\.pdf)"',
                            r'href="(/content/dam/my[^"]*brochure[^"]*\.pdf)"',
                            r'href="(/content/dam/my-wise/[^"]*\.pdf)"',
                        ]
                        for pattern in pdf_patterns:
                            pdf_matches = re.findall(pattern, page_html, re.IGNORECASE)
                            for pdf_href in pdf_matches:
                                # Skip non-AIA brochures
                                if "pidm" in pdf_href.lower():
                                    continue
                                if pdf_href.startswith("/"):
                                    pdf_url = config["base_url"] + pdf_href
                                else:
                                    pdf_url = pdf_href
                                break
                            if pdf_url:
                                break
                    
                    # Prepare page context for LLM
                    page_context = {
                        "plan_name": plan_name,
                        "description": description or "Not available",
                        "eligible_age": eligible_age or "Not specified",
                        "provider_name": config["name"],
                    }
                    
                    # Download and analyze PDF if available
                    llm_data = {}
                    if pdf_url:
                        logger.info(f"Downloading PDF: {pdf_url}")
                        pdf_bytes = await self._download_pdf(page, pdf_url)
                        
                        if pdf_bytes:
                            logger.info(f"Analyzing PDF with LLM ({len(pdf_bytes)} bytes)")
                            try:
                                llm_data = analyzer.analyze_pdf(pdf_bytes, page_context)
                                logger.info(f"LLM extracted {len(llm_data)} fields for {plan_name}")
                            except Exception as e:
                                logger.warning(f"LLM analysis failed for {plan_name}: {e}")
                        else:
                            logger.warning(f"Failed to download PDF from {pdf_url}")
                    else:
                        logger.info(f"No PDF brochure found for {plan_name}, using page content")
                        # Fall back to page content analysis
                        page_content_text = await page.inner_text("body")
                        if len(page_content_text) > 500:
                            try:
                                llm_data = analyzer.analyze_with_llm(page_content_text, page_context)
                            except Exception as e:
                                logger.warning(f"LLM page analysis failed for {plan_name}: {e}")
                    
                    # Also extract info from page HTML
                    page_html = await page.content()
                    coverage = self._extract_coverage_from_text(page_html)
                    limits = self._extract_limits_from_text(page_html)
                    
                    # Merge all data sources (LLM data takes priority)
                    plan_data = {
                        "plan_name": llm_data.get("plan_name") or plan_name,
                        "plan_type": llm_data.get("plan_type") or "Medical",
                        "coverage_type": llm_data.get("coverage_type") or "Individual",
                        "website": link,
                        # Limits
                        "annual_limit": llm_data.get("annual_limit") or limits.get("annual_limit"),
                        "lifetime_limit": llm_data.get("lifetime_limit") or limits.get("lifetime_limit"),
                        "room_board_limit": llm_data.get("room_board_limit") or limits.get("room_board_limit"),
                        # Coverage booleans
                        "outpatient_covered": llm_data.get("outpatient_covered") if llm_data.get("outpatient_covered") is not None else coverage.get("outpatient_covered"),
                        "maternity_covered": llm_data.get("maternity_covered") if llm_data.get("maternity_covered") is not None else coverage.get("maternity_covered"),
                        "dental_covered": llm_data.get("dental_covered") if llm_data.get("dental_covered") is not None else coverage.get("dental_covered"),
                        "optical_covered": llm_data.get("optical_covered") if llm_data.get("optical_covered") is not None else coverage.get("optical_covered"),
                        "mental_health_covered": llm_data.get("mental_health_covered") if llm_data.get("mental_health_covered") is not None else coverage.get("mental_health_covered"),
                        # Lists from LLM
                        "covered_conditions": llm_data.get("covered_conditions"),
                        "excluded_conditions": llm_data.get("excluded_conditions"),
                        # Premium and deductible
                        "monthly_premium_min": llm_data.get("monthly_premium_min"),
                        "monthly_premium_max": llm_data.get("monthly_premium_max"),
                        "deductible": llm_data.get("deductible"),
                        "co_payment_percentage": llm_data.get("co_payment_percentage"),
                        # Age from LLM or page
                        "min_age": llm_data.get("min_age"),
                        "max_age": llm_data.get("max_age"),
                        # Claim process
                        "claim_process": llm_data.get("claim_process"),
                    }
                    
                    normalized = normalize_plan_data(plan_data, provider_key)
                    plans.append(normalized)
                    logger.info(f"Successfully scraped: {plan_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to scrape product page {link}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Failed to scrape AIA with LLM: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']} with LLM analysis")
        return plans

    async def scrape_allianz_with_llm(self, openai_api_key: str = None) -> List[Dict[str, Any]]:
        """
        Scrape Allianz Malaysia health insurance products using LLM for PDF analysis.
        
        Args:
            openai_api_key: OpenAI API key. Uses env var if not provided.
        
        Returns:
            List of normalized plan dicts with detailed information.
        """
        from helpers.llm_analyzer import InsurancePDFAnalyzer
        
        provider_key = "Allianz"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        # Initialize LLM analyzer
        api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("OpenAI API key not provided. Cannot use LLM analysis.")
            return []
        
        analyzer = InsurancePDFAnalyzer(api_key=api_key)
        
        logger.info(f"Scraping {config['name']} products with LLM analysis...")
        
        page, context = await self._create_page()
        try:
            # Navigate to medical/hospitalisation product listing
            await page.goto(
                config["products_url"],
                wait_until="domcontentloaded",
                timeout=60000
            )
            await self._random_delay(5, 7)
            
            # Accept cookies if present
            try:
                cookie_btn = await page.query_selector("button:has-text('Accept')")
                if cookie_btn:
                    await cookie_btn.click()
                    await self._random_delay(1, 2)
            except Exception:
                pass
            
            # Extract product links from the page
            product_links = []
            page_content = await page.content()
            
            # Allianz-specific link patterns - ONLY medical-and-hospitalisation products
            link_patterns = [
                # Only match links within medical-and-hospitalisation section
                r'href="(/personal/life-health-and-savings/medical-and-hospitalisation/[^"]+\.html)"',
            ]
            
            # Skip category pages
            skip_pages = [
                "medical-and-hospitalisation.html", "life-health-and-savings.html",
                "overview.html", "contact-us.html", "faq.html", "claims",
            ]
            
            # Pages outside medical-and-hospitalisation should be excluded
            exclude_sections = [
                "/life-protection/", "/personal-accident/", "/savings-investments",
                "/critical-illness/", "/help-and-services/", "/a-z-reads/",
            ]
            
            for pattern in link_patterns:
                matches = re.findall(pattern, page_content)
                for match in matches:
                    if any(skip in match for skip in skip_pages):
                        continue
                    if any(excl in match for excl in exclude_sections):
                        continue
                    if "?" in match:
                        continue
                    full_url = config["base_url"] + match
                    if full_url not in product_links:
                        product_links.append(full_url)
            
            # Also look for product card links - but only within medical-and-hospitalisation
            card_selectors = [
                ".product-card a",
                ".product-tile a",
                "[class*='product'] a",
                ".card-body a",
            ]
            for selector in card_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        href = await el.get_attribute("href")
                        if href and "/medical-and-hospitalisation/" in href:
                            if href.startswith("/"):
                                href = config["base_url"] + href
                            if href not in product_links and not any(skip in href for skip in skip_pages):
                                product_links.append(href)
                except Exception:
                    continue
            
            product_links = list(set(product_links))[:20]
            logger.info(f"Found {len(product_links)} potential product pages to scrape")
            
            # Scrape each product page
            for link in product_links:
                await self._random_delay(2, 4)
                try:
                    logger.info(f"Scraping product page: {link}")
                    await page.goto(link, wait_until="domcontentloaded", timeout=45000)
                    await self._random_delay(2, 3)
                    
                    plan_name = await self._safe_get_text(page, "h1")
                    
                    if not plan_name or not is_valid_plan_name(plan_name):
                        logger.debug(f"Invalid plan name '{plan_name}' on {link}")
                        continue
                    
                    if plan_name.lower() in seen_names:
                        continue
                    
                    seen_names.add(plan_name.lower())
                    
                    # Get description
                    description = await self._safe_get_text(page, ".product-description p")
                    if not description:
                        description = await self._safe_get_text(page, "article p")
                    
                    # Get eligible age
                    page_text = await page.inner_text("body")
                    eligible_age = None
                    age_match = re.search(r"(?:entry|eligible)\s+age[:\s]+([^\n]+)", page_text, re.IGNORECASE)
                    if age_match:
                        eligible_age = age_match.group(1).strip()
                    
                    # Find PDF brochure
                    pdf_url = None
                    pdf_selectors = [
                        "a[href*='.pdf']",
                        "a:has-text('Brochure')",
                        "a:has-text('Download')",
                        "a:has-text('Product Disclosure')",
                    ]
                    
                    for selector in pdf_selectors:
                        try:
                            pdf_el = await page.query_selector(selector)
                            if pdf_el:
                                pdf_href = await pdf_el.get_attribute("href")
                                if pdf_href and ".pdf" in pdf_href.lower():
                                    if pdf_href.startswith("/"):
                                        pdf_url = config["base_url"] + pdf_href
                                    elif not pdf_href.startswith("http"):
                                        pdf_url = config["base_url"] + "/" + pdf_href
                                    else:
                                        pdf_url = pdf_href
                                    break
                        except Exception:
                            continue
                    
                    # Page context for LLM
                    page_context = {
                        "plan_name": plan_name,
                        "description": description or "Not available",
                        "eligible_age": eligible_age or "Not specified",
                        "provider_name": config["name"],
                    }
                    
                    # Download and analyze PDF
                    llm_data = {}
                    if pdf_url:
                        logger.info(f"Downloading PDF: {pdf_url}")
                        pdf_bytes = await self._download_pdf(page, pdf_url)
                        
                        if pdf_bytes:
                            try:
                                llm_data = analyzer.analyze_pdf(pdf_bytes, page_context)
                                logger.info(f"LLM extracted {len(llm_data)} fields for {plan_name}")
                            except Exception as e:
                                logger.warning(f"LLM analysis failed for {plan_name}: {e}")
                    else:
                        # Fall back to page content
                        if len(page_text) > 500:
                            try:
                                llm_data = analyzer.analyze_with_llm(page_text, page_context)
                            except Exception as e:
                                logger.warning(f"LLM page analysis failed for {plan_name}: {e}")
                    
                    # Extract additional info from page
                    page_html = await page.content()
                    coverage = self._extract_coverage_from_text(page_html)
                    limits = self._extract_limits_from_text(page_html)
                    
                    # Merge data
                    plan_data = {
                        "plan_name": llm_data.get("plan_name") or plan_name,
                        "plan_type": llm_data.get("plan_type") or "Medical",
                        "coverage_type": llm_data.get("coverage_type") or "Individual",
                        "website": link,
                        "annual_limit": llm_data.get("annual_limit") or limits.get("annual_limit"),
                        "lifetime_limit": llm_data.get("lifetime_limit") or limits.get("lifetime_limit"),
                        "room_board_limit": llm_data.get("room_board_limit") or limits.get("room_board_limit"),
                        "outpatient_covered": llm_data.get("outpatient_covered") if llm_data.get("outpatient_covered") is not None else coverage.get("outpatient_covered"),
                        "maternity_covered": llm_data.get("maternity_covered") if llm_data.get("maternity_covered") is not None else coverage.get("maternity_covered"),
                        "dental_covered": llm_data.get("dental_covered") if llm_data.get("dental_covered") is not None else coverage.get("dental_covered"),
                        "optical_covered": llm_data.get("optical_covered") if llm_data.get("optical_covered") is not None else coverage.get("optical_covered"),
                        "mental_health_covered": llm_data.get("mental_health_covered") if llm_data.get("mental_health_covered") is not None else coverage.get("mental_health_covered"),
                        "covered_conditions": llm_data.get("covered_conditions"),
                        "excluded_conditions": llm_data.get("excluded_conditions"),
                        "monthly_premium_min": llm_data.get("monthly_premium_min"),
                        "monthly_premium_max": llm_data.get("monthly_premium_max"),
                        "deductible": llm_data.get("deductible"),
                        "co_payment_percentage": llm_data.get("co_payment_percentage"),
                        "min_age": llm_data.get("min_age"),
                        "max_age": llm_data.get("max_age"),
                        "claim_process": llm_data.get("claim_process"),
                    }
                    
                    normalized = normalize_plan_data(plan_data, provider_key)
                    plans.append(normalized)
                    logger.info(f"Successfully scraped: {plan_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to scrape product page {link}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Failed to scrape Allianz with LLM: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']} with LLM analysis")
        return plans

    async def scrape_great_eastern_with_llm(self, openai_api_key: str = None) -> List[Dict[str, Any]]:
        """
        Scrape Great Eastern health insurance products using LLM for PDF analysis.
        Clicks "Load More" button until all products are visible.
        
        Args:
            openai_api_key: OpenAI API key. Uses env var if not provided.
        
        Returns:
            List of normalized plan dicts with detailed information.
        """
        from helpers.llm_analyzer import InsurancePDFAnalyzer
        
        provider_key = "GreatEastern"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("OpenAI API key not provided. Cannot use LLM analysis.")
            return []
        
        analyzer = InsurancePDFAnalyzer(api_key=api_key)
        
        logger.info(f"Scraping {config['name']} products with LLM analysis...")
        
        page, context = await self._create_page()
        try:
            await page.goto(
                config["products_url"],
                wait_until="domcontentloaded",
                timeout=60000
            )
            await self._random_delay(5, 7)
            
            # Accept cookies
            try:
                cookie_btn = await page.query_selector("button:has-text('Accept')")
                if cookie_btn:
                    await cookie_btn.click()
                    await self._random_delay(1, 2)
            except Exception:
                pass
            
            # Click "Load More" button until all products are loaded
            # Great Eastern uses a <button> with id="load-more"
            load_more_clicks = await self._click_load_more_until_gone(
                page, "#load-more", max_clicks=20
            )
            logger.info(f"Clicked 'Load More' {load_more_clicks} times")
            await self._random_delay(2, 3)
            
            # Extract product links - get ALL health-insurance products (LLM will filter later)
            product_links = []
            page_content = await page.content()
            
            # Great Eastern - get all links within health-insurance section
            # Note: Links can be absolute (https://...) or relative (/my/en/...)
            link_patterns = [
                # Absolute URLs
                r'href="(https://www\.greateasternlife\.com/my/en/personal-insurance/our-products/health-insurance/[^"]+)"',
                # Relative URLs
                r'href="(/my/en/personal-insurance/our-products/health-insurance/[^"]+)"',
            ]
            
            skip_pages = ["our-products.html", "overview.html", "contact", "health-insurance.html"]
            
            for pattern in link_patterns:
                matches = re.findall(pattern, page_content)
                for match in matches:
                    if any(skip in match for skip in skip_pages):
                        continue
                    if "?" in match and "category=" not in match:
                        continue
                    if match.startswith("http"):
                        full_url = match
                    else:
                        full_url = config["base_url"] + match
                    if full_url not in product_links:
                        product_links.append(full_url)
            
            # Also get links from product cards within health-insurance
            # Be explicit about the product names we're looking for
            card_selectors = [
                "a[href*='great-health-direct']",
                "a[href*='great-medivalue']",
                "a[href*='great-medic-lite']",
                "a[href*='smart-health-protector']",
                "a[href*='smart-baby-shield']",
                "a[href*='smartmedic-shield']",
                "a[href*='/health-insurance/great-']",
                "a[href*='/health-insurance/smart-']",
                "a[href*='/health-insurance/'][href$='.html']",
            ]
            for selector in card_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        href = await el.get_attribute("href")
                        if href and "/health-insurance/" in href:
                            # Skip main category pages
                            if any(skip in href for skip in skip_pages):
                                continue
                            if href.startswith("/"):
                                href = config["base_url"] + href
                            if href not in product_links:
                                product_links.append(href)
                except Exception:
                    continue
            
            product_links = list(set(product_links))[:25]  # Allow more products, LLM will filter
            logger.info(f"Found {len(product_links)} potential health insurance pages (LLM will classify)")
            
            # Scrape each product page with LLM classification
            for link in product_links:
                await self._random_delay(2, 4)
                try:
                    logger.info(f"Scraping product page: {link}")
                    await page.goto(link, wait_until="domcontentloaded", timeout=45000)
                    await self._random_delay(2, 3)
                    
                    plan_name = await self._safe_get_text(page, "h1")
                    
                    # Clean plan name - Great Eastern uses format: "Plan | Description | Category"
                    if plan_name and "|" in plan_name:
                        plan_name = plan_name.split("|")[0].strip()
                    
                    if not plan_name or not is_valid_plan_name(plan_name):
                        continue
                    
                    if plan_name.lower() in seen_names:
                        continue
                    
                    # Get description and key benefits for LLM classification
                    description = await self._safe_get_text(page, ".product-description")
                    if not description:
                        description = await self._safe_get_text(page, "article p")
                    
                    key_benefits = await self._safe_get_text(page, "[class*='benefit']")
                    if not key_benefits:
                        key_benefits = await self._safe_get_text(page, "[class*='feature']")
                    
                    classification_text = f"{description or ''} {key_benefits or ''}"
                    
                    # Use LLM to classify: Medical Insurance vs Critical Illness
                    is_medical = await self._classify_plan_type_with_llm(
                        plan_name, 
                        classification_text[:1000],  # Limit text length
                        openai_api_key
                    )
                    
                    if not is_medical:
                        logger.info(f"Skipping '{plan_name}' - classified as Critical Illness")
                        continue
                    
                    seen_names.add(plan_name.lower())
                    
                    page_text = await page.inner_text("body")
                    eligible_age = None
                    age_match = re.search(r"(?:entry|eligible|age)[:\s]+(\d+[^\.]+)", page_text, re.IGNORECASE)
                    if age_match:
                        eligible_age = age_match.group(1).strip()
                    
                    # Find PDF brochure
                    pdf_url = None
                    pdf_selectors = [
                        "a[href*='.pdf']",
                        "a:has-text('Brochure')",
                        "a:has-text('Product Summary')",
                        "a:has-text('Download')",
                    ]
                    
                    for selector in pdf_selectors:
                        try:
                            pdf_el = await page.query_selector(selector)
                            if pdf_el:
                                pdf_href = await pdf_el.get_attribute("href")
                                if pdf_href and ".pdf" in pdf_href.lower():
                                    if pdf_href.startswith("/"):
                                        pdf_url = config["base_url"] + pdf_href
                                    elif not pdf_href.startswith("http"):
                                        pdf_url = config["base_url"] + "/" + pdf_href
                                    else:
                                        pdf_url = pdf_href
                                    break
                        except Exception:
                            continue
                    
                    page_context = {
                        "plan_name": plan_name,
                        "description": description or "Not available",
                        "eligible_age": eligible_age or "Not specified",
                        "provider_name": config["name"],
                    }
                    
                    llm_data = {}
                    if pdf_url:
                        logger.info(f"Downloading PDF: {pdf_url}")
                        pdf_bytes = await self._download_pdf(page, pdf_url)
                        
                        if pdf_bytes:
                            try:
                                llm_data = analyzer.analyze_pdf(pdf_bytes, page_context)
                                logger.info(f"LLM extracted {len(llm_data)} fields for {plan_name}")
                            except Exception as e:
                                logger.warning(f"LLM analysis failed for {plan_name}: {e}")
                    else:
                        if len(page_text) > 500:
                            try:
                                llm_data = analyzer.analyze_with_llm(page_text, page_context)
                            except Exception as e:
                                logger.warning(f"LLM page analysis failed for {plan_name}: {e}")
                    
                    page_html = await page.content()
                    coverage = self._extract_coverage_from_text(page_html)
                    limits = self._extract_limits_from_text(page_html)
                    
                    plan_data = {
                        "plan_name": llm_data.get("plan_name") or plan_name,
                        "plan_type": llm_data.get("plan_type") or "Medical",
                        "coverage_type": llm_data.get("coverage_type") or "Individual",
                        "website": link,
                        "annual_limit": llm_data.get("annual_limit") or limits.get("annual_limit"),
                        "lifetime_limit": llm_data.get("lifetime_limit") or limits.get("lifetime_limit"),
                        "room_board_limit": llm_data.get("room_board_limit") or limits.get("room_board_limit"),
                        "outpatient_covered": llm_data.get("outpatient_covered") if llm_data.get("outpatient_covered") is not None else coverage.get("outpatient_covered"),
                        "maternity_covered": llm_data.get("maternity_covered") if llm_data.get("maternity_covered") is not None else coverage.get("maternity_covered"),
                        "dental_covered": llm_data.get("dental_covered") if llm_data.get("dental_covered") is not None else coverage.get("dental_covered"),
                        "optical_covered": llm_data.get("optical_covered") if llm_data.get("optical_covered") is not None else coverage.get("optical_covered"),
                        "mental_health_covered": llm_data.get("mental_health_covered") if llm_data.get("mental_health_covered") is not None else coverage.get("mental_health_covered"),
                        "covered_conditions": llm_data.get("covered_conditions"),
                        "excluded_conditions": llm_data.get("excluded_conditions"),
                        "monthly_premium_min": llm_data.get("monthly_premium_min"),
                        "monthly_premium_max": llm_data.get("monthly_premium_max"),
                        "deductible": llm_data.get("deductible"),
                        "co_payment_percentage": llm_data.get("co_payment_percentage"),
                        "min_age": llm_data.get("min_age"),
                        "max_age": llm_data.get("max_age"),
                        "claim_process": llm_data.get("claim_process"),
                    }
                    
                    normalized = normalize_plan_data(plan_data, provider_key)
                    plans.append(normalized)
                    logger.info(f"Successfully scraped: {plan_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to scrape product page {link}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Failed to scrape Great Eastern with LLM: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']} with LLM analysis")
        return plans

    async def scrape_etiqa_with_llm(self, openai_api_key: str = None) -> List[Dict[str, Any]]:
        """
        Scrape Etiqa health insurance products using LLM for PDF analysis.
        
        Args:
            openai_api_key: OpenAI API key. Uses env var if not provided.
        
        Returns:
            List of normalized plan dicts with detailed information.
        """
        from helpers.llm_analyzer import InsurancePDFAnalyzer
        
        provider_key = "Etiqa"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("OpenAI API key not provided. Cannot use LLM analysis.")
            return []
        
        analyzer = InsurancePDFAnalyzer(api_key=api_key)
        
        logger.info(f"Scraping {config['name']} products with LLM analysis...")
        
        page, context = await self._create_page()
        try:
            await page.goto(
                config["products_url"],
                wait_until="domcontentloaded",
                timeout=60000
            )
            await self._random_delay(5, 7)
            
            # Accept cookies
            try:
                cookie_btn = await page.query_selector("button:has-text('Accept')")
                if cookie_btn:
                    await cookie_btn.click()
                    await self._random_delay(1, 2)
            except Exception:
                pass
            
            # Extract product links
            product_links = []
            page_content = await page.content()
            
            # Etiqa-specific patterns
            link_patterns = [
                r'href="(/health/[^"]+)"',
                r'href="(/v2/health/[^"]+)"',
                r'href="(/medical[^"]*)"',
                r'href="(https://www\.etiqa\.com\.my/[^"]*health[^"]*)"',
            ]
            
            skip_pages = ["health$", "contact", "faq", "claim"]
            
            for pattern in link_patterns:
                matches = re.findall(pattern, page_content)
                for match in matches:
                    if any(skip in match for skip in skip_pages):
                        continue
                    if match.startswith("http"):
                        full_url = match
                    elif match.startswith("/"):
                        full_url = config["base_url"] + match
                    else:
                        full_url = config["base_url"] + "/" + match
                    if full_url not in product_links:
                        product_links.append(full_url)
            
            # Also look for product cards
            card_selectors = [
                ".product-card a",
                ".plan-card a",
                "[class*='product'] a",
                "[class*='plan'] a",
            ]
            for selector in card_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        href = await el.get_attribute("href")
                        if href:
                            if href.startswith("/"):
                                href = config["base_url"] + href
                            if href not in product_links:
                                product_links.append(href)
                except Exception:
                    continue
            
            product_links = list(set(product_links))[:20]
            logger.info(f"Found {len(product_links)} potential health product pages (LLM will classify)")
            
            # Scrape each product page with LLM classification
            for link in product_links:
                await self._random_delay(2, 4)
                try:
                    logger.info(f"Scraping product page: {link}")
                    await page.goto(link, wait_until="domcontentloaded", timeout=45000)
                    await self._random_delay(2, 3)
                    
                    plan_name = await self._safe_get_text(page, "h1")
                    
                    if not plan_name or not is_valid_plan_name(plan_name):
                        continue
                    
                    if plan_name.lower() in seen_names:
                        continue
                    
                    # Get description and key benefits for LLM classification
                    description = await self._safe_get_text(page, ".product-description")
                    if not description:
                        description = await self._safe_get_text(page, ".hero-description")
                    
                    key_benefits = await self._safe_get_text(page, "[class*='benefit']")
                    if not key_benefits:
                        key_benefits = await self._safe_get_text(page, "[class*='feature']")
                    
                    classification_text = f"{description or ''} {key_benefits or ''}"
                    
                    # Use LLM to classify: Medical Insurance vs Critical Illness
                    is_medical = await self._classify_plan_type_with_llm(
                        plan_name, 
                        classification_text[:1000],  # Limit text length
                        openai_api_key
                    )
                    
                    if not is_medical:
                        logger.info(f"Skipping '{plan_name}' - classified as Critical Illness")
                        continue
                    
                    seen_names.add(plan_name.lower())
                    
                    page_text = await page.inner_text("body")
                    eligible_age = None
                    age_match = re.search(r"(?:entry|eligible|age)[:\s]+(\d+[^\.]+)", page_text, re.IGNORECASE)
                    if age_match:
                        eligible_age = age_match.group(1).strip()
                    
                    # Find PDF brochure
                    pdf_url = None
                    pdf_selectors = [
                        "a[href*='.pdf']",
                        "a:has-text('Brochure')",
                        "a:has-text('Product Disclosure')",
                        "a:has-text('Download')",
                    ]
                    
                    for selector in pdf_selectors:
                        try:
                            pdf_el = await page.query_selector(selector)
                            if pdf_el:
                                pdf_href = await pdf_el.get_attribute("href")
                                if pdf_href and ".pdf" in pdf_href.lower():
                                    if pdf_href.startswith("/"):
                                        pdf_url = config["base_url"] + pdf_href
                                    elif not pdf_href.startswith("http"):
                                        pdf_url = config["base_url"] + "/" + pdf_href
                                    else:
                                        pdf_url = pdf_href
                                    break
                        except Exception:
                            continue
                    
                    page_context = {
                        "plan_name": plan_name,
                        "description": description or "Not available",
                        "eligible_age": eligible_age or "Not specified",
                        "provider_name": config["name"],
                    }
                    
                    llm_data = {}
                    if pdf_url:
                        logger.info(f"Downloading PDF: {pdf_url}")
                        pdf_bytes = await self._download_pdf(page, pdf_url)
                        
                        if pdf_bytes:
                            try:
                                llm_data = analyzer.analyze_pdf(pdf_bytes, page_context)
                                logger.info(f"LLM extracted {len(llm_data)} fields for {plan_name}")
                            except Exception as e:
                                logger.warning(f"LLM analysis failed for {plan_name}: {e}")
                    else:
                        if len(page_text) > 500:
                            try:
                                llm_data = analyzer.analyze_with_llm(page_text, page_context)
                            except Exception as e:
                                logger.warning(f"LLM page analysis failed for {plan_name}: {e}")
                    
                    page_html = await page.content()
                    coverage = self._extract_coverage_from_text(page_html)
                    limits = self._extract_limits_from_text(page_html)
                    
                    plan_data = {
                        "plan_name": llm_data.get("plan_name") or plan_name,
                        "plan_type": llm_data.get("plan_type") or "Medical",
                        "coverage_type": llm_data.get("coverage_type") or "Individual",
                        "website": link,
                        "annual_limit": llm_data.get("annual_limit") or limits.get("annual_limit"),
                        "lifetime_limit": llm_data.get("lifetime_limit") or limits.get("lifetime_limit"),
                        "room_board_limit": llm_data.get("room_board_limit") or limits.get("room_board_limit"),
                        "outpatient_covered": llm_data.get("outpatient_covered") if llm_data.get("outpatient_covered") is not None else coverage.get("outpatient_covered"),
                        "maternity_covered": llm_data.get("maternity_covered") if llm_data.get("maternity_covered") is not None else coverage.get("maternity_covered"),
                        "dental_covered": llm_data.get("dental_covered") if llm_data.get("dental_covered") is not None else coverage.get("dental_covered"),
                        "optical_covered": llm_data.get("optical_covered") if llm_data.get("optical_covered") is not None else coverage.get("optical_covered"),
                        "mental_health_covered": llm_data.get("mental_health_covered") if llm_data.get("mental_health_covered") is not None else coverage.get("mental_health_covered"),
                        "covered_conditions": llm_data.get("covered_conditions"),
                        "excluded_conditions": llm_data.get("excluded_conditions"),
                        "monthly_premium_min": llm_data.get("monthly_premium_min"),
                        "monthly_premium_max": llm_data.get("monthly_premium_max"),
                        "deductible": llm_data.get("deductible"),
                        "co_payment_percentage": llm_data.get("co_payment_percentage"),
                        "min_age": llm_data.get("min_age"),
                        "max_age": llm_data.get("max_age"),
                        "claim_process": llm_data.get("claim_process"),
                    }
                    
                    normalized = normalize_plan_data(plan_data, provider_key)
                    plans.append(normalized)
                    logger.info(f"Successfully scraped: {plan_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to scrape product page {link}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Failed to scrape Etiqa with LLM: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']} with LLM analysis")
        return plans

    async def scrape_prudential_with_llm(self, openai_api_key: str = None) -> List[Dict[str, Any]]:
        """
        Scrape Prudential Malaysia health insurance products using LLM for PDF analysis.
        
        Args:
            openai_api_key: OpenAI API key. Uses env var if not provided.
        
        Returns:
            List of normalized plan dicts with detailed information.
        """
        from helpers.llm_analyzer import InsurancePDFAnalyzer
        
        provider_key = "Prudential"
        config = PROVIDERS[provider_key]
        plans = []
        seen_names = set()
        
        api_key = openai_api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            logger.error("OpenAI API key not provided. Cannot use LLM analysis.")
            return []
        
        analyzer = InsurancePDFAnalyzer(api_key=api_key)
        
        logger.info(f"Scraping {config['name']} products with LLM analysis...")
        
        page, context = await self._create_page()
        try:
            await page.goto(
                config["products_url"],
                wait_until="domcontentloaded",
                timeout=60000
            )
            await self._random_delay(5, 7)
            
            # Accept cookies
            try:
                cookie_btn = await page.query_selector("button:has-text('Accept')")
                if cookie_btn:
                    await cookie_btn.click()
                    await self._random_delay(1, 2)
            except Exception:
                pass
            
            # Extract product links from "Discover our Medical Plans" section
            # Prudential medical products are under /products-riders/ not /medical-plans/
            product_links = []
            
            # The most reliable approach: find card links in the medical plans section
            # Product cards link to /products-riders/prumillion-med/, /products-riders/pruvalue-med/, etc.
            card_selectors = [
                # Product cards on the medical-plans page link to products-riders
                "[class*='card'] a[href*='/products-riders/']",
                "a[href*='prumillion-med']",
                "a[href*='pruvalue-med']",
                # Generic card selectors
                "[class*='card'] a",
            ]
            
            # Skip non-medical product links
            skip_patterns = [
                "/critical-illness", "/life-insurance", "/wealth-insurance",
                "/savings-investment", "newsroom", "announcements",
                "epay.", "partnersweb.", "claims-and-support",
            ]
            
            for selector in card_selectors:
                try:
                    elements = await page.query_selector_all(selector)
                    for el in elements:
                        href = await el.get_attribute("href")
                        if not href:
                            continue
                        # Only include links to products-riders that look like medical plans
                        if "/products-riders/" in href:
                            # Check if it contains med keywords
                            if "med" in href.lower():
                                if any(skip in href.lower() for skip in skip_patterns):
                                    continue
                                if href.startswith("/"):
                                    href = config["base_url"] + href
                                if href not in product_links:
                                    product_links.append(href)
                except Exception as e:
                    logger.debug(f"Selector {selector} error: {e}")
                    continue
            
            product_links = list(set(product_links))[:10]  # Medical plans page typically has few products
            logger.info(f"Found {len(product_links)} medical plan pages under 'Discover our Medical Plans'")
            
            # Scrape each product page - all should be Medical Insurance (from medical-plans section)
            for link in product_links:
                await self._random_delay(2, 4)
                try:
                    logger.info(f"Scraping medical plan page: {link}")
                    await page.goto(link, wait_until="domcontentloaded", timeout=45000)
                    await self._random_delay(2, 3)
                    
                    plan_name = await self._safe_get_text(page, "h1")
                    
                    if not plan_name or not is_valid_plan_name(plan_name):
                        continue
                    
                    if plan_name.lower() in seen_names:
                        continue
                    
                    seen_names.add(plan_name.lower())
                    
                    description = await self._safe_get_text(page, ".product-description")
                    if not description:
                        description = await self._safe_get_text(page, "article p")
                    
                    page_text = await page.inner_text("body")
                    eligible_age = None
                    age_match = re.search(r"(?:entry|eligible|age)[:\s]+(\d+[^\.]+)", page_text, re.IGNORECASE)
                    if age_match:
                        eligible_age = age_match.group(1).strip()
                    
                    # Find PDF brochure
                    pdf_url = None
                    pdf_selectors = [
                        "a[href*='.pdf']",
                        "a:has-text('Brochure')",
                        "a:has-text('Product Summary')",
                        "a:has-text('Download')",
                    ]
                    
                    for selector in pdf_selectors:
                        try:
                            pdf_el = await page.query_selector(selector)
                            if pdf_el:
                                pdf_href = await pdf_el.get_attribute("href")
                                if pdf_href and ".pdf" in pdf_href.lower():
                                    if pdf_href.startswith("/"):
                                        pdf_url = config["base_url"] + pdf_href
                                    elif not pdf_href.startswith("http"):
                                        pdf_url = config["base_url"] + "/" + pdf_href
                                    else:
                                        pdf_url = pdf_href
                                    break
                        except Exception:
                            continue
                    
                    page_context = {
                        "plan_name": plan_name,
                        "description": description or "Not available",
                        "eligible_age": eligible_age or "Not specified",
                        "provider_name": config["name"],
                    }
                    
                    llm_data = {}
                    if pdf_url:
                        logger.info(f"Downloading PDF: {pdf_url}")
                        pdf_bytes = await self._download_pdf(page, pdf_url)
                        
                        if pdf_bytes:
                            try:
                                llm_data = analyzer.analyze_pdf(pdf_bytes, page_context)
                                logger.info(f"LLM extracted {len(llm_data)} fields for {plan_name}")
                            except Exception as e:
                                logger.warning(f"LLM analysis failed for {plan_name}: {e}")
                    else:
                        if len(page_text) > 500:
                            try:
                                llm_data = analyzer.analyze_with_llm(page_text, page_context)
                            except Exception as e:
                                logger.warning(f"LLM page analysis failed for {plan_name}: {e}")
                    
                    page_html = await page.content()
                    coverage = self._extract_coverage_from_text(page_html)
                    limits = self._extract_limits_from_text(page_html)
                    
                    plan_data = {
                        "plan_name": llm_data.get("plan_name") or plan_name,
                        "plan_type": llm_data.get("plan_type") or "Medical",
                        "coverage_type": llm_data.get("coverage_type") or "Individual",
                        "website": link,
                        "annual_limit": llm_data.get("annual_limit") or limits.get("annual_limit"),
                        "lifetime_limit": llm_data.get("lifetime_limit") or limits.get("lifetime_limit"),
                        "room_board_limit": llm_data.get("room_board_limit") or limits.get("room_board_limit"),
                        "outpatient_covered": llm_data.get("outpatient_covered") if llm_data.get("outpatient_covered") is not None else coverage.get("outpatient_covered"),
                        "maternity_covered": llm_data.get("maternity_covered") if llm_data.get("maternity_covered") is not None else coverage.get("maternity_covered"),
                        "dental_covered": llm_data.get("dental_covered") if llm_data.get("dental_covered") is not None else coverage.get("dental_covered"),
                        "optical_covered": llm_data.get("optical_covered") if llm_data.get("optical_covered") is not None else coverage.get("optical_covered"),
                        "mental_health_covered": llm_data.get("mental_health_covered") if llm_data.get("mental_health_covered") is not None else coverage.get("mental_health_covered"),
                        "covered_conditions": llm_data.get("covered_conditions"),
                        "excluded_conditions": llm_data.get("excluded_conditions"),
                        "monthly_premium_min": llm_data.get("monthly_premium_min"),
                        "monthly_premium_max": llm_data.get("monthly_premium_max"),
                        "deductible": llm_data.get("deductible"),
                        "co_payment_percentage": llm_data.get("co_payment_percentage"),
                        "min_age": llm_data.get("min_age"),
                        "max_age": llm_data.get("max_age"),
                        "claim_process": llm_data.get("claim_process"),
                    }
                    
                    normalized = normalize_plan_data(plan_data, provider_key)
                    plans.append(normalized)
                    logger.info(f"Successfully scraped: {plan_name}")
                    
                except Exception as e:
                    logger.error(f"Failed to scrape product page {link}: {e}")
                    continue
            
        except Exception as e:
            logger.error(f"Failed to scrape Prudential with LLM: {e}")
        finally:
            await page.close()
            await context.close()
        
        logger.info(f"Scraped {len(plans)} plans from {config['name']} with LLM analysis")
        return plans

    async def scrape_all_providers_with_llm(self, openai_api_key: str = None) -> List[Dict[str, Any]]:
        """
        Scrape all insurance providers using LLM-powered PDF analysis.
        
        Args:
            openai_api_key: OpenAI API key. Uses env var if not provided.
        
        Returns:
            List of normalized plan dicts from all providers.
        """
        all_plans = []
        
        scrapers = [
            ("AIA", self.scrape_aia_with_llm),
            ("Allianz", self.scrape_allianz_with_llm),
            ("GreatEastern", self.scrape_great_eastern_with_llm),
            ("Etiqa", self.scrape_etiqa_with_llm),
            ("Prudential", self.scrape_prudential_with_llm),
        ]
        
        for provider_name, scraper in scrapers:
            try:
                logger.info(f"Starting LLM scrape for {provider_name}...")
                plans = await scraper(openai_api_key)
                all_plans.extend(plans)
                logger.info(f"Scraped {len(plans)} plans from {provider_name}")
                await self._random_delay(5, 10)  # Delay between providers
            except Exception as e:
                logger.error(f"Failed to scrape {provider_name} with LLM: {e}")
        
        logger.info(f"Total plans scraped from all providers: {len(all_plans)}")
        return all_plans

    async def scrape_all_providers(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Scrape all insurance providers.
        
        Returns:
            Dict mapping provider key to list of normalized plan dicts.
        """
        results = {}
        
        scraper_methods = [
            ("AIA", self.scrape_aia),
            ("Prudential", self.scrape_prudential),
            ("Allianz", self.scrape_allianz),
            ("GreatEastern", self.scrape_great_eastern),
            ("Etiqa", self.scrape_etiqa),
        ]
        
        for provider_key, scraper in scraper_methods:
            try:
                plans = await scraper()
                results[provider_key] = plans
                await self._random_delay(3, 5)  # Delay between providers
            except Exception as e:
                logger.error(f"Failed to scrape {provider_key}: {e}")
                results[provider_key] = []
        
        return results


async def scrape_insurance_plans(headless: bool = True) -> List[Dict[str, Any]]:
    """
    Scrape insurance plans from all providers.
    
    Args:
        headless: Run browser in headless mode.
    
    Returns:
        List of normalized plan dicts ready for database insertion.
    """
    all_plans = []
    
    async with InsuranceScraper(headless=headless) as scraper:
        results = await scraper.scrape_all_providers()
        
        for provider_key, plans in results.items():
            all_plans.extend(plans)
            logger.info(f"{provider_key}: {len(plans)} plans")
    
    logger.info(f"Total plans scraped: {len(all_plans)}")
    return all_plans


def scrape_insurance_plans_sync(headless: bool = True) -> List[Dict[str, Any]]:
    """
    Synchronous wrapper for scraping insurance plans.
    
    Args:
        headless: Run browser in headless mode.
    
    Returns:
        List of normalized plan dicts.
    """
    return asyncio.run(scrape_insurance_plans(headless=headless))


async def scrape_aia_with_llm(
    headless: bool = True,
    openai_api_key: str = None
) -> List[Dict[str, Any]]:
    """
    Scrape AIA insurance plans using LLM-powered PDF analysis.
    
    Args:
        headless: Run browser in headless mode.
        openai_api_key: OpenAI API key (optional, uses env var if not provided).
    
    Returns:
        List of normalized plan dicts with detailed information.
    """
    async with InsuranceScraper(headless=headless) as scraper:
        plans = await scraper.scrape_aia_with_llm(openai_api_key=openai_api_key)
    
    logger.info(f"Total AIA plans scraped with LLM: {len(plans)}")
    return plans


def scrape_aia_with_llm_sync(
    headless: bool = True,
    openai_api_key: str = None
) -> List[Dict[str, Any]]:
    """
    Synchronous wrapper for LLM-powered AIA scraping.
    
    Args:
        headless: Run browser in headless mode.
        openai_api_key: OpenAI API key (optional, uses env var if not provided).
    
    Returns:
        List of normalized plan dicts.
    """
    return asyncio.run(scrape_aia_with_llm(headless=headless, openai_api_key=openai_api_key))


async def scrape_all_providers_with_llm(
    headless: bool = True,
    openai_api_key: str = None
) -> List[Dict[str, Any]]:
    """
    Scrape all insurance providers using LLM-powered PDF analysis.
    
    Args:
        headless: Run browser in headless mode.
        openai_api_key: OpenAI API key (optional, uses env var if not provided).
    
    Returns:
        List of normalized plan dicts from all providers.
    """
    async with InsuranceScraper(headless=headless) as scraper:
        plans = await scraper.scrape_all_providers_with_llm(openai_api_key=openai_api_key)
    
    logger.info(f"Total plans scraped with LLM from all providers: {len(plans)}")
    return plans


def scrape_all_providers_with_llm_sync(
    headless: bool = True,
    openai_api_key: str = None
) -> List[Dict[str, Any]]:
    """
    Synchronous wrapper for LLM-powered scraping of all providers.
    
    Args:
        headless: Run browser in headless mode.
        openai_api_key: OpenAI API key (optional, uses env var if not provided).
    
    Returns:
        List of normalized plan dicts from all providers.
    """
    return asyncio.run(scrape_all_providers_with_llm(headless=headless, openai_api_key=openai_api_key))

