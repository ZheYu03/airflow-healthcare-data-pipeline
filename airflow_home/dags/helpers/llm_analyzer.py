"""LLM-powered analyzer for insurance PDF brochures.

Uses OpenAI GPT-4o to extract structured data from insurance product brochures
and map them to the Supabase Insurance Plans table schema.
"""

import io
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional

logger = logging.getLogger(__name__)

# Schema for insurance plan extraction
INSURANCE_EXTRACTION_SCHEMA = {
    "plan_name": "The official name of the insurance plan",
    "plan_type": "Type: Medical, Life, Critical Illness, or Accident",
    "coverage_type": "Coverage type: Individual, Family, or Group",
    "annual_limit": "Maximum annual coverage limit in RM (number or null)",
    "lifetime_limit": "Maximum lifetime coverage limit in RM (number or null)",
    "room_board_limit": "Daily room and board limit in RM (number or null)",
    "outpatient_covered": "Whether outpatient treatment is covered (true/false)",
    "maternity_covered": "Whether maternity benefits are covered (true/false)",
    "dental_covered": "Whether dental treatment is covered (true/false)",
    "optical_covered": "Whether optical/vision care is covered (true/false)",
    "mental_health_covered": "Whether mental health treatment is covered (true/false)",
    "covered_conditions": "List of covered conditions/benefits",
    "excluded_conditions": "List of excluded conditions/exclusions",
    "monthly_premium_min": "Minimum monthly premium in RM (number or null)",
    "monthly_premium_max": "Maximum monthly premium in RM (number or null)",
    "deductible": "Deductible amount in RM (number or null)",
    "co_payment_percentage": "Co-payment percentage (number 0-100 or null)",
    "min_age": "Minimum entry age in years (integer)",
    "max_age": "Maximum entry age in years (integer)",
    "claim_process": "Description of the claim process",
}

EXTRACTION_PROMPT = """You are an expert insurance analyst. Analyze the following insurance product brochure/document and extract structured information.

IMPORTANT INSTRUCTIONS:
1. Extract ONLY information that is explicitly stated in the document
2. If information is not found, use null for numbers/strings or false for booleans
3. For monetary values, extract the number without currency symbols (e.g., 1000000 not "RM 1,000,000")
4. For coverage limits, look for terms like "annual limit", "yearly limit", "lifetime limit", "overall limit"
5. For room & board, look for "daily room" or "room and board" limits
6. Ages should be integers only
7. Lists should contain specific items mentioned in the document

Return a valid JSON object with the following structure:
{{
    "plan_name": "string - official plan name",
    "plan_type": "string - Medical|Life|Critical Illness|Accident",
    "coverage_type": "string - Individual|Family|Group",
    "annual_limit": number or null,
    "lifetime_limit": number or null,
    "room_board_limit": number or null,
    "outpatient_covered": boolean,
    "maternity_covered": boolean,
    "dental_covered": boolean,
    "optical_covered": boolean,
    "mental_health_covered": boolean,
    "covered_conditions": ["list", "of", "covered", "conditions"],
    "excluded_conditions": ["list", "of", "exclusions"],
    "monthly_premium_min": number or null,
    "monthly_premium_max": number or null,
    "deductible": number or null,
    "co_payment_percentage": number (0-100) or null,
    "min_age": integer or null,
    "max_age": integer or null,
    "claim_process": "string description or null"
}}

DOCUMENT CONTENT:
---
{document_content}
---

PAGE CONTEXT (from website):
- Plan Name: {page_plan_name}
- Description: {page_description}
- Eligible Age: {page_eligible_age}
- Provider: {provider_name}

Extract and return ONLY the JSON object, no additional text or explanation."""


class InsurancePDFAnalyzer:
    """Analyzes insurance PDF brochures using OpenAI GPT-4o."""

    def __init__(self, api_key: str = None, model: str = "gpt-4o"):
        """
        Initialize the analyzer.
        
        Args:
            api_key: OpenAI API key. If None, uses OPENAI_API_KEY env var.
            model: OpenAI model to use (default: gpt-4o).
        """
        self.api_key = api_key or os.environ.get("OPENAI_API_KEY")
        if not self.api_key:
            raise ValueError("OpenAI API key must be provided or set in OPENAI_API_KEY env var")
        
        self.model = model
        self._client = None

    @property
    def client(self):
        """Lazy load OpenAI client."""
        if self._client is None:
            from openai import OpenAI
            self._client = OpenAI(api_key=self.api_key)
        return self._client

    def extract_text_from_pdf(self, pdf_bytes: bytes) -> str:
        """
        Extract text content from PDF bytes.
        
        Args:
            pdf_bytes: PDF file content as bytes.
            
        Returns:
            Extracted text content.
        """
        text_parts = []
        
        # Try pdfplumber first (better for complex layouts)
        try:
            import pdfplumber
            with io.BytesIO(pdf_bytes) as pdf_file:
                with pdfplumber.open(pdf_file) as pdf:
                    for page in pdf.pages:
                        text = page.extract_text()
                        if text:
                            text_parts.append(text)
            
            if text_parts:
                logger.info(f"Extracted {len(text_parts)} pages using pdfplumber")
                return "\n\n".join(text_parts)
        except Exception as e:
            logger.warning(f"pdfplumber failed: {e}, trying PyPDF2")
        
        # Fallback to PyPDF2
        try:
            from PyPDF2 import PdfReader
            with io.BytesIO(pdf_bytes) as pdf_file:
                reader = PdfReader(pdf_file)
                for page in reader.pages:
                    text = page.extract_text()
                    if text:
                        text_parts.append(text)
            
            if text_parts:
                logger.info(f"Extracted {len(text_parts)} pages using PyPDF2")
                return "\n\n".join(text_parts)
        except Exception as e:
            logger.error(f"PyPDF2 also failed: {e}")
        
        return ""

    def analyze_with_llm(
        self,
        document_content: str,
        page_context: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Analyze document content using GPT-4o.
        
        Args:
            document_content: Text content from PDF.
            page_context: Additional context from the web page.
            
        Returns:
            Extracted insurance plan data as dict.
        """
        if not document_content or len(document_content.strip()) < 100:
            logger.warning("Document content too short or empty")
            return {}
        
        page_context = page_context or {}
        
        # Truncate if too long (GPT-4o has 128k context, but keep reasonable)
        max_chars = 50000
        if len(document_content) > max_chars:
            document_content = document_content[:max_chars] + "\n\n[... content truncated ...]"
        
        prompt = EXTRACTION_PROMPT.format(
            document_content=document_content,
            page_plan_name=page_context.get("plan_name", "Unknown"),
            page_description=page_context.get("description", "Not available"),
            page_eligible_age=page_context.get("eligible_age", "Not specified"),
            provider_name=page_context.get("provider_name", "Unknown"),
        )
        
        try:
            logger.info(f"Sending {len(document_content)} chars to GPT-4o for analysis")
            
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert insurance analyst. Extract structured data from insurance documents. Always respond with valid JSON only."
                    },
                    {
                        "role": "user",
                        "content": prompt
                    }
                ],
                temperature=0.1,  # Low temperature for consistent extraction
                max_tokens=4000,
                response_format={"type": "json_object"}
            )
            
            result_text = response.choices[0].message.content
            logger.info(f"Received response from GPT-4o ({len(result_text)} chars)")
            
            # Parse JSON response
            result = json.loads(result_text)
            return self._normalize_result(result)
            
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse LLM response as JSON: {e}")
            return {}
        except Exception as e:
            logger.error(f"LLM analysis failed: {e}")
            return {}

    def _normalize_result(self, result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize and validate the LLM extraction result.
        
        Args:
            result: Raw result from LLM.
            
        Returns:
            Normalized result matching database schema.
        """
        normalized = {}
        
        # String fields
        for field in ["plan_name", "plan_type", "coverage_type", "claim_process"]:
            value = result.get(field)
            normalized[field] = str(value) if value else None
        
        # Numeric fields (float)
        for field in ["annual_limit", "lifetime_limit", "room_board_limit",
                      "monthly_premium_min", "monthly_premium_max", "deductible"]:
            value = result.get(field)
            if value is not None:
                try:
                    # Handle string values like "1,000,000" or "RM 500"
                    if isinstance(value, str):
                        value = re.sub(r"[^\d.]", "", value)
                    normalized[field] = float(value) if value else None
                except (ValueError, TypeError):
                    normalized[field] = None
            else:
                normalized[field] = None
        
        # Percentage field
        co_pay = result.get("co_payment_percentage")
        if co_pay is not None:
            try:
                if isinstance(co_pay, str):
                    co_pay = re.sub(r"[^\d.]", "", co_pay)
                normalized["co_payment_percentage"] = float(co_pay) if co_pay else None
            except (ValueError, TypeError):
                normalized["co_payment_percentage"] = None
        else:
            normalized["co_payment_percentage"] = None
        
        # Integer fields (age)
        for field in ["min_age", "max_age"]:
            value = result.get(field)
            if value is not None:
                try:
                    if isinstance(value, str):
                        # Extract first number from string
                        match = re.search(r"\d+", value)
                        value = int(match.group()) if match else None
                    else:
                        value = int(value)
                    normalized[field] = value
                except (ValueError, TypeError):
                    normalized[field] = None
            else:
                normalized[field] = None
        
        # Boolean fields
        for field in ["outpatient_covered", "maternity_covered", "dental_covered",
                      "optical_covered", "mental_health_covered"]:
            value = result.get(field)
            if value is not None:
                if isinstance(value, bool):
                    normalized[field] = value
                elif isinstance(value, str):
                    normalized[field] = value.lower() in ("true", "yes", "1")
                else:
                    normalized[field] = bool(value)
            else:
                normalized[field] = None
        
        # List fields (JSON arrays)
        for field in ["covered_conditions", "excluded_conditions"]:
            value = result.get(field)
            if value is not None and isinstance(value, list):
                # Filter out empty strings and ensure all items are strings
                normalized[field] = [str(item) for item in value if item]
            else:
                normalized[field] = None
        
        return normalized

    def analyze_pdf(
        self,
        pdf_bytes: bytes,
        page_context: Dict[str, str] = None
    ) -> Dict[str, Any]:
        """
        Full pipeline: extract PDF text and analyze with LLM.
        
        Args:
            pdf_bytes: PDF file content as bytes.
            page_context: Additional context from the web page.
            
        Returns:
            Extracted insurance plan data.
        """
        # Extract text from PDF
        text_content = self.extract_text_from_pdf(pdf_bytes)
        
        if not text_content:
            logger.warning("No text extracted from PDF")
            return {}
        
        # Analyze with LLM
        return self.analyze_with_llm(text_content, page_context)


def analyze_insurance_pdf(
    pdf_bytes: bytes,
    page_context: Dict[str, str] = None,
    api_key: str = None
) -> Dict[str, Any]:
    """
    Convenience function to analyze an insurance PDF.
    
    Args:
        pdf_bytes: PDF file content as bytes.
        page_context: Additional context from the web page.
        api_key: OpenAI API key (optional, uses env var if not provided).
        
    Returns:
        Extracted insurance plan data.
    """
    analyzer = InsurancePDFAnalyzer(api_key=api_key)
    return analyzer.analyze_pdf(pdf_bytes, page_context)

