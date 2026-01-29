#!/usr/bin/env python3
"""Quick test for Prudential scraping - should only get 3 medical plans"""
import asyncio
import os
import sys

# Add the dags folder to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "airflow_home", "dags"))

from helpers.insurance_scraper import InsuranceScraper

async def test_prudential():
    print("=" * 60)
    print("Testing Prudential - expecting 3 medical plans:")
    print("  1. PRUMillion Med 2.0")
    print("  2. PRUMillion Med Active")
    print("  3. PRUValue Med")
    print("=" * 60)
    
    async with InsuranceScraper(headless=True) as scraper:
        plans = await scraper.scrape_prudential_with_llm()
        
        print(f"\n{'=' * 60}")
        print(f"RESULTS: Scraped {len(plans)} Prudential plans")
        print("=" * 60)
        
        for i, plan in enumerate(plans, 1):
            print(f"{i}. {plan.get('plan_name', 'Unknown')}")
            print(f"   Type: {plan.get('plan_type', 'N/A')}")
            print(f"   Annual Limit: {plan.get('annual_limit', 'N/A')}")
            print()
        
        return plans

if __name__ == "__main__":
    plans = asyncio.run(test_prudential())
    print(f"\nTotal plans: {len(plans)}")
    print("Expected: 3 plans")



