"""
⭐ PIPELINE 4: GLASSDOOR SENTIMENT (CHURN SIGNAL)
=================================================
Tracks employee sentiment as a leading indicator of turnover.

WHY THIS MATTERS:
- Declining ratings predict mass resignations
- Unhappy employees = high turnover = staffing demand
- "Management" and "Culture" scores are strongest predictors
- 2-3 month lead time before turnover spikes

DATA SOURCE: Glassdoor public data (rate-limited scraping)
NOTE: This is the budget-friendly version. For production scale,
      consider ZenRows ($29/mo) or ScraperAPI ($49/mo).
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import time
import json
import re

import requests
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import retry, stop_after_attempt, wait_exponential

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings
from database.connection import db


class GlassdoorPipeline:
    """
    Pipeline for collecting Glassdoor sentiment data.
    
    Budget-Friendly Approach:
    1. Uses standard requests with delays (respects rate limits)
    2. Parses JSON-LD structured data (stable, SEO-focused)
    3. Caches results to minimize requests
    4. Falls back to estimated scores when blocked
    
    Usage:
        pipeline = GlassdoorPipeline()
        
        # Get sentiment for a specific company
        result = pipeline.get_company_sentiment("Tesla")
        
        # Process multiple companies
        results = pipeline.run(["Amazon", "UPS", "FedEx"])
    """
    
    # Glassdoor blocks aggressive scraping, so we rate limit ourselves
    REQUEST_DELAY = 3.0  # seconds between requests
    
    def __init__(self):
        """Initialize the pipeline."""
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
        })
        
        self.last_request_time = 0
        self._cache = {}
        
        logger.info("GlassdoorPipeline initialized (budget mode)")
        logger.info(f"  - Request delay: {self.REQUEST_DELAY}s between requests")
    
    def run(self, company_names: List[str]) -> List[Dict]:
        """
        Process multiple companies.
        
        Args:
            company_names: List of company names to look up
            
        Returns:
            List of sentiment results
        """
        logger.info("=" * 50)
        logger.info("⭐ STARTING PIPELINE 4: GLASSDOOR SENTIMENT")
        logger.info("=" * 50)
        
        results = []
        
        for name in company_names:
            logger.info(f"\n📊 Processing: {name}")
            
            try:
                sentiment = self.get_company_sentiment(name)
                
                if sentiment and "error" not in sentiment:
                    logger.info(f"   ✅ Rating: {sentiment.get('overall_rating', 'N/A')}")
                    results.append(sentiment)
                else:
                    logger.warning(f"   ⚠️ Could not retrieve sentiment")
                    # Use estimated score based on industry averages
                    estimated = self._estimate_sentiment(name)
                    results.append(estimated)
                    
            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                continue
        
        logger.info(f"\n📈 Processed {len(results)} companies")
        return results
    
    def get_company_sentiment(self, company_name: str) -> Optional[Dict]:
        """
        Get Glassdoor sentiment for a company.
        
        Args:
            company_name: Company name to search
            
        Returns:
            Dict with rating data or None
        """
        # Check cache first
        cache_key = company_name.lower().strip()
        if cache_key in self._cache:
            logger.debug(f"Cache hit for {company_name}")
            return self._cache[cache_key]
        
        # Search for the company
        glassdoor_url = self._search_glassdoor(company_name)
        
        if not glassdoor_url:
            return None
        
        # Fetch and parse the page
        sentiment = self._fetch_sentiment(glassdoor_url)
        
        if sentiment:
            sentiment["company_name"] = company_name
            sentiment["glassdoor_url"] = glassdoor_url
            self._cache[cache_key] = sentiment
            
            # Save to database
            self._save_sentiment(sentiment)
        
        return sentiment
    
    def _rate_limit(self):
        """Enforce rate limiting between requests."""
        elapsed = time.time() - self.last_request_time
        if elapsed < self.REQUEST_DELAY:
            sleep_time = self.REQUEST_DELAY - elapsed
            logger.debug(f"Rate limiting: sleeping {sleep_time:.1f}s")
            time.sleep(sleep_time)
        self.last_request_time = time.time()
    
    def _search_glassdoor(self, company_name: str) -> Optional[str]:
        """
        Search for a company on Glassdoor.
        
        Returns the reviews page URL or None.
        """
        self._rate_limit()
        
        # Use Google to find the Glassdoor page (more reliable than Glassdoor search)
        search_query = f"site:glassdoor.com/Reviews {company_name} reviews"
        
        try:
            # Try DuckDuckGo HTML search (no API key needed)
            ddg_url = f"https://html.duckduckgo.com/html/?q={requests.utils.quote(search_query)}"
            
            response = self.session.get(ddg_url, timeout=10)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find result links
                for link in soup.find_all('a', class_='result__a'):
                    href = link.get('href', '')
                    if 'glassdoor.com/Reviews' in href and '-Reviews-' in href:
                        # Clean up the URL
                        if href.startswith('//'):
                            href = 'https:' + href
                        return href
            
        except Exception as e:
            logger.debug(f"DuckDuckGo search failed: {e}")
        
        # Fallback: construct likely URL
        slug = self._create_slug(company_name)
        return f"https://www.glassdoor.com/Reviews/{slug}-Reviews-E0.htm"
    
    @staticmethod
    def _create_slug(name: str) -> str:
        """Create a URL slug from company name."""
        # Remove common suffixes
        clean = re.sub(r'\s+(LLC|Inc|Corp|Co|Ltd)\.?$', '', name, flags=re.IGNORECASE)
        # Convert to slug format
        slug = re.sub(r'[^a-zA-Z0-9]+', '-', clean.strip())
        return slug.strip('-')
    
    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=5))
    def _fetch_sentiment(self, url: str) -> Optional[Dict]:
        """
        Fetch and parse Glassdoor page for sentiment data.
        
        Args:
            url: Glassdoor reviews page URL
            
        Returns:
            Dict with sentiment data
        """
        self._rate_limit()
        
        try:
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 403:
                logger.warning("Glassdoor blocking requests (403)")
                return None
            
            if response.status_code != 200:
                logger.warning(f"HTTP {response.status_code} from Glassdoor")
                return None
            
            return self._parse_page(response.text)
            
        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed: {e}")
            return None
    
    def _parse_page(self, html: str) -> Optional[Dict]:
        """
        Parse Glassdoor HTML for rating data.
        
        Uses JSON-LD structured data which is more stable than DOM parsing.
        """
        soup = BeautifulSoup(html, 'html.parser')
        
        # Method 1: Try JSON-LD (most reliable)
        for script in soup.find_all('script', type='application/ld+json'):
            try:
                data = json.loads(script.string)
                
                # Look for Employer or Organization schema
                if isinstance(data, dict):
                    if data.get('@type') in ['Employer', 'Organization']:
                        if 'aggregateRating' in data:
                            rating_data = data['aggregateRating']
                            return {
                                "overall_rating": float(rating_data.get('ratingValue', 0)),
                                "review_count": int(rating_data.get('reviewCount', 0)),
                                "best_rating": float(rating_data.get('bestRating', 5)),
                                "worst_rating": float(rating_data.get('worstRating', 1)),
                                "source": "json-ld"
                            }
                    
                    # Sometimes nested in @graph
                    if '@graph' in data:
                        for item in data['@graph']:
                            if item.get('@type') in ['Employer', 'Organization']:
                                if 'aggregateRating' in item:
                                    rating_data = item['aggregateRating']
                                    return {
                                        "overall_rating": float(rating_data.get('ratingValue', 0)),
                                        "review_count": int(rating_data.get('reviewCount', 0)),
                                        "source": "json-ld"
                                    }
                                    
            except (json.JSONDecodeError, TypeError, ValueError):
                continue
        
        # Method 2: Try parsing visible rating (fallback)
        try:
            # Look for rating in page content
            rating_match = re.search(r'(\d\.\d)\s*out of\s*5', html)
            if rating_match:
                return {
                    "overall_rating": float(rating_match.group(1)),
                    "review_count": 0,  # Unknown
                    "source": "regex"
                }
        except Exception:
            pass
        
        return None
    
    def _estimate_sentiment(self, company_name: str) -> Dict:
        """
        Estimate sentiment when we can't scrape.
        
        Uses industry averages as baseline.
        """
        # Industry average is about 3.4 on Glassdoor
        return {
            "company_name": company_name,
            "overall_rating": 3.4,
            "review_count": 0,
            "source": "estimated",
            "note": "Unable to scrape - using industry average"
        }
    
    def _save_sentiment(self, sentiment: Dict):
        """Save sentiment to database via company's signal history."""
        try:
            # Find or create the company
            company = db.get_or_create_company(
                company_name=sentiment.get("company_name", "Unknown"),
                zip_code="00000",  # Unknown - will be enriched later
                glassdoor_url=sentiment.get("glassdoor_url")
            )
            
            if company:
                # Update signal history
                db.save_signal_history(
                    company_id=company["id"],
                    signals={
                        "glassdoor_rating": sentiment.get("overall_rating"),
                        "glassdoor_review_count": sentiment.get("review_count"),
                        "record_date": datetime.now().date().isoformat()
                    }
                )
                
        except Exception as e:
            logger.debug(f"Error saving sentiment: {e}")
    
    def calculate_sentiment_score(self, rating: float) -> float:
        """
        Convert Glassdoor rating to propensity score.
        
        INVERSE relationship: Lower rating = higher staffing need
        
        Logic:
        - 5.0 rating → 0 (no turnover expected)
        - 3.5 rating → 50 (average)
        - 2.0 rating → 100 (high turnover expected)
        
        Args:
            rating: Glassdoor rating (1-5)
            
        Returns:
            Score from 0-100
        """
        if not rating or rating < 1:
            return 50  # Default to average
        
        # Inverse linear scaling
        # rating 5 → score 0
        # rating 1 → score 100
        score = (5 - rating) / 4 * 100
        
        return min(max(score, 0), 100)


# ===========================================
# STANDALONE EXECUTION
# ===========================================

def main():
    """Run the pipeline when executed directly."""
    # Configure logging
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO"
    )
    
    # Test companies (mix of known and unknown)
    test_companies = [
        "Amazon",
        "UPS",
        "FedEx",
        "XPO Logistics",
        "DHL"
    ]
    
    # Run pipeline
    pipeline = GlassdoorPipeline()
    
    print("\n" + "=" * 60)
    print("⭐ GLASSDOOR SENTIMENT ANALYSIS")
    print("=" * 60)
    
    # Test single company
    result = pipeline.get_company_sentiment("Amazon")
    
    if result:
        print(f"\n📊 Amazon Sentiment:")
        print(f"   Rating: {result.get('overall_rating', 'N/A')}/5")
        print(f"   Reviews: {result.get('review_count', 'N/A')}")
        print(f"   Source: {result.get('source', 'unknown')}")
        
        # Calculate score
        score = pipeline.calculate_sentiment_score(result.get("overall_rating", 3.5))
        print(f"   Propensity Score: {score:.1f}")
    else:
        print("\n⚠️ Could not retrieve Amazon sentiment")
    
    # Show scoring examples
    print("\n📈 SENTIMENT SCORE EXAMPLES:")
    for rating in [5.0, 4.0, 3.5, 3.0, 2.5, 2.0]:
        score = pipeline.calculate_sentiment_score(rating)
        print(f"   Rating {rating} → Score: {score:.1f}")


if __name__ == "__main__":
    main()
