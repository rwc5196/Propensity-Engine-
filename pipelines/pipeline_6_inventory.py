"""
📦 PIPELINE 6: INVENTORY TURNOVER (SUPPLY CHAIN SIGNAL)
=======================================================
Extracts inventory metrics from SEC filings for public companies.

WHY THIS MATTERS:
- High inventory turnover = goods moving fast = labor demand
- Rising turnover = need more warehouse workers
- Falling turnover = potential layoffs (contrarian signal)
- Works best for retail, distribution, manufacturing clients

DATA SOURCE: SEC EDGAR (free, official government data)
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import time

import requests
import pandas as pd
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings
from database.connection import db


class InventoryPipeline:
    """
    Pipeline for extracting inventory turnover from SEC EDGAR.
    
    Uses SEC's free XBRL API to get financial data from 10-K/10-Q filings.
    
    Key Metric: Inventory Turnover = COGS / Average Inventory
    - Higher ratio = faster-moving inventory = more labor needed
    - Industry benchmarks: Retail ~10x, Distribution ~8x, Manufacturing ~5x
    
    Usage:
        pipeline = InventoryPipeline()
        
        # Get turnover for a specific company
        result = pipeline.get_turnover("WMT")  # Walmart
        
        # Process multiple tickers
        results = pipeline.run(["WMT", "HD", "COST", "TGT"])
    """
    
    # SEC rate limit: 10 requests per second
    REQUEST_DELAY = 0.15
    
    # XBRL tags for financial data (with fallbacks)
    COGS_TAGS = [
        "CostOfGoodsAndServicesSold",
        "CostOfRevenue",
        "CostOfGoodsSold"
    ]
    
    INVENTORY_TAGS = [
        "InventoryNet",
        "InventoryGross",
        "Inventories"
    ]
    
    def __init__(self):
        """Initialize the pipeline."""
        self.user_agent = settings.api.sec_user_agent
        
        if not self.user_agent or "@" not in self.user_agent:
            logger.warning("⚠️ SEC_USER_AGENT not properly configured")
            logger.info("Set SEC_USER_AGENT='AppName your@email.com' in .env")
            self.user_agent = "PropensityEngine demo@example.com"
        
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": self.user_agent,
            "Accept-Encoding": "gzip, deflate"
        })
        
        self.last_request = 0
        self._ticker_cache = None
        
        logger.info("InventoryPipeline initialized")
        logger.info(f"  - User-Agent: {self.user_agent[:30]}...")
    
    def run(self, tickers: List[str]) -> List[Dict]:
        """
        Process multiple company tickers.
        
        Args:
            tickers: List of stock tickers
            
        Returns:
            List of turnover results
        """
        logger.info("=" * 50)
        logger.info("📦 STARTING PIPELINE 6: INVENTORY TURNOVER")
        logger.info("=" * 50)
        
        results = []
        
        for ticker in tickers:
            logger.info(f"\n📊 Processing: {ticker}")
            
            try:
                turnover = self.get_turnover(ticker)
                
                if turnover and "error" not in turnover:
                    logger.info(f"   ✅ Turnover ratio: {turnover.get('turnover_ratio', 'N/A')}")
                    results.append(turnover)
                else:
                    error = turnover.get("error", "Unknown error") if turnover else "No data"
                    logger.warning(f"   ⚠️ {error}")
                    
            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                continue
        
        logger.info(f"\n📈 Processed {len(results)}/{len(tickers)} companies")
        return results
    
    def _rate_limit(self):
        """Enforce SEC rate limit."""
        elapsed = time.time() - self.last_request
        if elapsed < self.REQUEST_DELAY:
            time.sleep(self.REQUEST_DELAY - elapsed)
        self.last_request = time.time()
    
    def get_cik(self, ticker: str) -> Optional[str]:
        """
        Convert ticker to SEC CIK (Central Index Key).
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            10-digit CIK string or None
        """
        # Load ticker mapping (cached)
        if self._ticker_cache is None:
            self._rate_limit()
            
            try:
                url = "https://www.sec.gov/files/company_tickers.json"
                response = self.session.get(url, timeout=10)
                response.raise_for_status()
                self._ticker_cache = response.json()
            except Exception as e:
                logger.error(f"Failed to load ticker mapping: {e}")
                return None
        
        # Search for ticker
        ticker_upper = ticker.upper()
        for entry in self._ticker_cache.values():
            if entry.get("ticker") == ticker_upper:
                # Zero-pad CIK to 10 digits
                return str(entry["cik_str"]).zfill(10)
        
        return None
    
    def get_turnover(self, ticker: str) -> Dict:
        """
        Calculate inventory turnover for a company.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Dict with turnover data
        """
        # Get CIK
        cik = self.get_cik(ticker)
        if not cik:
            return {"ticker": ticker, "error": "Ticker not found in SEC database"}
        
        # Fetch company facts
        self._rate_limit()
        
        try:
            url = f"https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json"
            response = self.session.get(url, timeout=15)
            
            if response.status_code == 404:
                return {"ticker": ticker, "error": "No XBRL data available"}
            
            response.raise_for_status()
            data = response.json()
            
        except Exception as e:
            return {"ticker": ticker, "error": f"API error: {str(e)}"}
        
        # Extract facts
        try:
            facts = data.get("facts", {}).get("us-gaap", {})
            
            if not facts:
                return {"ticker": ticker, "error": "No US-GAAP facts found"}
            
            # Get COGS
            cogs = self._extract_latest_value(facts, self.COGS_TAGS)
            
            # Get Inventory
            inventory = self._extract_latest_value(facts, self.INVENTORY_TAGS)
            
            if cogs is None:
                return {"ticker": ticker, "error": "COGS data not found"}
            
            if inventory is None or inventory == 0:
                return {"ticker": ticker, "error": "Inventory data not found or zero"}
            
            # Calculate turnover
            turnover_ratio = cogs["value"] / inventory["value"]
            
            result = {
                "ticker": ticker,
                "cik": cik,
                "period_end": cogs.get("period_end"),
                "cogs": cogs["value"],
                "inventory": inventory["value"],
                "turnover_ratio": round(turnover_ratio, 2),
                "form": cogs.get("form", "unknown"),
                "score": self.calculate_turnover_score(turnover_ratio)
            }
            
            # Save to database
            self._save_turnover(result)
            
            return result
            
        except Exception as e:
            return {"ticker": ticker, "error": f"Calculation error: {str(e)}"}
    
    def _extract_latest_value(
        self, 
        facts: Dict, 
        tags: List[str]
    ) -> Optional[Dict]:
        """
        Extract the latest value from XBRL facts using tag fallback.
        
        Args:
            facts: US-GAAP facts dictionary
            tags: List of possible tag names
            
        Returns:
            Dict with value and metadata
        """
        for tag in tags:
            if tag not in facts:
                continue
            
            tag_data = facts[tag]
            
            # Get USD units
            if "units" not in tag_data:
                continue
            
            if "USD" in tag_data["units"]:
                entries = tag_data["units"]["USD"]
            else:
                continue
            
            # Convert to DataFrame for easier filtering
            df = pd.DataFrame(entries)
            
            # Filter to 10-K and 10-Q filings only
            df = df[df["form"].isin(["10-K", "10-Q"])]
            
            if df.empty:
                continue
            
            # Convert dates
            df["end"] = pd.to_datetime(df["end"])
            
            # Get latest entry
            latest = df.sort_values("end").iloc[-1]
            
            return {
                "value": float(latest["val"]),
                "period_end": latest["end"].date().isoformat(),
                "form": latest["form"],
                "tag": tag
            }
        
        return None
    
    def _save_turnover(self, result: Dict):
        """Save turnover data to database."""
        try:
            # Find or create company
            company = db.get_or_create_company(
                company_name=result["ticker"],  # Use ticker as name for now
                zip_code="00000",
                ticker=result["ticker"]
            )
            
            if company:
                db.save_signal_history(
                    company_id=company["id"],
                    signals={
                        "inventory_turnover_ratio": result.get("turnover_ratio"),
                        "turnover_score": result.get("score"),
                        "record_date": datetime.now().date().isoformat()
                    }
                )
                
        except Exception as e:
            logger.debug(f"Error saving turnover: {e}")
    
    def calculate_turnover_score(self, turnover_ratio: float) -> float:
        """
        Convert inventory turnover to propensity score.
        
        Higher turnover = faster-moving goods = more labor needed
        
        Industry benchmarks (annual):
        - Retail: 8-12x (high)
        - Distribution: 6-10x (medium-high)
        - Manufacturing: 4-6x (medium)
        - Industrial: 2-4x (low)
        
        Args:
            turnover_ratio: Annual inventory turnover
            
        Returns:
            Score from 0-100
        """
        if turnover_ratio <= 0:
            return 0.0
        
        # Scale: 0-2x = low, 5x = medium, 10+ = high
        # Using sigmoid-like scaling
        import math
        
        # Normalize to 0-1 range (5x = 0.5, 10x = 0.67, 15x = 0.75)
        normalized = turnover_ratio / (turnover_ratio + 5)
        
        # Scale to 0-100
        score = normalized * 100
        
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
    
    # Test tickers (major DFW-area employers with warehouses)
    test_tickers = [
        "WMT",   # Walmart
        "HD",    # Home Depot
        "TGT",   # Target
        "COST",  # Costco
        "KR",    # Kroger
    ]
    
    # Run pipeline
    pipeline = InventoryPipeline()
    results = pipeline.run(test_tickers)
    
    # Display results
    print("\n" + "=" * 60)
    print("📦 INVENTORY TURNOVER RESULTS")
    print("=" * 60)
    
    for r in results:
        if "error" not in r:
            print(f"\n{r['ticker']}:")
            print(f"   COGS: ${r['cogs']:,.0f}")
            print(f"   Inventory: ${r['inventory']:,.0f}")
            print(f"   Turnover: {r['turnover_ratio']}x")
            print(f"   Score: {r['score']:.1f}")
    
    # Show scoring examples
    print("\n📊 TURNOVER SCORE EXAMPLES:")
    for ratio in [2.0, 5.0, 8.0, 10.0, 15.0]:
        score = pipeline.calculate_turnover_score(ratio)
        print(f"   {ratio}x turnover → Score: {score:.1f}")


if __name__ == "__main__":
    main()
