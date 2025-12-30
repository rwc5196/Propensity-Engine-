"""
📍 PIPELINE 7: LOCAL LABOR MARKET (MARKET TIGHTNESS SIGNAL)
===========================================================
Tracks local unemployment rates as indicator of labor supply.

WHY THIS MATTERS:
- Low unemployment = tight labor market = companies MUST use agencies
- Can't self-recruit when everyone is already employed
- Validates premium pricing (scarcity = higher fees)
- Hyper-local data (county level) is key

DATA SOURCE: BLS LAUS API (free with key, higher limits)
"""

import os
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import json

import requests
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings
from database.connection import db


class LaborMarketPipeline:
    """
    Pipeline for local area unemployment statistics.
    
    Uses BLS LAUS (Local Area Unemployment Statistics) data.
    
    Key Insight: Staffing markets are HYPER-LOCAL
    - National rate (e.g., 3.8%) is meaningless
    - County rate determines your pricing power
    - Sub-3% = "extreme tightness" = premium pricing
    
    Usage:
        pipeline = LaborMarketPipeline()
        
        # Get unemployment for a zip code
        result = pipeline.get_market_data("75001")  # Dallas zip
        
        # Process target areas
        results = pipeline.run()
    """
    
    # BLS API endpoints
    BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    
    # DFW-area FIPS codes (Texas counties)
    DFW_COUNTIES = {
        "Dallas": {"fips": "48113", "state": "TX"},
        "Tarrant": {"fips": "48439", "state": "TX"},
        "Collin": {"fips": "48085", "state": "TX"},
        "Denton": {"fips": "48121", "state": "TX"},
        "Ellis": {"fips": "48139", "state": "TX"},
        "Johnson": {"fips": "48251", "state": "TX"},
        "Kaufman": {"fips": "48257", "state": "TX"},
        "Parker": {"fips": "48367", "state": "TX"},
        "Rockwall": {"fips": "48397", "state": "TX"},
        "Wise": {"fips": "48497", "state": "TX"},
    }
    
    # ZIP to County mapping (simplified - for production use uszipcode library)
    ZIP_TO_COUNTY = {
        # Dallas County
        "75201": "Dallas", "75202": "Dallas", "75203": "Dallas",
        "75204": "Dallas", "75205": "Dallas", "75206": "Dallas",
        "75207": "Dallas", "75208": "Dallas", "75209": "Dallas",
        "75210": "Dallas", "75211": "Dallas", "75212": "Dallas",
        "75214": "Dallas", "75215": "Dallas", "75216": "Dallas",
        "75217": "Dallas", "75218": "Dallas", "75219": "Dallas",
        "75220": "Dallas", "75223": "Dallas", "75224": "Dallas",
        "75001": "Dallas", "75006": "Dallas", "75019": "Dallas",
        "75038": "Dallas", "75039": "Dallas", "75040": "Dallas",
        "75041": "Dallas", "75042": "Dallas", "75043": "Dallas",
        "75050": "Dallas", "75060": "Dallas", "75061": "Dallas",
        "75062": "Dallas", "75063": "Dallas",
        # Tarrant County
        "76001": "Tarrant", "76002": "Tarrant", "76006": "Tarrant",
        "76010": "Tarrant", "76011": "Tarrant", "76012": "Tarrant",
        "76013": "Tarrant", "76014": "Tarrant", "76015": "Tarrant",
        "76016": "Tarrant", "76017": "Tarrant", "76018": "Tarrant",
        "76102": "Tarrant", "76103": "Tarrant", "76104": "Tarrant",
        "76105": "Tarrant", "76106": "Tarrant", "76107": "Tarrant",
        "76108": "Tarrant", "76109": "Tarrant", "76110": "Tarrant",
        "76111": "Tarrant", "76112": "Tarrant", "76114": "Tarrant",
        "76115": "Tarrant", "76116": "Tarrant", "76117": "Tarrant",
        "76118": "Tarrant", "76119": "Tarrant", "76120": "Tarrant",
        # Collin County
        "75002": "Collin", "75009": "Collin", "75013": "Collin",
        "75023": "Collin", "75024": "Collin", "75025": "Collin",
        "75034": "Collin", "75035": "Collin", "75069": "Collin",
        "75070": "Collin", "75071": "Collin", "75074": "Collin",
        "75075": "Collin", "75080": "Collin", "75081": "Collin",
        "75082": "Collin", "75093": "Collin", "75094": "Collin",
        # Denton County
        "75007": "Denton", "75010": "Denton", "75022": "Denton",
        "75028": "Denton", "75056": "Denton", "75057": "Denton",
        "75067": "Denton", "75068": "Denton", "75077": "Denton",
        "76201": "Denton", "76202": "Denton", "76203": "Denton",
        "76205": "Denton", "76207": "Denton", "76208": "Denton",
        "76209": "Denton", "76210": "Denton",
    }
    
    def __init__(self):
        """Initialize the pipeline."""
        self.api_key = settings.api.bls_api_key
        
        if self.api_key:
            logger.info("✅ BLS API key configured (higher rate limits)")
        else:
            logger.warning("⚠️ BLS_API_KEY not set - using public API (limited)")
        
        try:
            from addfips import AddFIPS
            self.fips_lookup = AddFIPS()
            logger.info("✅ AddFIPS library loaded")
        except ImportError:
            logger.warning("addfips not installed - using hardcoded mappings")
            self.fips_lookup = None
    
    def run(self) -> Dict[str, Dict]:
        """
        Run the pipeline for all DFW counties.
        
        Returns:
            Dict mapping county name to unemployment data
        """
        logger.info("=" * 50)
        logger.info("📍 STARTING PIPELINE 7: LOCAL LABOR MARKET")
        logger.info("=" * 50)
        
        results = {}
        
        for county_name, info in self.DFW_COUNTIES.items():
            logger.info(f"\n📊 Processing: {county_name} County")
            
            try:
                data = self._fetch_county_data(info["fips"])
                
                if data and "error" not in data:
                    data["county"] = county_name
                    data["state"] = info["state"]
                    results[county_name] = data
                    
                    logger.info(f"   ✅ Unemployment: {data.get('unemployment_rate', 'N/A')}%")
                    logger.info(f"   📈 Market: {data.get('market_condition', 'N/A')}")
                else:
                    error = data.get("error", "No data") if data else "No data"
                    logger.warning(f"   ⚠️ {error}")
                    
            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                continue
        
        logger.info(f"\n📈 Processed {len(results)}/{len(self.DFW_COUNTIES)} counties")
        return results
    
    def get_market_data(self, zip_code: str) -> Dict:
        """
        Get labor market data for a specific zip code.
        
        Args:
            zip_code: 5-digit zip code
            
        Returns:
            Dict with unemployment data
        """
        # Map zip to county
        county = self.ZIP_TO_COUNTY.get(zip_code)
        
        if not county:
            # Try to resolve using addfips
            county = self._resolve_zip(zip_code)
        
        if not county:
            return {
                "zip_code": zip_code,
                "error": "Could not map zip to county"
            }
        
        # Get county info
        county_info = self.DFW_COUNTIES.get(county)
        
        if not county_info:
            return {
                "zip_code": zip_code,
                "county": county,
                "error": "County not in DFW area"
            }
        
        # Fetch data
        result = self._fetch_county_data(county_info["fips"])
        
        if result:
            result["zip_code"] = zip_code
            result["county"] = county
        
        return result
    
    def _resolve_zip(self, zip_code: str) -> Optional[str]:
        """Resolve zip code to county name."""
        if not self.fips_lookup:
            return None
        
        try:
            # This would need the uszipcode library for full mapping
            # For now, just return None for unknown zips
            return None
        except Exception:
            return None
    
    def _fetch_county_data(self, fips_code: str) -> Dict:
        """
        Fetch unemployment data for a county from BLS.
        
        Args:
            fips_code: 5-digit FIPS code
            
        Returns:
            Dict with unemployment data
        """
        # Build BLS series ID
        # Format: LAUCN + FIPS + 0000000003 (unemployment rate)
        series_id = f"LAUCN{fips_code}0000000003"
        
        # Get current year
        current_year = str(datetime.now().year)
        
        # Build request
        payload = {
            "seriesid": [series_id],
            "startyear": str(int(current_year) - 1),
            "endyear": current_year
        }
        
        if self.api_key:
            payload["registrationkey"] = self.api_key
        
        try:
            response = requests.post(
                self.BLS_API_URL,
                json=payload,
                headers={"Content-Type": "application/json"},
                timeout=15
            )
            
            response.raise_for_status()
            data = response.json()
            
            if data.get("status") != "REQUEST_SUCCEEDED":
                messages = data.get("message", ["Unknown error"])
                return {"error": "; ".join(messages)}
            
            # Parse response
            series_data = data.get("Results", {}).get("series", [])
            
            if not series_data or not series_data[0].get("data"):
                return {"error": "No data in response"}
            
            # Get latest observation
            observations = series_data[0]["data"]
            latest = observations[0]  # Most recent first
            
            rate = float(latest["value"])
            
            return {
                "fips_code": fips_code,
                "series_id": series_id,
                "period": f"{latest['periodName']} {latest['year']}",
                "unemployment_rate": rate,
                "market_condition": self._classify_market(rate),
                "score": self.calculate_tightness_score(rate)
            }
            
        except requests.exceptions.RequestException as e:
            return {"error": f"API request failed: {str(e)}"}
        except (KeyError, IndexError, ValueError) as e:
            return {"error": f"Data parsing error: {str(e)}"}
    
    @staticmethod
    def _classify_market(rate: float) -> str:
        """
        Classify labor market tightness.
        
        Benchmarks (US average ~4%):
        - < 3%: Extreme tightness (very hard to hire)
        - 3-4%: Tight (frictional unemployment)
        - 4-5%: Normal
        - 5-6%: Loose (worker surplus)
        - > 6%: Very loose (high unemployment)
        """
        if rate < 3.0:
            return "EXTREME_TIGHTNESS"
        elif rate < 4.0:
            return "TIGHT"
        elif rate < 5.0:
            return "NORMAL"
        elif rate < 6.0:
            return "LOOSE"
        else:
            return "VERY_LOOSE"
    
    def calculate_tightness_score(self, unemployment_rate: float) -> float:
        """
        Convert unemployment rate to propensity score.
        
        INVERSE relationship: Lower unemployment = higher score
        
        Logic:
        - 2% unemployment → 100 (extreme demand for agencies)
        - 4% unemployment → 50 (average)
        - 6%+ unemployment → 0 (surplus of workers)
        
        Args:
            unemployment_rate: County unemployment rate (%)
            
        Returns:
            Score from 0-100
        """
        if unemployment_rate <= 0:
            return 100.0
        
        # Inverse linear: 2% → 100, 6% → 0
        # score = (6 - rate) / 4 * 100
        score = (6 - unemployment_rate) / 4 * 100
        
        return min(max(score, 0), 100)
    
    def get_regional_summary(self) -> Dict:
        """
        Get summary statistics for the DFW region.
        
        Returns:
            Dict with regional labor market summary
        """
        results = self.run()
        
        if not results:
            return {"error": "No data available"}
        
        rates = [r["unemployment_rate"] for r in results.values() 
                 if "unemployment_rate" in r]
        
        if not rates:
            return {"error": "No rate data"}
        
        avg_rate = sum(rates) / len(rates)
        min_rate = min(rates)
        max_rate = max(rates)
        
        return {
            "region": "DFW Metroplex",
            "counties_analyzed": len(results),
            "average_unemployment": round(avg_rate, 2),
            "lowest_unemployment": round(min_rate, 2),
            "highest_unemployment": round(max_rate, 2),
            "overall_condition": self._classify_market(avg_rate),
            "regional_score": self.calculate_tightness_score(avg_rate)
        }


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
    
    # Run pipeline
    pipeline = LaborMarketPipeline()
    results = pipeline.run()
    
    # Display results
    print("\n" + "=" * 60)
    print("📍 DFW LABOR MARKET ANALYSIS")
    print("=" * 60)
    
    for county, data in results.items():
        if "unemployment_rate" in data:
            rate = data["unemployment_rate"]
            condition = data["market_condition"]
            score = data["score"]
            
            emoji = "🔴" if score > 75 else "🟡" if score > 50 else "🟢"
            print(f"\n{emoji} {county} County:")
            print(f"   Unemployment: {rate}%")
            print(f"   Condition: {condition}")
            print(f"   Score: {score:.1f}")
    
    # Regional summary
    summary = pipeline.get_regional_summary()
    
    print("\n" + "=" * 60)
    print("📊 REGIONAL SUMMARY")
    print("=" * 60)
    print(f"\nDFW Metroplex:")
    print(f"   Average unemployment: {summary.get('average_unemployment', 'N/A')}%")
    print(f"   Overall condition: {summary.get('overall_condition', 'N/A')}")
    print(f"   Regional score: {summary.get('regional_score', 'N/A'):.1f}")
    
    # Scoring examples
    print("\n📈 TIGHTNESS SCORE EXAMPLES:")
    for rate in [2.0, 3.0, 4.0, 5.0, 6.0, 7.0]:
        score = pipeline.calculate_tightness_score(rate)
        print(f"   {rate}% unemployment → Score: {score:.1f}")


if __name__ == "__main__":
    main()
