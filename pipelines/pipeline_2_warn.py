"""
⚠️ PIPELINE 2: WARN NOTICES (DISTRESS SIGNAL)
==============================================
Collects WARN (Worker Adjustment and Retraining Notification) Act data.

WHY THIS MATTERS:
- WARN notices signal 60-day advance warning of layoffs/closures
- Competitor closing = their workers need new jobs (SUPPLY opportunity)
- Competitor closing = their clients need new staffing provider (DEMAND opportunity)
- Companies near closures may absorb the displaced workforce

DATA SOURCE: warn-scraper library (free, scrapes state websites)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import re
import tempfile

import pandas as pd
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings, WARN_STATES
from database.connection import db


class WARNPipeline:
    """
    Pipeline for collecting and processing WARN notice data.
    
    How it works:
    1. Uses warn-scraper to fetch notices from state websites
    2. Filters for industrial/logistics companies
    3. Calculates proximity to target areas
    4. Saves to database
    
    Usage:
        pipeline = WARNPipeline()
        results = pipeline.run()
    """
    
    def __init__(self):
        """Initialize the pipeline."""
        self.target_state = settings.geography.target_state
        self.target_zips = set(settings.geography.zips_list)
        self.lookback_days = 90  # WARN notices are 60 days ahead, look back 90
        
        # Industrial keywords for filtering
        self.industrial_keywords = [
            "logistics", "warehouse", "distribution", "fulfillment",
            "manufacturing", "assembly", "packaging", "freight",
            "trucking", "shipping", "supply chain", "3pl",
            "cold storage", "food processing", "industrial"
        ]
        
        self.industrial_pattern = re.compile(
            "|".join(self.industrial_keywords),
            re.IGNORECASE
        )
        
        logger.info(f"WARNPipeline initialized:")
        logger.info(f"  - Target state: {self.target_state}")
        logger.info(f"  - Lookback: {self.lookback_days} days")
    
    def run(self) -> pd.DataFrame:
        """
        Run the full pipeline.
        
        Returns:
            DataFrame with all collected WARN notices
        """
        logger.info("=" * 50)
        logger.info("⚠️ STARTING PIPELINE 2: WARN NOTICES")
        logger.info("=" * 50)
        
        all_notices = []
        
        # Process each state
        states_to_process = [self.target_state] + [
            s for s in WARN_STATES if s != self.target_state
        ]
        
        for state in states_to_process:
            logger.info(f"\n📍 Processing: {state}")
            
            try:
                notices = self._fetch_state_notices(state)
                
                if notices is not None and not notices.empty:
                    # Filter for industrial
                    industrial = self._filter_industrial(notices)
                    logger.info(f"   ✅ Found {len(industrial)} industrial notices")
                    
                    # Save to database
                    self._save_notices(industrial, state)
                    
                    all_notices.append(industrial)
                else:
                    logger.info(f"   ⚠️ No notices found or scraper unavailable")
                    
            except Exception as e:
                logger.error(f"   ❌ Error processing {state}: {e}")
                continue
        
        # Combine results
        if all_notices:
            result = pd.concat(all_notices, ignore_index=True)
            logger.info(f"\n📊 TOTAL: {len(result)} industrial WARN notices")
            return result
        
        logger.warning("No WARN notices collected")
        return pd.DataFrame()
    
    def _fetch_state_notices(self, state: str) -> Optional[pd.DataFrame]:
        """
        Fetch WARN notices for a specific state.
        
        Args:
            state: Two-letter state code
            
        Returns:
            DataFrame of notices or None
        """
        try:
            # Import warn-scraper
            from warn import scrapers
            
            # Get the scraper for this state
            scraper_name = state.lower()
            
            if not hasattr(scrapers, scraper_name):
                logger.warning(f"No scraper available for {state}")
                return None
            
            scraper = getattr(scrapers, scraper_name)
            
            # Create temp directory for output
            with tempfile.TemporaryDirectory() as temp_dir:
                # Run the scraper
                logger.info(f"   Running {state} scraper...")
                scraper.scrape(temp_dir)
                
                # Read the output CSV
                csv_path = Path(temp_dir) / f"{state.lower()}.csv"
                
                if csv_path.exists():
                    df = pd.read_csv(csv_path)
                    df["source_state"] = state
                    
                    # Filter by date
                    df = self._filter_by_date(df)
                    
                    return df
                else:
                    logger.warning(f"No output file from {state} scraper")
                    return None
                    
        except ImportError:
            logger.error("warn-scraper not installed. Run: pip install warn-scraper")
            return self._fallback_fetch(state)
            
        except Exception as e:
            logger.error(f"Scraper error for {state}: {e}")
            return self._fallback_fetch(state)
    
    def _fallback_fetch(self, state: str) -> Optional[pd.DataFrame]:
        """
        Fallback method if warn-scraper fails.
        Attempts to fetch from cached/known data sources.
        
        Args:
            state: State code
            
        Returns:
            DataFrame or None
        """
        # For Texas, we can try the TWC API directly
        if state == "TX":
            return self._fetch_texas_direct()
        
        return None
    
    def _fetch_texas_direct(self) -> Optional[pd.DataFrame]:
        """
        Fetch Texas WARN notices directly from TWC.
        Texas publishes a relatively clean Excel file.
        """
        import requests
        
        url = "https://www.twc.texas.gov/files/news/warn-act-listings-702.xlsx"
        
        try:
            logger.info("   Trying direct Texas WARN fetch...")
            
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            # Read Excel directly from bytes
            df = pd.read_excel(response.content)
            df["source_state"] = "TX"
            
            # Standardize column names
            df = df.rename(columns={
                "JOB_SITE_NAME": "company_name",
                "NOTICE_DATE": "notice_date",
                "TOTAL_LAYOFF_NUMBER": "affected_count",
                "CITY_NAME": "city",
                "ZIP_CODE": "zip_code"
            })
            
            # Filter by date
            df = self._filter_by_date(df)
            
            logger.info(f"   ✅ Fetched {len(df)} Texas notices directly")
            return df
            
        except Exception as e:
            logger.error(f"Direct Texas fetch failed: {e}")
            return None
    
    def _filter_by_date(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter notices to recent timeframe."""
        if df.empty:
            return df
        
        # Find the date column
        date_col = None
        for col in ["notice_date", "NOTICE_DATE", "date", "Date"]:
            if col in df.columns:
                date_col = col
                break
        
        if not date_col:
            return df
        
        # Convert to datetime
        df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
        
        # Filter to lookback period
        cutoff = datetime.now() - timedelta(days=self.lookback_days)
        mask = df[date_col] >= cutoff
        
        return df[mask].copy()
    
    def _filter_industrial(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter to only industrial/logistics companies.
        
        Args:
            df: Raw WARN notices
            
        Returns:
            Filtered DataFrame
        """
        if df.empty:
            return df
        
        # Find company name column
        name_col = None
        for col in ["company_name", "JOB_SITE_NAME", "EMPLOYER_NAME", "Company"]:
            if col in df.columns:
                name_col = col
                break
        
        if not name_col:
            logger.warning("No company name column found")
            return df
        
        # Filter using pattern
        mask = df[name_col].fillna("").str.contains(
            self.industrial_pattern,
            regex=True
        )
        
        # Also include based on job type if available
        if "layoff_type" in df.columns or "LAYOFF_TYPE" in df.columns:
            type_col = "layoff_type" if "layoff_type" in df.columns else "LAYOFF_TYPE"
            mask = mask | df[type_col].fillna("").str.contains(
                self.industrial_pattern,
                regex=True
            )
        
        filtered = df[mask].copy()
        filtered["is_industrial"] = True
        
        return filtered
    
    def _save_notices(self, df: pd.DataFrame, state: str):
        """Save WARN notices to database."""
        if df.empty:
            return
        
        saved_count = 0
        
        for _, row in df.iterrows():
            try:
                # Map columns (they vary by state)
                notice_data = {
                    "source_state": state,
                    "company_name": str(row.get("company_name", row.get("JOB_SITE_NAME", ""))),
                    "notice_date": self._extract_date(row, ["notice_date", "NOTICE_DATE"]),
                    "effective_date": self._extract_date(row, ["effective_date", "LAYOFF_DATE"]),
                    "affected_count": self._extract_int(row, ["affected_count", "TOTAL_LAYOFF_NUMBER"]),
                    "city": str(row.get("city", row.get("CITY_NAME", ""))),
                    "zip_code": str(row.get("zip_code", row.get("ZIP_CODE", ""))),
                    "is_industrial": True
                }
                
                # Determine layoff type
                notice_data["layoff_type"] = self._classify_layoff_type(row)
                
                db.save_warn_notice(notice_data)
                saved_count += 1
                
            except Exception as e:
                logger.debug(f"Error saving notice: {e}")
                continue
        
        logger.info(f"   💾 Saved {saved_count} notices to database")
    
    @staticmethod
    def _extract_date(row: pd.Series, possible_cols: List[str]) -> Optional[str]:
        """Extract date from various possible columns."""
        for col in possible_cols:
            if col in row.index and pd.notna(row[col]):
                try:
                    dt = pd.to_datetime(row[col])
                    return dt.date().isoformat()
                except:
                    continue
        return None
    
    @staticmethod
    def _extract_int(row: pd.Series, possible_cols: List[str]) -> Optional[int]:
        """Extract integer from various possible columns."""
        for col in possible_cols:
            if col in row.index and pd.notna(row[col]):
                try:
                    return int(float(row[col]))
                except:
                    continue
        return None
    
    @staticmethod
    def _classify_layoff_type(row: pd.Series) -> str:
        """Classify the type of WARN notice."""
        # Check various columns for clues
        text_to_check = " ".join([
            str(row.get(col, ""))
            for col in ["company_name", "description", "layoff_type", "LAYOFF_TYPE"]
            if col in row.index
        ]).lower()
        
        if "clos" in text_to_check:
            return "closure"
        elif "reloc" in text_to_check:
            return "relocation"
        else:
            return "layoff"
    
    def calculate_distress_score(
        self, 
        distance_miles: float, 
        affected_count: int = 50
    ) -> float:
        """
        Calculate distress signal score based on proximity and magnitude.
        
        Closer = higher opportunity (their workers/clients need solutions)
        
        Formula: (1 / (distance + 1)) * magnitude_factor
        
        Args:
            distance_miles: Distance to the WARN location
            affected_count: Number of workers affected
            
        Returns:
            Score from 0-100
        """
        # Distance factor: 0 miles = 1.0, 10 miles = 0.09, 50 miles = 0.02
        distance_factor = 1 / (distance_miles + 1)
        
        # Magnitude factor: scale based on affected workers
        # 50 workers = 1.0, 200 workers = 1.5, 500+ workers = 2.0
        magnitude_factor = min(1 + (affected_count / 200), 2.0)
        
        # Calculate raw score (0-2 range)
        raw_score = distance_factor * magnitude_factor
        
        # Scale to 0-100
        score = raw_score * 50  # Max possible is ~100
        
        return min(max(score, 0), 100)
    
    def get_nearby_warn_notices(
        self, 
        zip_code: str, 
        radius_miles: float = 25
    ) -> List[Dict]:
        """
        Find WARN notices near a given location.
        
        Note: For production, you'd want a proper geospatial query.
        This is a simplified version using zip code matching.
        
        Args:
            zip_code: Center zip code
            radius_miles: Search radius (approximate)
            
        Returns:
            List of nearby WARN notices
        """
        # For MVP, just match first 3 digits of zip (roughly same area)
        prefix = zip_code[:3]
        
        notices = db.query(
            "raw_warn_notices",
            filters={"is_industrial": True},
            order_by="-notice_date",
            limit=100
        )
        
        # Filter by zip prefix
        nearby = [
            n for n in notices
            if n.get("zip_code", "").startswith(prefix)
        ]
        
        return nearby


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
    pipeline = WARNPipeline()
    results = pipeline.run()
    
    # Show sample results
    if not results.empty:
        print("\n" + "=" * 60)
        print("📋 SAMPLE WARN NOTICES")
        print("=" * 60)
        
        # Find displayable columns
        display_cols = []
        for col in ["company_name", "JOB_SITE_NAME", "city", "CITY_NAME", 
                    "notice_date", "NOTICE_DATE", "affected_count", "TOTAL_LAYOFF_NUMBER"]:
            if col in results.columns:
                display_cols.append(col)
        
        print(results[display_cols[:5]].head(10).to_string(index=False))
        
        # Show score examples
        print("\n📊 DISTRESS SCORES (by distance):")
        for distance in [0, 5, 10, 25, 50]:
            score = pipeline.calculate_distress_score(distance, affected_count=100)
            print(f"   {distance} miles away → Score: {score:.1f}")


if __name__ == "__main__":
    main()
