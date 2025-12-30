"""
🏗️ PIPELINE 1: BUILDING PERMITS (CAPEX/EXPANSION SIGNAL)
=========================================================
Collects building permit data from Socrata Open Data portals.

WHY THIS MATTERS:
- A company can't expand workforce without physical expansion
- Permits for "conveyor", "racking", "warehouse" = future labor demand
- 4-6 month lead time before they need workers

DATA SOURCE: Socrata API (free, no key required but recommended)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import re

import pandas as pd
from sodapy import Socrata
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings, SOCRATA_ENDPOINTS, INDUSTRIAL_KEYWORDS
from database.connection import db


class PermitPipeline:
    """
    Pipeline for collecting and processing building permit data.
    
    How it works:
    1. Connects to city open data portals (Socrata)
    2. Queries for permits in target areas
    3. Filters for industrial keywords
    4. Saves to database
    
    Usage:
        pipeline = PermitPipeline()
        results = pipeline.run()
    """
    
    def __init__(self):
        """Initialize the pipeline with configuration."""
        self.app_token = settings.api.socrata_app_token or None
        self.lookback_days = settings.pipeline.permit_lookback_days
        self.min_value = settings.pipeline.min_permit_value
        self.target_zips = set(settings.geography.zips_list)
        
        # Build regex pattern for industrial keywords
        self.industrial_pattern = re.compile(
            "|".join(INDUSTRIAL_KEYWORDS),
            re.IGNORECASE
        )
        
        logger.info(f"PermitPipeline initialized:")
        logger.info(f"  - Lookback: {self.lookback_days} days")
        logger.info(f"  - Min value: ${self.min_value:,}")
        logger.info(f"  - Target zips: {len(self.target_zips)}")
    
    def run(self) -> pd.DataFrame:
        """
        Run the full pipeline for all configured cities.
        
        Returns:
            DataFrame with all collected permits
        """
        logger.info("=" * 50)
        logger.info("🏗️ STARTING PIPELINE 1: BUILDING PERMITS")
        logger.info("=" * 50)
        
        all_permits = []
        
        # Process each city endpoint
        for city_name, config in SOCRATA_ENDPOINTS.items():
            logger.info(f"\n📍 Processing: {city_name}")
            
            try:
                permits = self._fetch_city_permits(city_name, config)
                if not permits.empty:
                    # Filter for industrial permits
                    industrial = self._filter_industrial(permits)
                    logger.info(f"   ✅ Found {len(industrial)} industrial permits")
                    
                    # Save to database
                    self._save_permits(industrial, city_name)
                    
                    all_permits.append(industrial)
                else:
                    logger.info(f"   ⚠️ No permits found")
                    
            except Exception as e:
                logger.error(f"   ❌ Error processing {city_name}: {e}")
                continue
        
        # Combine all results
        if all_permits:
            result = pd.concat(all_permits, ignore_index=True)
            logger.info(f"\n📊 TOTAL: {len(result)} industrial permits collected")
            return result
        
        logger.warning("No permits collected from any city")
        return pd.DataFrame()
    
    def _fetch_city_permits(
        self, 
        city_name: str, 
        config: Dict
    ) -> pd.DataFrame:
        """
        Fetch permits from a single city's Socrata endpoint.
        
        Args:
            city_name: Display name for logging
            config: Endpoint configuration dict
            
        Returns:
            DataFrame of raw permits
        """
        # Initialize Socrata client
        client = Socrata(
            config["domain"],
            self.app_token,
            timeout=30
        )
        
        # Calculate date range
        end_date = datetime.now()
        start_date = end_date - timedelta(days=self.lookback_days)
        
        # Build SoQL query
        # Note: Field names vary by city, so we use the config
        date_field = config["date_field"]
        desc_field = config["description_field"]
        value_field = config["value_field"]
        addr_field = config["address_field"]
        
        # Build WHERE clause for industrial keywords
        keyword_clauses = " OR ".join([
            f"lower({desc_field}) like '%{kw}%'"
            for kw in INDUSTRIAL_KEYWORDS[:10]  # Limit to avoid query length issues
        ])
        
        query = f"""
            SELECT
                {date_field} as issue_date,
                {desc_field} as work_description,
                {value_field} as reported_cost,
                {addr_field} as address,
                *
            WHERE
                {date_field} >= '{start_date.strftime('%Y-%m-%d')}'
                AND {value_field} >= {self.min_value}
                AND ({keyword_clauses})
            ORDER BY {date_field} DESC
            LIMIT 2000
        """
        
        try:
            results = client.get(
                config["dataset_id"],
                query=query
            )
            
            if results:
                df = pd.DataFrame.from_records(results)
                df["source_city"] = city_name
                return df
            
            return pd.DataFrame()
            
        except Exception as e:
            logger.error(f"Socrata query failed for {city_name}: {e}")
            
            # Fallback: simpler query without keyword filter
            try:
                logger.info("   Trying fallback query...")
                simple_query = f"""
                    SELECT *
                    WHERE {date_field} >= '{start_date.strftime('%Y-%m-%d')}'
                      AND {value_field} >= {self.min_value}
                    ORDER BY {date_field} DESC
                    LIMIT 1000
                """
                results = client.get(config["dataset_id"], query=simple_query)
                
                if results:
                    df = pd.DataFrame.from_records(results)
                    df["source_city"] = city_name
                    return df
                    
            except Exception as e2:
                logger.error(f"Fallback query also failed: {e2}")
            
            return pd.DataFrame()
    
    def _filter_industrial(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Filter permits to only include industrial/warehouse projects.
        
        Args:
            df: Raw permit DataFrame
            
        Returns:
            Filtered DataFrame
        """
        if df.empty:
            return df
        
        # Get the description column (may vary by source)
        desc_col = None
        for col in ["work_description", "description", "permit_description"]:
            if col in df.columns:
                desc_col = col
                break
        
        if not desc_col:
            logger.warning("No description column found, returning all permits")
            return df
        
        # Filter using regex pattern
        mask = df[desc_col].fillna("").str.contains(
            self.industrial_pattern, 
            regex=True
        )
        
        filtered = df[mask].copy()
        
        # Add classification column
        filtered["is_industrial"] = True
        
        return filtered
    
    def _save_permits(self, df: pd.DataFrame, city_name: str):
        """
        Save permits to the database.
        
        Args:
            df: Filtered permit DataFrame
            city_name: Source city name
        """
        if df.empty:
            return
        
        saved_count = 0
        
        for _, row in df.iterrows():
            try:
                # Map to our schema
                permit_data = {
                    "source_city": city_name,
                    "source_dataset": SOCRATA_ENDPOINTS.get(city_name, {}).get("dataset_id"),
                    "permit_id": str(row.get("permit_number", row.get("id", ""))),
                    "issue_date": self._parse_date(row.get("issue_date")),
                    "work_description": row.get("work_description", ""),
                    "reported_cost": self._parse_number(row.get("reported_cost")),
                    "address": row.get("address", ""),
                    "contractor_name": row.get("contractor_name", row.get("contractor", "")),
                    "is_industrial": True
                }
                
                db.save_permit(permit_data)
                saved_count += 1
                
            except Exception as e:
                logger.debug(f"Error saving permit: {e}")
                continue
        
        logger.info(f"   💾 Saved {saved_count} permits to database")
    
    @staticmethod
    def _parse_date(value) -> Optional[str]:
        """Parse various date formats to ISO string."""
        if not value:
            return None
        
        if isinstance(value, str):
            # Try to parse ISO format
            for fmt in ["%Y-%m-%dT%H:%M:%S", "%Y-%m-%d", "%m/%d/%Y"]:
                try:
                    dt = datetime.strptime(value[:19], fmt)
                    return dt.date().isoformat()
                except ValueError:
                    continue
        
        return None
    
    @staticmethod
    def _parse_number(value) -> Optional[float]:
        """Parse various number formats."""
        if not value:
            return None
        
        if isinstance(value, (int, float)):
            return float(value)
        
        if isinstance(value, str):
            # Remove currency symbols and commas
            clean = value.replace("$", "").replace(",", "").strip()
            try:
                return float(clean)
            except ValueError:
                return None
        
        return None
    
    def calculate_expansion_score(self, permit_value: float) -> float:
        """
        Calculate the expansion signal score (0-100) based on permit value.
        
        Uses logarithmic scaling:
        - $50,000 → ~47
        - $100,000 → ~50
        - $500,000 → ~57
        - $1,000,000 → ~60
        - $5,000,000 → ~67
        - $10,000,000 → ~70
        
        Args:
            permit_value: Dollar value of the permit
            
        Returns:
            Score from 0-100
        """
        import numpy as np
        
        if not permit_value or permit_value <= 0:
            return 0.0
        
        # Logarithmic scaling: log10(value) / log10(10M) * 100
        # This gives us:
        #   $100K → 50, $1M → 60, $10M → 70
        max_value = 10_000_000  # $10M = "perfect" score
        
        score = (np.log10(permit_value) / np.log10(max_value)) * 100
        
        # Clamp to 0-100
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
    
    # Run pipeline
    pipeline = PermitPipeline()
    results = pipeline.run()
    
    # Show sample results
    if not results.empty:
        print("\n" + "=" * 60)
        print("📋 SAMPLE RESULTS")
        print("=" * 60)
        
        display_cols = ["source_city", "issue_date", "reported_cost", "work_description"]
        available_cols = [c for c in display_cols if c in results.columns]
        
        print(results[available_cols].head(10).to_string(index=False))
        
        # Calculate scores for top permits
        print("\n📊 EXPANSION SCORES:")
        for _, row in results.head(5).iterrows():
            value = row.get("reported_cost", 0)
            if value:
                try:
                    value = float(str(value).replace("$", "").replace(",", ""))
                    score = pipeline.calculate_expansion_score(value)
                    print(f"   ${value:,.0f} → Score: {score:.1f}")
                except:
                    pass


if __name__ == "__main__":
    main()
