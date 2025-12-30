"""
📈 PIPELINE 3: MACRO ECONOMIC INDICATORS (TIMING SIGNAL)
========================================================
Collects economic data from FRED (Federal Reserve Economic Data).

WHY THIS MATTERS:
- Freight indices predict warehouse labor demand 3-6 months ahead
- Rising inventories = more warehouse workers needed
- Economic expansion = higher hiring across all sectors
- Acts as a "multiplier" on other signals

DATA SOURCE: FRED API (free with key)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings, FRED_SERIES
from database.connection import db


class MacroPipeline:
    """
    Pipeline for collecting and analyzing macro economic indicators.
    
    Key Series:
    - Cass Freight Index: Volume of freight shipments
    - Manufacturing Inventories: Supply chain activity
    - Trucking Employment: Logistics labor demand
    - Warehouse Employment: Direct indicator of warehouse hiring
    
    Usage:
        pipeline = MacroPipeline()
        results = pipeline.run()
        modifier = pipeline.get_macro_modifier()  # Apply to other scores
    """
    
    def __init__(self):
        """Initialize the pipeline."""
        self.api_key = settings.api.fred_api_key
        self.fred = None
        
        if self.api_key:
            try:
                from fredapi import Fred
                self.fred = Fred(api_key=self.api_key)
                logger.info("✅ FRED API initialized")
            except ImportError:
                logger.error("fredapi not installed. Run: pip install fredapi")
        else:
            logger.warning("⚠️ FRED_API_KEY not set - using mock data")
    
    def run(self) -> Dict[str, pd.Series]:
        """
        Run the pipeline to collect all economic indicators.
        
        Returns:
            Dictionary of series name → pandas Series
        """
        logger.info("=" * 50)
        logger.info("📈 STARTING PIPELINE 3: MACRO ECONOMIC DATA")
        logger.info("=" * 50)
        
        results = {}
        
        for name, series_id in FRED_SERIES.items():
            logger.info(f"\n📊 Fetching: {name} ({series_id})")
            
            try:
                data = self._fetch_series(series_id)
                
                if data is not None and len(data) > 0:
                    results[name] = data
                    
                    # Calculate trend
                    trend = self._calculate_trend(data)
                    logger.info(f"   ✅ Got {len(data)} observations")
                    logger.info(f"   📈 Trend: {trend['direction']} ({trend['pct_change']:.1%})")
                    
                    # Save to database
                    self._save_indicator(series_id, name, data, trend)
                else:
                    logger.warning(f"   ⚠️ No data returned")
                    
            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                continue
        
        return results
    
    def _fetch_series(self, series_id: str, periods: int = 24) -> Optional[pd.Series]:
        """
        Fetch a FRED series.
        
        Args:
            series_id: FRED series identifier
            periods: Number of months to fetch
            
        Returns:
            pandas Series with the data
        """
        if self.fred:
            try:
                # Fetch last N observations
                data = self.fred.get_series(series_id)
                return data.tail(periods)
                
            except Exception as e:
                logger.error(f"FRED API error: {e}")
                return self._get_mock_data(series_id)
        else:
            return self._get_mock_data(series_id)
    
    def _get_mock_data(self, series_id: str) -> pd.Series:
        """
        Generate mock data when API is unavailable.
        Used for testing and development.
        """
        import numpy as np
        
        logger.info("   Using mock data (API unavailable)")
        
        # Generate 24 months of mock data
        dates = pd.date_range(
            end=datetime.now(),
            periods=24,
            freq='M'
        )
        
        # Different patterns for different series
        if "FREIGHT" in series_id.upper():
            # Freight: slight upward trend with seasonality
            base = 1.1
            trend = np.linspace(0, 0.1, 24)
            seasonal = 0.05 * np.sin(np.linspace(0, 4*np.pi, 24))
            noise = np.random.normal(0, 0.02, 24)
            values = base + trend + seasonal + noise
            
        elif "INV" in series_id.upper():
            # Inventories: cyclical
            base = 700
            cycle = 50 * np.sin(np.linspace(0, 2*np.pi, 24))
            noise = np.random.normal(0, 10, 24)
            values = base + cycle + noise
            
        else:
            # Employment: steady growth
            base = 1500
            trend = np.linspace(0, 100, 24)
            noise = np.random.normal(0, 10, 24)
            values = base + trend + noise
        
        return pd.Series(values, index=dates)
    
    def _calculate_trend(self, data: pd.Series) -> Dict:
        """
        Calculate trend metrics for a series.
        
        Returns:
            Dict with trend direction and percentage change
        """
        if len(data) < 6:
            return {"direction": "unknown", "pct_change": 0.0}
        
        # 3-month moving average comparison
        current_avg = data.iloc[-3:].mean()
        previous_avg = data.iloc[-6:-3].mean()
        
        if previous_avg == 0:
            pct_change = 0.0
        else:
            pct_change = (current_avg - previous_avg) / previous_avg
        
        # Classify direction
        if pct_change > 0.05:
            direction = "expanding"
        elif pct_change < -0.05:
            direction = "contracting"
        else:
            direction = "stable"
        
        return {
            "direction": direction,
            "pct_change": pct_change,
            "current_value": data.iloc[-1],
            "current_avg": current_avg,
            "previous_avg": previous_avg
        }
    
    def _save_indicator(
        self, 
        series_id: str, 
        series_name: str, 
        data: pd.Series,
        trend: Dict
    ):
        """Save economic indicator to database."""
        try:
            # Save latest observation
            record = {
                "series_id": series_id,
                "series_name": series_name,
                "record_date": data.index[-1].date().isoformat(),
                "value": float(data.iloc[-1]),
                "pct_change_mom": trend.get("pct_change", 0),
                "trend_direction": trend.get("direction", "unknown")
            }
            
            db.upsert(
                "economic_indicators",
                record,
                conflict_columns=["series_id", "record_date"]
            )
            
            logger.debug(f"   💾 Saved indicator to database")
            
        except Exception as e:
            logger.error(f"Error saving indicator: {e}")
    
    def get_macro_modifier(self) -> float:
        """
        Calculate the overall macro environment modifier.
        
        This modifier adjusts propensity scores based on economic conditions:
        - Expansion: 1.1 (boost scores by 10%)
        - Stable: 1.0 (no change)
        - Contraction: 0.9 (reduce scores by 10%)
        
        Returns:
            Multiplier between 0.8 and 1.2
        """
        # Fetch the key freight indicator
        freight_data = self._fetch_series(FRED_SERIES["freight_shipments"])
        
        if freight_data is None or len(freight_data) < 6:
            logger.warning("Insufficient data for macro modifier, using 1.0")
            return 1.0
        
        trend = self._calculate_trend(freight_data)
        pct_change = trend["pct_change"]
        
        # Calculate modifier
        if pct_change > 0.10:
            modifier = 1.2  # Strong expansion
        elif pct_change > 0.05:
            modifier = 1.1  # Moderate expansion
        elif pct_change > -0.05:
            modifier = 1.0  # Stable
        elif pct_change > -0.10:
            modifier = 0.9  # Moderate contraction
        else:
            modifier = 0.8  # Strong contraction
        
        logger.info(f"📊 Macro Modifier: {modifier} ({trend['direction']})")
        
        return modifier
    
    def get_sector_outlook(self) -> Dict[str, str]:
        """
        Get outlook for different sectors based on indicators.
        
        Returns:
            Dict mapping sector to outlook (bullish/neutral/bearish)
        """
        outlook = {}
        
        # Warehouse outlook from freight + warehouse employment
        freight = self._fetch_series(FRED_SERIES["freight_shipments"])
        warehouse_emp = self._fetch_series(FRED_SERIES.get("warehouse_employment", ""))
        
        if freight is not None:
            freight_trend = self._calculate_trend(freight)
            if freight_trend["direction"] == "expanding":
                outlook["warehouse"] = "bullish"
            elif freight_trend["direction"] == "contracting":
                outlook["warehouse"] = "bearish"
            else:
                outlook["warehouse"] = "neutral"
        
        # Manufacturing outlook
        inventory = self._fetch_series(FRED_SERIES["manufacturing_inventory"])
        
        if inventory is not None:
            inv_trend = self._calculate_trend(inventory)
            # Rising inventories can mean either:
            # - Expansion (building stock for demand) = bullish
            # - Bullwhip (demand dropped, stuck with stock) = bearish
            # Use freight as tie-breaker
            if inv_trend["direction"] == "expanding":
                if outlook.get("warehouse") == "bullish":
                    outlook["manufacturing"] = "bullish"
                else:
                    outlook["manufacturing"] = "neutral"  # Inventory build with low freight = watch
            else:
                outlook["manufacturing"] = "neutral"
        
        return outlook


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
    pipeline = MacroPipeline()
    results = pipeline.run()
    
    # Show macro modifier
    print("\n" + "=" * 60)
    print("📊 ECONOMIC ENVIRONMENT ANALYSIS")
    print("=" * 60)
    
    modifier = pipeline.get_macro_modifier()
    print(f"\n🎯 Macro Modifier: {modifier}")
    print(f"   (Apply this to all propensity scores)")
    
    # Show sector outlook
    outlook = pipeline.get_sector_outlook()
    print("\n📈 Sector Outlook:")
    for sector, status in outlook.items():
        emoji = "🟢" if status == "bullish" else "🟡" if status == "neutral" else "🔴"
        print(f"   {emoji} {sector.title()}: {status.upper()}")
    
    # Example application
    print("\n📝 Example Score Adjustment:")
    base_score = 75
    adjusted = base_score * modifier
    print(f"   Base propensity score: {base_score}")
    print(f"   After macro adjustment: {adjusted:.1f}")


if __name__ == "__main__":
    main()
