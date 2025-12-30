"""
🔧 PROPENSITY ENGINE PIPELINES
==============================
Seven independent data collection microservices.

Each pipeline:
- Has a single responsibility
- Can run independently
- Produces a standardized score (0-100)
- Saves to the shared database

Usage:
    from pipelines import (
        PermitPipeline,
        WARNPipeline,
        MacroPipeline,
        GlassdoorPipeline,
        JobPipeline,
        InventoryPipeline,
        LaborMarketPipeline
    )
    
    # Run individual pipeline
    permits = PermitPipeline()
    results = permits.run()
    
    # Or run all at once
    from pipelines import run_all_pipelines
    run_all_pipelines()
"""

from .pipeline_1_permits import PermitPipeline
from .pipeline_2_warn import WARNPipeline
from .pipeline_3_macro import MacroPipeline
from .pipeline_4_glassdoor import GlassdoorPipeline
from .pipeline_5_jobs import JobPipeline
from .pipeline_6_inventory import InventoryPipeline
from .pipeline_7_labor import LaborMarketPipeline

__all__ = [
    "PermitPipeline",
    "WARNPipeline",
    "MacroPipeline",
    "GlassdoorPipeline",
    "JobPipeline",
    "InventoryPipeline",
    "LaborMarketPipeline",
]


def run_all_pipelines():
    """
    Convenience function to run all pipelines sequentially.
    
    Returns:
        Dict with results from each pipeline
    """
    from loguru import logger
    
    logger.info("=" * 60)
    logger.info("🚀 RUNNING ALL 7 PIPELINES")
    logger.info("=" * 60)
    
    results = {}
    
    # Pipeline 1: Building Permits
    try:
        logger.info("\n[1/7] Building Permits...")
        p1 = PermitPipeline()
        results["permits"] = p1.run()
    except Exception as e:
        logger.error(f"Pipeline 1 failed: {e}")
        results["permits"] = None
    
    # Pipeline 2: WARN Notices
    try:
        logger.info("\n[2/7] WARN Notices...")
        p2 = WARNPipeline()
        results["warn"] = p2.run()
    except Exception as e:
        logger.error(f"Pipeline 2 failed: {e}")
        results["warn"] = None
    
    # Pipeline 3: Macro Economic
    try:
        logger.info("\n[3/7] Economic Indicators...")
        p3 = MacroPipeline()
        results["macro"] = p3.run()
        results["macro_modifier"] = p3.get_macro_modifier()
    except Exception as e:
        logger.error(f"Pipeline 3 failed: {e}")
        results["macro"] = None
        results["macro_modifier"] = 1.0
    
    # Pipeline 4: Glassdoor (skip in bulk - rate limited)
    logger.info("\n[4/7] Glassdoor Sentiment... (on-demand only)")
    results["glassdoor"] = "On-demand - see GlassdoorPipeline"
    
    # Pipeline 5: Job Postings
    try:
        logger.info("\n[5/7] Job Postings...")
        p5 = JobPipeline()
        results["jobs"] = p5.run()
    except Exception as e:
        logger.error(f"Pipeline 5 failed: {e}")
        results["jobs"] = None
    
    # Pipeline 6: Inventory (requires tickers)
    logger.info("\n[6/7] Inventory Turnover... (requires ticker list)")
    results["inventory"] = "Requires ticker input - see InventoryPipeline"
    
    # Pipeline 7: Labor Market
    try:
        logger.info("\n[7/7] Labor Market...")
        p7 = LaborMarketPipeline()
        results["labor"] = p7.run()
        results["labor_summary"] = p7.get_regional_summary()
    except Exception as e:
        logger.error(f"Pipeline 7 failed: {e}")
        results["labor"] = None
    
    logger.info("\n" + "=" * 60)
    logger.info("✅ ALL PIPELINES COMPLETE")
    logger.info("=" * 60)
    
    return results
