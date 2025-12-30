#!/usr/bin/env python3
"""
🚀 RUN ALL PIPELINES
====================
Master script to execute the entire propensity scoring workflow.

WHAT IT DOES:
1. Runs all 7 data collection pipelines
2. Calculates propensity scores for all companies
3. Identifies hot leads
4. Optionally generates outreach emails

USAGE:
    python scripts/run_all_pipelines.py
    
    # With options:
    python scripts/run_all_pipelines.py --skip-glassdoor --generate-emails
"""

import sys
import os
from pathlib import Path
import argparse
from datetime import datetime

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from loguru import logger


def setup_logging(verbose: bool = False):
    """Configure logging."""
    logger.remove()
    
    level = "DEBUG" if verbose else "INFO"
    
    # Console output
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level=level
    )
    
    # File output
    log_dir = PROJECT_ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    
    log_file = log_dir / f"pipeline_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
    logger.add(log_file, level="DEBUG")
    
    return log_file


def validate_environment():
    """Check that required environment variables are set."""
    from config.settings import settings
    
    validation = settings.validate()
    
    if not validation["database_configured"]:
        logger.error("❌ Supabase not configured!")
        logger.info("   Set SUPABASE_URL and SUPABASE_KEY in .env")
        return False
    
    if not validation["gemini_configured"]:
        logger.warning("⚠️ Gemini API not configured - AI features disabled")
    
    if not validation["weights_valid"]:
        logger.error("❌ Scoring weights don't sum to 1.0!")
        return False
    
    logger.info("✅ Environment validated")
    return True


def run_pipelines(skip_glassdoor: bool = False, skip_sec: bool = False):
    """
    Run all data collection pipelines.
    
    Args:
        skip_glassdoor: Skip Glassdoor (rate limited)
        skip_sec: Skip SEC data (requires ticker list)
    """
    from pipelines import (
        PermitPipeline,
        WARNPipeline,
        MacroPipeline,
        GlassdoorPipeline,
        JobPipeline,
        InventoryPipeline,
        LaborMarketPipeline
    )
    
    results = {}
    
    # Pipeline 1: Building Permits
    logger.info("\n" + "=" * 50)
    logger.info("📍 [1/7] BUILDING PERMITS")
    logger.info("=" * 50)
    try:
        p1 = PermitPipeline()
        results["permits"] = p1.run()
        logger.info(f"✅ Collected {len(results['permits']) if results['permits'] is not None else 0} permits")
    except Exception as e:
        logger.error(f"❌ Pipeline 1 failed: {e}")
        results["permits"] = None
    
    # Pipeline 2: WARN Notices
    logger.info("\n" + "=" * 50)
    logger.info("⚠️ [2/7] WARN NOTICES")
    logger.info("=" * 50)
    try:
        p2 = WARNPipeline()
        results["warn"] = p2.run()
        logger.info(f"✅ Collected {len(results['warn']) if results['warn'] is not None else 0} notices")
    except Exception as e:
        logger.error(f"❌ Pipeline 2 failed: {e}")
        results["warn"] = None
    
    # Pipeline 3: Macro Economic
    logger.info("\n" + "=" * 50)
    logger.info("📈 [3/7] ECONOMIC INDICATORS")
    logger.info("=" * 50)
    try:
        p3 = MacroPipeline()
        results["macro"] = p3.run()
        results["macro_modifier"] = p3.get_macro_modifier()
        logger.info(f"✅ Macro modifier: {results['macro_modifier']}")
    except Exception as e:
        logger.error(f"❌ Pipeline 3 failed: {e}")
        results["macro_modifier"] = 1.0
    
    # Pipeline 4: Glassdoor Sentiment
    if not skip_glassdoor:
        logger.info("\n" + "=" * 50)
        logger.info("⭐ [4/7] GLASSDOOR SENTIMENT")
        logger.info("=" * 50)
        logger.info("ℹ️ Glassdoor is rate-limited - processing sample companies")
        try:
            p4 = GlassdoorPipeline()
            # Just test with a few companies
            sample_companies = ["Amazon", "XPO Logistics", "FedEx"]
            results["glassdoor"] = p4.run(sample_companies)
        except Exception as e:
            logger.error(f"❌ Pipeline 4 failed: {e}")
            results["glassdoor"] = None
    else:
        logger.info("\n⏭️ Skipping Glassdoor (--skip-glassdoor flag)")
        results["glassdoor"] = None
    
    # Pipeline 5: Job Postings
    logger.info("\n" + "=" * 50)
    logger.info("💼 [5/7] JOB POSTINGS")
    logger.info("=" * 50)
    try:
        p5 = JobPipeline()
        results["jobs"] = p5.run()
        logger.info(f"✅ Collected {len(results['jobs']) if results['jobs'] is not None else 0} jobs")
    except Exception as e:
        logger.error(f"❌ Pipeline 5 failed: {e}")
        results["jobs"] = None
    
    # Pipeline 6: Inventory Turnover
    if not skip_sec:
        logger.info("\n" + "=" * 50)
        logger.info("📦 [6/7] INVENTORY TURNOVER")
        logger.info("=" * 50)
        try:
            p6 = InventoryPipeline()
            # Process DFW-area public companies
            dfw_tickers = ["WMT", "HD", "TGT", "COST", "KR"]
            results["inventory"] = p6.run(dfw_tickers)
        except Exception as e:
            logger.error(f"❌ Pipeline 6 failed: {e}")
            results["inventory"] = None
    else:
        logger.info("\n⏭️ Skipping SEC data (--skip-sec flag)")
        results["inventory"] = None
    
    # Pipeline 7: Labor Market
    logger.info("\n" + "=" * 50)
    logger.info("📍 [7/7] LABOR MARKET")
    logger.info("=" * 50)
    try:
        p7 = LaborMarketPipeline()
        results["labor"] = p7.run()
        results["labor_summary"] = p7.get_regional_summary()
        logger.info(f"✅ Regional unemployment: {results['labor_summary'].get('average_unemployment', 'N/A')}%")
    except Exception as e:
        logger.error(f"❌ Pipeline 7 failed: {e}")
        results["labor"] = None
    
    return results


def calculate_scores():
    """Calculate propensity scores for all companies."""
    from orchestration.scoring_engine import ScoringEngine
    
    logger.info("\n" + "=" * 50)
    logger.info("🎯 CALCULATING PROPENSITY SCORES")
    logger.info("=" * 50)
    
    engine = ScoringEngine()
    results = engine.score_all(limit=500)
    
    # Summarize by tier
    tiers = {"hot": 0, "warm": 0, "cool": 0, "cold": 0}
    for r in results:
        tier = r.get("score_tier", "cold")
        tiers[tier] = tiers.get(tier, 0) + 1
    
    logger.info(f"\n📊 SCORING RESULTS:")
    logger.info(f"   🔥 Hot leads:  {tiers['hot']}")
    logger.info(f"   👍 Warm leads: {tiers['warm']}")
    logger.info(f"   🤔 Cool leads: {tiers['cool']}")
    logger.info(f"   ❄️ Cold leads: {tiers['cold']}")
    
    return results


def generate_emails(scores: list):
    """Generate outreach emails for hot leads."""
    from orchestration.sales_agent import SalesAgent
    
    logger.info("\n" + "=" * 50)
    logger.info("📧 GENERATING OUTREACH EMAILS")
    logger.info("=" * 50)
    
    # Filter to hot leads only
    hot_leads = [s for s in scores if s.get("score_tier") == "hot"]
    
    if not hot_leads:
        logger.info("No hot leads to process")
        return []
    
    agent = SalesAgent()
    emails = []
    
    for lead in hot_leads[:10]:  # Limit to 10 for demo
        try:
            email = agent.generate_outreach(
                company_name=lead.get("company_name", "Company"),
                signals=lead.get("breakdown", {})
            )
            email["company_name"] = lead.get("company_name")
            email["propensity_score"] = lead.get("propensity_score")
            emails.append(email)
        except Exception as e:
            logger.error(f"Error generating email for {lead.get('company_name')}: {e}")
    
    logger.info(f"✅ Generated {len(emails)} outreach emails")
    
    return emails


def print_summary(pipeline_results: dict, scores: list, emails: list):
    """Print final summary."""
    logger.info("\n" + "=" * 60)
    logger.info("📊 PIPELINE RUN COMPLETE")
    logger.info("=" * 60)
    
    logger.info("\n📈 DATA COLLECTED:")
    
    if pipeline_results.get("permits") is not None:
        logger.info(f"   • Building permits: {len(pipeline_results['permits'])}")
    
    if pipeline_results.get("warn") is not None:
        logger.info(f"   • WARN notices: {len(pipeline_results['warn'])}")
    
    if pipeline_results.get("jobs") is not None:
        logger.info(f"   • Job postings: {len(pipeline_results['jobs'])}")
    
    if pipeline_results.get("labor_summary"):
        summary = pipeline_results["labor_summary"]
        logger.info(f"   • DFW unemployment: {summary.get('average_unemployment', 'N/A')}%")
    
    logger.info(f"\n🎯 COMPANIES SCORED: {len(scores)}")
    
    hot_count = len([s for s in scores if s.get("score_tier") == "hot"])
    logger.info(f"   • Hot leads: {hot_count}")
    
    if emails:
        logger.info(f"\n📧 EMAILS GENERATED: {len(emails)}")
    
    logger.info("\n✅ Run complete!")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Run all propensity scoring pipelines"
    )
    parser.add_argument(
        "--skip-glassdoor",
        action="store_true",
        help="Skip Glassdoor pipeline (rate limited)"
    )
    parser.add_argument(
        "--skip-sec",
        action="store_true",
        help="Skip SEC inventory pipeline"
    )
    parser.add_argument(
        "--generate-emails",
        action="store_true",
        help="Generate outreach emails for hot leads"
    )
    parser.add_argument(
        "--verbose", "-v",
        action="store_true",
        help="Enable verbose logging"
    )
    
    args = parser.parse_args()
    
    # Setup
    log_file = setup_logging(args.verbose)
    
    logger.info("🚀 PROPENSITY ENGINE")
    logger.info(f"📅 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📝 Log file: {log_file}")
    
    # Validate environment
    if not validate_environment():
        logger.error("Environment validation failed. Exiting.")
        sys.exit(1)
    
    # Run pipelines
    pipeline_results = run_pipelines(
        skip_glassdoor=args.skip_glassdoor,
        skip_sec=args.skip_sec
    )
    
    # Calculate scores
    scores = calculate_scores()
    
    # Generate emails if requested
    emails = []
    if args.generate_emails:
        emails = generate_emails(scores)
    
    # Print summary
    print_summary(pipeline_results, scores, emails)


if __name__ == "__main__":
    main()
