"""
💼 PIPELINE 5: JOB POSTINGS (HIRING VELOCITY SIGNAL)
====================================================
Tracks job posting velocity as an indicator of hiring demand.

WHY THIS MATTERS:
- High job post volume = high turnover (pain point for staffing)
- Rapid increase = expansion or crisis (opportunity)
- Same jobs posted repeatedly = hard-to-fill roles (premium pricing)
- Monitoring competitors shows their client needs

DATA SOURCE: JobSpy (free, scrapes Indeed/LinkedIn/ZipRecruiter)
"""

import os
import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Dict, List, Optional
import re

import pandas as pd
from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings
from database.connection import db


class JobPipeline:
    """
    Pipeline for tracking job posting velocity.
    
    Uses python-jobspy to scrape major job boards without API keys.
    
    Key Metrics:
    - Post count: How many jobs posted in last 30 days
    - Velocity: Rate of change (acceleration = urgent need)
    - Repetition: Same role posted multiple times = hard to fill
    
    Usage:
        pipeline = JobPipeline()
        
        # Search for industrial jobs in DFW
        results = pipeline.run()
        
        # Get job velocity for a specific company
        velocity = pipeline.get_company_velocity("Amazon")
    """
    
    # Industrial job title keywords
    INDUSTRIAL_TITLES = [
        "warehouse",
        "forklift",
        "picker",
        "packer",
        "material handler",
        "shipping",
        "receiving",
        "logistics",
        "inventory",
        "stock",
        "loader",
        "unloader",
        "assembler",
        "assembly",
        "machine operator",
        "production",
        "manufacturing",
        "driver",
        "cdl",
        "order selector",
        "reach truck",
        "cherry picker"
    ]
    
    def __init__(self):
        """Initialize the pipeline."""
        self.target_cities = settings.geography.cities_list
        self.target_state = settings.geography.target_state
        
        # Build search pattern
        self.title_pattern = re.compile(
            "|".join(self.INDUSTRIAL_TITLES),
            re.IGNORECASE
        )
        
        logger.info("JobPipeline initialized")
        logger.info(f"  - Target: {', '.join(self.target_cities[:3])}...")
    
    def run(self) -> pd.DataFrame:
        """
        Run the job scraping pipeline.
        
        Returns:
            DataFrame with job postings
        """
        logger.info("=" * 50)
        logger.info("💼 STARTING PIPELINE 5: JOB POSTINGS")
        logger.info("=" * 50)
        
        all_jobs = []
        
        # Search each target city
        for city in self.target_cities[:5]:  # Limit to avoid rate limiting
            location = f"{city}, {self.target_state}"
            logger.info(f"\n📍 Searching: {location}")
            
            try:
                jobs = self._search_jobs(location)
                
                if jobs is not None and not jobs.empty:
                    logger.info(f"   ✅ Found {len(jobs)} industrial jobs")
                    
                    # Save to database
                    self._save_jobs(jobs)
                    
                    all_jobs.append(jobs)
                else:
                    logger.warning(f"   ⚠️ No jobs found")
                    
            except Exception as e:
                logger.error(f"   ❌ Error: {e}")
                continue
        
        # Combine results
        if all_jobs:
            result = pd.concat(all_jobs, ignore_index=True)
            result = result.drop_duplicates(subset=["job_url"], keep="first")
            
            logger.info(f"\n📊 TOTAL: {len(result)} unique industrial jobs")
            
            # Calculate velocity metrics
            self._analyze_velocity(result)
            
            return result
        
        logger.warning("No jobs collected")
        return pd.DataFrame()
    
    def _search_jobs(
        self, 
        location: str,
        search_term: str = "warehouse OR forklift OR logistics"
    ) -> Optional[pd.DataFrame]:
        """
        Search for jobs using JobSpy.
        
        Args:
            location: City, State format
            search_term: Job title keywords
            
        Returns:
            DataFrame of jobs
        """
        try:
            from jobspy import scrape_jobs
            
            logger.info(f"   Scraping job boards...")
            
            jobs = scrape_jobs(
                site_name=["indeed", "linkedin", "zip_recruiter"],
                search_term=search_term,
                location=location,
                results_wanted=100,  # Per site
                hours_old=168,  # Last 7 days
                country_indeed="USA"
            )
            
            if jobs is not None and len(jobs) > 0:
                # Add metadata
                jobs["search_location"] = location
                jobs["scraped_at"] = datetime.now()
                
                # Filter to industrial titles only
                jobs = self._filter_industrial(jobs)
                
                return jobs
            
            return None
            
        except ImportError:
            logger.error("python-jobspy not installed. Run: pip install python-jobspy")
            return self._fallback_search(location)
            
        except Exception as e:
            logger.error(f"JobSpy error: {e}")
            return self._fallback_search(location)
    
    def _fallback_search(self, location: str) -> Optional[pd.DataFrame]:
        """
        Fallback when JobSpy fails.
        Returns mock data for testing.
        """
        logger.info("   Using mock job data (scraper unavailable)")
        
        # Generate mock data
        companies = [
            "Amazon Fulfillment", "XPO Logistics", "UPS Supply Chain",
            "FedEx Ground", "Walmart Distribution", "Target DC",
            "Home Depot RDC", "Sysco Foods", "McLane Company"
        ]
        
        titles = [
            "Warehouse Associate", "Forklift Operator", "Material Handler",
            "Order Selector", "Shipping Clerk", "Inventory Specialist"
        ]
        
        import random
        
        records = []
        for _ in range(50):
            records.append({
                "company": random.choice(companies),
                "title": random.choice(titles),
                "location": location,
                "date_posted": datetime.now() - timedelta(days=random.randint(0, 7)),
                "job_url": f"https://example.com/job/{random.randint(10000, 99999)}",
                "site": random.choice(["indeed", "linkedin", "ziprecruiter"])
            })
        
        return pd.DataFrame(records)
    
    def _filter_industrial(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter jobs to industrial/warehouse roles only."""
        if df.empty:
            return df
        
        # Find title column
        title_col = None
        for col in ["title", "job_title", "Title"]:
            if col in df.columns:
                title_col = col
                break
        
        if not title_col:
            return df
        
        # Filter using pattern
        mask = df[title_col].fillna("").str.contains(
            self.title_pattern,
            regex=True
        )
        
        filtered = df[mask].copy()
        filtered["is_industrial"] = True
        
        return filtered
    
    def _save_jobs(self, df: pd.DataFrame):
        """Save jobs to database."""
        if df.empty:
            return
        
        saved_count = 0
        
        for _, row in df.iterrows():
            try:
                job_data = {
                    "company_name": str(row.get("company", row.get("company_name", ""))),
                    "job_title": str(row.get("title", row.get("job_title", ""))),
                    "city": self._extract_city(row.get("location", "")),
                    "state": self.target_state,
                    "source_board": str(row.get("site", "unknown")),
                    "job_url": str(row.get("job_url", "")),
                    "posted_date": self._extract_date(row.get("date_posted")),
                    "is_industrial": True
                }
                
                # Use upsert to avoid duplicates
                db.upsert(
                    "raw_job_postings",
                    job_data,
                    conflict_columns=["job_url"]
                )
                saved_count += 1
                
            except Exception as e:
                logger.debug(f"Error saving job: {e}")
                continue
        
        logger.info(f"   💾 Saved {saved_count} jobs to database")
    
    @staticmethod
    def _extract_city(location: str) -> str:
        """Extract city from location string."""
        if not location:
            return ""
        # Usually format is "City, ST" or "City, State"
        parts = location.split(",")
        return parts[0].strip() if parts else ""
    
    @staticmethod
    def _extract_date(value) -> Optional[str]:
        """Extract date from various formats."""
        if not value:
            return datetime.now().date().isoformat()
        
        if isinstance(value, datetime):
            return value.date().isoformat()
        
        if isinstance(value, str):
            try:
                return pd.to_datetime(value).date().isoformat()
            except:
                return datetime.now().date().isoformat()
        
        return datetime.now().date().isoformat()
    
    def _analyze_velocity(self, df: pd.DataFrame):
        """Analyze job posting velocity by company."""
        if df.empty:
            return
        
        # Find company column
        company_col = None
        for col in ["company", "company_name", "Company"]:
            if col in df.columns:
                company_col = col
                break
        
        if not company_col:
            return
        
        # Count by company
        velocity = df.groupby(company_col).size().sort_values(ascending=False)
        
        logger.info("\n📊 TOP HIRING COMPANIES (by job post count):")
        for company, count in velocity.head(10).items():
            logger.info(f"   • {company}: {count} jobs")
    
    def get_company_velocity(self, company_name: str) -> Dict:
        """
        Get job posting velocity metrics for a specific company.
        
        Args:
            company_name: Company to analyze
            
        Returns:
            Dict with velocity metrics
        """
        # Query from database
        jobs = db.query(
            "raw_job_postings",
            filters={"company_name": company_name},
            order_by="-posted_date",
            limit=100
        )
        
        if not jobs:
            return {
                "company_name": company_name,
                "job_count_30d": 0,
                "velocity_score": 0
            }
        
        # Filter to last 30 days
        cutoff = datetime.now() - timedelta(days=30)
        recent = [
            j for j in jobs
            if j.get("posted_date") and 
               datetime.fromisoformat(j["posted_date"]) >= cutoff
        ]
        
        count_30d = len(recent)
        
        # Calculate velocity score
        velocity_score = self.calculate_velocity_score(count_30d)
        
        return {
            "company_name": company_name,
            "job_count_30d": count_30d,
            "velocity_score": velocity_score,
            "latest_posting": recent[0]["posted_date"] if recent else None
        }
    
    def calculate_velocity_score(self, job_count_30d: int) -> float:
        """
        Calculate velocity score from job posting count.
        
        Logic:
        - 0 jobs → 0 (no activity)
        - 5 jobs → 50 (normal turnover)
        - 10+ jobs → 100 (high demand/high turnover)
        
        Args:
            job_count_30d: Number of jobs posted in last 30 days
            
        Returns:
            Score from 0-100
        """
        if job_count_30d <= 0:
            return 0.0
        
        # Linear scaling: 10 jobs = max score
        score = (job_count_30d / 10) * 100
        
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
    pipeline = JobPipeline()
    results = pipeline.run()
    
    # Show sample results
    if not results.empty:
        print("\n" + "=" * 60)
        print("📋 SAMPLE JOB POSTINGS")
        print("=" * 60)
        
        display_cols = []
        for col in ["company", "company_name", "title", "job_title", "location", "site"]:
            if col in results.columns:
                display_cols.append(col)
        
        if display_cols:
            print(results[display_cols[:4]].head(10).to_string(index=False))
    
    # Show velocity scoring
    print("\n📊 VELOCITY SCORE EXAMPLES:")
    for count in [0, 2, 5, 10, 15, 20]:
        score = pipeline.calculate_velocity_score(count)
        print(f"   {count} jobs/month → Score: {score:.1f}")


if __name__ == "__main__":
    main()
