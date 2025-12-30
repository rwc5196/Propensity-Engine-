"""
⚙️ PROPENSITY ENGINE SETTINGS
==============================
Central configuration for all pipelines and services.
Loads values from environment variables with sensible defaults.
"""

import os
from pathlib import Path
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field
from dotenv import load_dotenv

# Load .env file from project root
PROJECT_ROOT = Path(__file__).parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
if ENV_FILE.exists():
    load_dotenv(ENV_FILE)
else:
    # Try config directory
    load_dotenv(PROJECT_ROOT / "config" / ".env")


class DatabaseSettings(BaseSettings):
    """Supabase database configuration."""
    supabase_url: str = Field(default="", env="SUPABASE_URL")
    supabase_key: str = Field(default="", env="SUPABASE_KEY")
    
    @property
    def is_configured(self) -> bool:
        return bool(self.supabase_url and self.supabase_key)


class APISettings(BaseSettings):
    """API keys for external services."""
    gemini_api_key: str = Field(default="", env="GEMINI_API_KEY")
    fred_api_key: str = Field(default="", env="FRED_API_KEY")
    bls_api_key: str = Field(default="", env="BLS_API_KEY")
    socrata_app_token: str = Field(default="", env="SOCRATA_APP_TOKEN")
    sec_user_agent: str = Field(
        default="PropensityEngine contact@example.com",
        env="SEC_USER_AGENT"
    )


class GeographySettings(BaseSettings):
    """Target geography configuration."""
    target_cities: str = Field(
        default="Dallas,Fort Worth,Arlington,Irving,Plano",
        env="TARGET_CITIES"
    )
    target_state: str = Field(default="TX", env="TARGET_STATE")
    target_zips: str = Field(default="75001,75006,75019", env="TARGET_ZIPS")
    
    @property
    def cities_list(self) -> List[str]:
        return [c.strip() for c in self.target_cities.split(",")]
    
    @property
    def zips_list(self) -> List[str]:
        return [z.strip() for z in self.target_zips.split(",")]


class PipelineSettings(BaseSettings):
    """Pipeline-specific configuration."""
    min_permit_value: int = Field(default=50000, env="MIN_PERMIT_VALUE")
    permit_lookback_days: int = Field(default=30, env="PERMIT_LOOKBACK_DAYS")
    hot_lead_threshold: int = Field(default=75, env="HOT_LEAD_THRESHOLD")


class ScoringWeights(BaseSettings):
    """Propensity score calculation weights."""
    expansion: float = Field(default=0.25, env="WEIGHT_EXPANSION")
    distress: float = Field(default=0.20, env="WEIGHT_DISTRESS")
    job_velocity: float = Field(default=0.20, env="WEIGHT_JOB_VELOCITY")
    sentiment: float = Field(default=0.15, env="WEIGHT_SENTIMENT")
    market_tightness: float = Field(default=0.10, env="WEIGHT_MARKET_TIGHTNESS")
    macro: float = Field(default=0.10, env="WEIGHT_MACRO")
    
    def validate_weights(self) -> bool:
        """Ensure weights sum to 1.0."""
        total = (
            self.expansion + self.distress + self.job_velocity +
            self.sentiment + self.market_tightness + self.macro
        )
        return abs(total - 1.0) < 0.001


class AISettings(BaseSettings):
    """AI model configuration."""
    gemini_flash_model: str = Field(
        default="gemini-3-flash-preview",
        env="GEMINI_FLASH_MODEL"
    )
    gemini_pro_model: str = Field(
        default="gemini-3-pro-preview",
        env="GEMINI_PRO_MODEL"
    )
    claude_model: str = Field(
        default="claude-sonnet-4-20250514",
        env="CLAUDE_MODEL"
    )


class Settings:
    """
    Master settings class that combines all configuration.
    
    Usage:
        from config.settings import settings
        
        # Access database settings
        url = settings.database.supabase_url
        
        # Access geography
        cities = settings.geography.cities_list
        
        # Access scoring weights
        w = settings.weights.expansion
    """
    
    def __init__(self):
        self.database = DatabaseSettings()
        self.api = APISettings()
        self.geography = GeographySettings()
        self.pipeline = PipelineSettings()
        self.weights = ScoringWeights()
        self.ai = AISettings()
        self.project_root = PROJECT_ROOT
    
    def validate(self) -> dict:
        """
        Validate all settings and return status.
        Returns dict with validation results.
        """
        results = {
            "database_configured": self.database.is_configured,
            "gemini_configured": bool(self.api.gemini_api_key),
            "fred_configured": bool(self.api.fred_api_key),
            "weights_valid": self.weights.validate_weights(),
            "target_cities": len(self.geography.cities_list),
            "target_zips": len(self.geography.zips_list),
        }
        results["all_valid"] = all([
            results["database_configured"],
            results["gemini_configured"],
            results["weights_valid"]
        ])
        return results
    
    def print_status(self):
        """Print configuration status to console."""
        validation = self.validate()
        
        print("\n" + "="*50)
        print("⚙️  PROPENSITY ENGINE CONFIGURATION STATUS")
        print("="*50)
        
        print("\n📡 API Keys:")
        print(f"  • Supabase: {'✅ Configured' if validation['database_configured'] else '❌ Missing'}")
        print(f"  • Gemini:   {'✅ Configured' if validation['gemini_configured'] else '❌ Missing'}")
        print(f"  • FRED:     {'✅ Configured' if validation['fred_configured'] else '⚠️  Optional'}")
        
        print("\n🎯 Geography:")
        print(f"  • State: {self.geography.target_state}")
        print(f"  • Cities: {validation['target_cities']} configured")
        print(f"  • Zip codes: {validation['target_zips']} configured")
        
        print("\n⚖️  Scoring Weights:")
        print(f"  • Valid: {'✅ Yes' if validation['weights_valid'] else '❌ No (must sum to 1.0)'}")
        
        print("\n" + "="*50)
        if validation["all_valid"]:
            print("✅ All critical settings configured! Ready to run.")
        else:
            print("❌ Some settings are missing. Check your .env file.")
        print("="*50 + "\n")
        
        return validation


# Singleton instance - import this in other modules
settings = Settings()


# ===========================================
# SOCRATA ENDPOINTS (Building Permits)
# ===========================================
# These are the open data portals for major cities

SOCRATA_ENDPOINTS = {
    "Dallas": {
        "domain": "www.dallasopendata.com",
        "dataset_id": "e7gq-4sah",
        "date_field": "permit_issue_date",
        "description_field": "work_description",
        "value_field": "estimated_cost",
        "address_field": "site_address"
    },
    "Fort Worth": {
        "domain": "data.fortworthtexas.gov",
        "dataset_id": "x4m5-e4hn",
        "date_field": "issue_date",
        "description_field": "description",
        "value_field": "valuation",
        "address_field": "address"
    },
    # Houston (nearby market, good for expansion)
    "Houston": {
        "domain": "data.houstontx.gov",
        "dataset_id": "9bts-bhwh",
        "date_field": "permit_issue_date",
        "description_field": "permit_description",
        "value_field": "project_value",
        "address_field": "site_address"
    },
    # Add more cities as you expand
}


# ===========================================
# INDUSTRIAL KEYWORDS (for permit filtering)
# ===========================================
# These keywords in permit descriptions indicate industrial expansion

INDUSTRIAL_KEYWORDS = [
    "warehouse",
    "distribution",
    "logistics",
    "conveyor",
    "racking",
    "rack",
    "mezzanine",
    "loading dock",
    "dock leveler",
    "cold storage",
    "freezer",
    "cooler",
    "manufacturing",
    "assembly",
    "packaging",
    "fulfillment",
    "cross-dock",
    "forklift",
    "pallet",
    "industrial",
    "3pl",
    "third party logistics",
]


# ===========================================
# WARN NOTICE STATES
# ===========================================
# States we scrape WARN notices from

WARN_STATES = ["TX", "CA", "IL", "NY", "FL", "GA", "OH", "PA"]


# ===========================================
# FRED SERIES (Economic Indicators)
# ===========================================

FRED_SERIES = {
    "freight_shipments": "FRGSHPUSM649NCIS",  # Cass Freight Index
    "manufacturing_inventory": "MNFCTRMPCIMSA",
    "trucking_employment": "CES4348400001",
    "warehouse_employment": "CES4349300001",
}


if __name__ == "__main__":
    # Test configuration when run directly
    settings.print_status()
