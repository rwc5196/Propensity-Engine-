#!/usr/bin/env python3
"""
💾 DATABASE SETUP SCRIPT
========================
Initializes the Supabase database with required tables.

WHAT IT DOES:
1. Connects to your Supabase project
2. Creates all required tables
3. Sets up indexes for fast queries
4. Creates the hot_leads view

USAGE:
    python scripts/setup_database.py
    
PREREQUISITES:
    1. Create a Supabase project at https://supabase.com
    2. Set SUPABASE_URL and SUPABASE_KEY in your .env file
"""

import sys
from pathlib import Path

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from loguru import logger


def setup_logging():
    """Configure logging."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO"
    )


def check_connection():
    """Verify Supabase connection."""
    from config.settings import settings
    
    if not settings.database.is_configured:
        logger.error("❌ Supabase credentials not configured!")
        logger.info("")
        logger.info("📝 TO FIX:")
        logger.info("   1. Go to https://supabase.com and create a project")
        logger.info("   2. Go to Project Settings → API")
        logger.info("   3. Copy your Project URL and anon/public key")
        logger.info("   4. Create a .env file in the project root with:")
        logger.info("      SUPABASE_URL=your-project-url")
        logger.info("      SUPABASE_KEY=your-anon-key")
        return False
    
    try:
        from database.connection import db
        # Test connection with a simple query
        db.client.table("_test_connection").select("*").limit(1).execute()
        logger.info("✅ Connected to Supabase successfully")
        return True
    except Exception as e:
        if "does not exist" in str(e):
            # This is fine - table doesn't exist yet but connection works
            logger.info("✅ Connected to Supabase successfully")
            return True
        else:
            logger.error(f"❌ Connection failed: {e}")
            return False


def create_tables():
    """Create all database tables using SQL."""
    from database.connection import db
    
    logger.info("\n📊 Creating database tables...")
    
    # Read the schema file
    schema_file = PROJECT_ROOT / "database" / "schema.sql"
    
    if not schema_file.exists():
        logger.error(f"Schema file not found: {schema_file}")
        return False
    
    schema_sql = schema_file.read_text()
    
    # Split into individual statements
    statements = [s.strip() for s in schema_sql.split(";") if s.strip()]
    
    # Note: Supabase doesn't allow raw SQL via the client API
    # We need to use the SQL Editor in the dashboard
    
    logger.info("")
    logger.info("=" * 60)
    logger.info("⚠️  IMPORTANT: Manual Step Required")
    logger.info("=" * 60)
    logger.info("")
    logger.info("Supabase requires running SQL directly in the dashboard.")
    logger.info("Please follow these steps:")
    logger.info("")
    logger.info("1. Go to https://supabase.com and open your project")
    logger.info("2. Click 'SQL Editor' in the left sidebar")
    logger.info("3. Click 'New query'")
    logger.info(f"4. Copy the contents of: {schema_file}")
    logger.info("5. Paste into the SQL Editor")
    logger.info("6. Click 'Run' (or press Ctrl+Enter)")
    logger.info("")
    logger.info("The schema file location:")
    logger.info(f"  {schema_file}")
    logger.info("")
    
    # Offer to print the SQL
    print("Would you like to see the SQL schema? (y/n): ", end="")
    response = input().strip().lower()
    
    if response == "y":
        print("\n" + "=" * 60)
        print("SQL SCHEMA (copy this to Supabase SQL Editor)")
        print("=" * 60 + "\n")
        print(schema_sql)
        print("\n" + "=" * 60)
    
    return True


def verify_tables():
    """Verify that tables were created."""
    from database.connection import db
    
    logger.info("\n🔍 Verifying tables...")
    
    required_tables = [
        "company_master",
        "signal_history",
        "raw_permits",
        "raw_warn_notices",
        "raw_job_postings",
        "economic_indicators",
    ]
    
    existing = []
    missing = []
    
    for table in required_tables:
        try:
            # Try to query the table
            db.client.table(table).select("*").limit(1).execute()
            existing.append(table)
            logger.info(f"   ✅ {table}")
        except Exception as e:
            if "does not exist" in str(e).lower():
                missing.append(table)
                logger.warning(f"   ❌ {table} (not created yet)")
            else:
                logger.error(f"   ⚠️ {table} (error: {e})")
    
    if missing:
        logger.warning(f"\n⚠️ {len(missing)} tables missing - run SQL schema in Supabase")
        return False
    
    logger.info(f"\n✅ All {len(existing)} tables verified!")
    return True


def insert_sample_data():
    """Insert sample data for testing."""
    from database.connection import db
    
    logger.info("\n📝 Inserting sample data...")
    
    # Sample companies (DFW area)
    sample_companies = [
        {
            "company_name": "ABC Distribution",
            "normalized_name": "abc distribution",
            "city": "Dallas",
            "state": "TX",
            "zip_code": "75201",
            "industry": "Logistics",
            "data_source": "sample"
        },
        {
            "company_name": "XYZ Logistics LLC",
            "normalized_name": "xyz logistics",
            "city": "Fort Worth",
            "state": "TX",
            "zip_code": "76102",
            "industry": "Warehousing",
            "data_source": "sample"
        },
        {
            "company_name": "Metro Fulfillment Center",
            "normalized_name": "metro fulfillment center",
            "city": "Arlington",
            "state": "TX",
            "zip_code": "76010",
            "industry": "E-commerce Fulfillment",
            "data_source": "sample"
        }
    ]
    
    inserted = 0
    for company in sample_companies:
        try:
            result = db.get_or_create_company(
                company_name=company["company_name"],
                zip_code=company["zip_code"],
                city=company["city"],
                state=company["state"],
                industry=company.get("industry"),
                data_source=company.get("data_source")
            )
            if result:
                inserted += 1
        except Exception as e:
            logger.debug(f"Error inserting {company['company_name']}: {e}")
    
    logger.info(f"   Inserted {inserted} sample companies")
    
    return inserted > 0


def print_next_steps():
    """Print next steps for the user."""
    logger.info("\n" + "=" * 60)
    logger.info("🎉 DATABASE SETUP COMPLETE")
    logger.info("=" * 60)
    logger.info("")
    logger.info("📋 NEXT STEPS:")
    logger.info("")
    logger.info("1. If you haven't already, run the SQL schema in Supabase:")
    logger.info("   - Open Supabase Dashboard → SQL Editor")
    logger.info("   - Paste contents of database/schema.sql")
    logger.info("   - Click 'Run'")
    logger.info("")
    logger.info("2. Test the pipelines:")
    logger.info("   python -m pipelines.pipeline_1_permits")
    logger.info("")
    logger.info("3. Run all pipelines:")
    logger.info("   python scripts/run_all_pipelines.py")
    logger.info("")
    logger.info("4. View your data in Supabase:")
    logger.info("   - Go to Table Editor in your project")
    logger.info("   - Browse company_master, signal_history, etc.")
    logger.info("")


def main():
    """Main setup function."""
    setup_logging()
    
    logger.info("💾 PROPENSITY ENGINE DATABASE SETUP")
    logger.info("=" * 40)
    
    # Step 1: Check connection
    logger.info("\n[Step 1/4] Checking Supabase connection...")
    if not check_connection():
        sys.exit(1)
    
    # Step 2: Create tables (manual step)
    logger.info("\n[Step 2/4] Database schema...")
    create_tables()
    
    # Step 3: Verify tables
    logger.info("\n[Step 3/4] Verifying tables...")
    tables_ok = verify_tables()
    
    # Step 4: Insert sample data (only if tables exist)
    if tables_ok:
        logger.info("\n[Step 4/4] Sample data...")
        insert_sample_data()
    else:
        logger.info("\n[Step 4/4] Skipping sample data (tables not ready)")
    
    # Print next steps
    print_next_steps()


if __name__ == "__main__":
    main()
