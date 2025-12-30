"""
💾 DATABASE CONNECTION MODULE
==============================
Handles all Supabase interactions with retry logic and error handling.
"""

import os
from typing import Any, Dict, List, Optional
from datetime import datetime, date
import json

from supabase import create_client, Client
from tenacity import retry, stop_after_attempt, wait_exponential
from loguru import logger

# Import settings
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings


class DatabaseConnection:
    """
    Singleton class for Supabase database operations.
    
    Usage:
        from database.connection import db
        
        # Insert a record
        db.insert("company_master", {"company_name": "Acme Inc"})
        
        # Query records
        results = db.query("company_master", filters={"city": "Dallas"})
        
        # Upsert (insert or update)
        db.upsert("signal_history", data, conflict_columns=["company_id", "record_date"])
    """
    
    _instance: Optional['DatabaseConnection'] = None
    _client: Optional[Client] = None
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if self._client is None:
            self._initialize_client()
    
    def _initialize_client(self):
        """Initialize the Supabase client."""
        url = settings.database.supabase_url
        key = settings.database.supabase_key
        
        if not url or not key:
            logger.warning("⚠️ Supabase credentials not configured!")
            logger.info("Set SUPABASE_URL and SUPABASE_KEY in your .env file")
            return
        
        try:
            self._client = create_client(url, key)
            logger.info("✅ Connected to Supabase successfully")
        except Exception as e:
            logger.error(f"❌ Failed to connect to Supabase: {e}")
            raise
    
    @property
    def client(self) -> Client:
        """Get the Supabase client, initializing if needed."""
        if self._client is None:
            self._initialize_client()
        return self._client
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def insert(
        self, 
        table: str, 
        data: Dict[str, Any]
    ) -> Optional[Dict]:
        """
        Insert a single record into a table.
        
        Args:
            table: Table name
            data: Dictionary of column:value pairs
            
        Returns:
            The inserted record or None on error
        """
        try:
            # Convert dates to strings for JSON serialization
            clean_data = self._serialize_data(data)
            
            response = self.client.table(table).insert(clean_data).execute()
            
            if response.data:
                logger.debug(f"Inserted record into {table}")
                return response.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Insert error in {table}: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def insert_many(
        self, 
        table: str, 
        records: List[Dict[str, Any]]
    ) -> List[Dict]:
        """
        Insert multiple records at once (batch insert).
        
        Args:
            table: Table name
            records: List of dictionaries
            
        Returns:
            List of inserted records
        """
        if not records:
            return []
        
        try:
            clean_records = [self._serialize_data(r) for r in records]
            response = self.client.table(table).insert(clean_records).execute()
            
            logger.info(f"Inserted {len(response.data)} records into {table}")
            return response.data
            
        except Exception as e:
            logger.error(f"Batch insert error in {table}: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def upsert(
        self, 
        table: str, 
        data: Dict[str, Any],
        conflict_columns: List[str]
    ) -> Optional[Dict]:
        """
        Insert or update a record based on conflict columns.
        
        Args:
            table: Table name
            data: Dictionary of column:value pairs
            conflict_columns: Columns that determine uniqueness
            
        Returns:
            The upserted record
        """
        try:
            clean_data = self._serialize_data(data)
            
            response = (
                self.client
                .table(table)
                .upsert(clean_data, on_conflict=",".join(conflict_columns))
                .execute()
            )
            
            if response.data:
                logger.debug(f"Upserted record in {table}")
                return response.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Upsert error in {table}: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def query(
        self,
        table: str,
        columns: str = "*",
        filters: Optional[Dict[str, Any]] = None,
        order_by: Optional[str] = None,
        limit: Optional[int] = None,
        offset: int = 0
    ) -> List[Dict]:
        """
        Query records from a table.
        
        Args:
            table: Table name
            columns: Comma-separated column names or "*" for all
            filters: Dictionary of column:value pairs for WHERE clause
            order_by: Column name to sort by (prefix with - for DESC)
            limit: Maximum records to return
            offset: Number of records to skip
            
        Returns:
            List of matching records
        """
        try:
            query = self.client.table(table).select(columns)
            
            # Apply filters
            if filters:
                for col, val in filters.items():
                    if isinstance(val, list):
                        query = query.in_(col, val)
                    else:
                        query = query.eq(col, val)
            
            # Apply ordering
            if order_by:
                if order_by.startswith("-"):
                    query = query.order(order_by[1:], desc=True)
                else:
                    query = query.order(order_by)
            
            # Apply pagination
            if limit:
                query = query.limit(limit)
            if offset:
                query = query.offset(offset)
            
            response = query.execute()
            return response.data
            
        except Exception as e:
            logger.error(f"Query error in {table}: {e}")
            raise
    
    def get_by_id(self, table: str, id: str) -> Optional[Dict]:
        """Get a single record by UUID."""
        results = self.query(table, filters={"id": id}, limit=1)
        return results[0] if results else None
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def update(
        self,
        table: str,
        id: str,
        data: Dict[str, Any]
    ) -> Optional[Dict]:
        """
        Update a record by ID.
        
        Args:
            table: Table name
            id: Record UUID
            data: Fields to update
            
        Returns:
            Updated record
        """
        try:
            clean_data = self._serialize_data(data)
            
            response = (
                self.client
                .table(table)
                .update(clean_data)
                .eq("id", id)
                .execute()
            )
            
            if response.data:
                logger.debug(f"Updated record {id} in {table}")
                return response.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Update error in {table}: {e}")
            raise
    
    @retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    def delete(self, table: str, id: str) -> bool:
        """Delete a record by ID."""
        try:
            self.client.table(table).delete().eq("id", id).execute()
            logger.debug(f"Deleted record {id} from {table}")
            return True
        except Exception as e:
            logger.error(f"Delete error in {table}: {e}")
            return False
    
    def execute_sql(self, sql: str) -> Any:
        """
        Execute raw SQL (use with caution).
        Requires Supabase Pro plan for full SQL access.
        """
        try:
            response = self.client.rpc("execute_sql", {"query": sql}).execute()
            return response.data
        except Exception as e:
            logger.error(f"SQL execution error: {e}")
            raise
    
    @staticmethod
    def _serialize_data(data: Dict[str, Any]) -> Dict[str, Any]:
        """Convert Python objects to JSON-serializable types."""
        result = {}
        for key, value in data.items():
            if isinstance(value, (datetime, date)):
                result[key] = value.isoformat()
            elif value is None:
                result[key] = None
            else:
                result[key] = value
        return result
    
    # ===================================
    # CONVENIENCE METHODS FOR THIS PROJECT
    # ===================================
    
    def get_or_create_company(
        self,
        company_name: str,
        zip_code: str,
        **extra_fields
    ) -> Dict:
        """
        Get existing company or create new one.
        
        Args:
            company_name: Company name
            zip_code: Location zip code
            **extra_fields: Additional fields like city, state, etc.
            
        Returns:
            Company record (existing or newly created)
        """
        # Normalize the name for matching
        normalized = self._normalize_company_name(company_name)
        
        # Try to find existing
        existing = self.query(
            "company_master",
            filters={"normalized_name": normalized, "zip_code": zip_code},
            limit=1
        )
        
        if existing:
            return existing[0]
        
        # Create new
        data = {
            "company_name": company_name,
            "normalized_name": normalized,
            "zip_code": zip_code,
            **extra_fields
        }
        return self.insert("company_master", data)
    
    @staticmethod
    def _normalize_company_name(name: str) -> str:
        """Normalize company name for matching."""
        if not name:
            return ""
        
        # Lowercase
        normalized = name.lower().strip()
        
        # Remove common suffixes
        suffixes = [
            " llc", " inc", " corp", " corporation", " co", " company",
            " ltd", " limited", " lp", " llp", " pllc", " pc"
        ]
        for suffix in suffixes:
            if normalized.endswith(suffix):
                normalized = normalized[:-len(suffix)]
        
        # Remove punctuation
        normalized = normalized.replace(",", "").replace(".", "").replace("'", "")
        
        return normalized.strip()
    
    def save_permit(self, permit_data: Dict) -> Optional[Dict]:
        """Save a raw permit record."""
        return self.upsert(
            "raw_permits",
            permit_data,
            conflict_columns=["source_city", "permit_id"]
        )
    
    def save_warn_notice(self, warn_data: Dict) -> Optional[Dict]:
        """Save a raw WARN notice."""
        return self.upsert(
            "raw_warn_notices",
            warn_data,
            conflict_columns=["source_state", "company_name", "notice_date"]
        )
    
    def save_signal_history(self, company_id: str, signals: Dict) -> Optional[Dict]:
        """Save a signal history snapshot."""
        data = {"company_id": company_id, **signals}
        return self.upsert(
            "signal_history",
            data,
            conflict_columns=["company_id", "record_date"]
        )
    
    def get_hot_leads(self, min_score: int = 75, limit: int = 100) -> List[Dict]:
        """Get companies with high propensity scores."""
        return self.query(
            "hot_leads",
            order_by="-propensity_score",
            limit=limit
        )


# Singleton instance - use this in other modules
db = DatabaseConnection()


# ===========================================
# TESTING
# ===========================================

if __name__ == "__main__":
    # Test the connection
    logger.info("Testing database connection...")
    
    try:
        # Test insert
        test_company = db.get_or_create_company(
            company_name="Test Company LLC",
            zip_code="75001",
            city="Dallas",
            state="TX"
        )
        logger.info(f"✅ Test company created/retrieved: {test_company}")
        
        # Test query
        companies = db.query("company_master", limit=5)
        logger.info(f"✅ Found {len(companies)} companies in database")
        
        logger.info("✅ All database tests passed!")
        
    except Exception as e:
        logger.error(f"❌ Database test failed: {e}")
