#!/usr/bin/env python3
"""
Apollo.io Contact Enrichment Script
====================================
Finds procurement/plant/operations contacts for companies
that have Hunter email patterns but wrong/missing contacts.

Usage:
    python apollo_enrichment.py              # Enrich top 10 companies
    python apollo_enrichment.py --limit 5    # Enrich top 5 companies
    python apollo_enrichment.py --company "Cummins"  # Search single company

Setup:
    1. Add APOLLO_API_KEY to environment or GitHub Secrets
    2. Requires: pip install requests supabase
"""

import os
import sys
import time
import requests
import argparse
from datetime import datetime

# Configuration
APOLLO_API_KEY = os.environ.get('APOLLO_API_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')

# Target titles - prioritized for staffing decision makers
PRIORITY_TITLES = [
    # Priority 1: Procurement (vendor decisions)
    "Procurement Manager", "Procurement Director", "VP Procurement",
    "Purchasing Manager", "Purchasing Director", "Sourcing Manager",
    
    # Priority 2: Plant/Facility (direct hiring)
    "Plant Manager", "Facility Manager", "Site Manager",
    "General Manager", "Warehouse Manager", "Distribution Manager",
    
    # Priority 3: Operations (workforce planning)
    "Operations Manager", "Operations Director", "VP Operations",
    "Director of Operations", "Production Manager", "Manufacturing Manager",
    
    # Priority 4: HR (staffing relationships)
    "HR Director", "Human Resources Director", "VP HR",
    "Talent Acquisition Manager", "Workforce Manager"
]


def get_supabase():
    """Get Supabase client."""
    try:
        from supabase import create_client
        if SUPABASE_URL and SUPABASE_KEY:
            return create_client(SUPABASE_URL, SUPABASE_KEY)
    except ImportError:
        print("⚠️  supabase not installed. Run: pip install supabase")
    return None


def apollo_search(company_name: str, location: str = None) -> dict:
    """
    Search Apollo for people at a company.
    
    Args:
        company_name: Company to search
        location: Optional city/state filter
    
    Returns:
        Dict with contacts or error
    """
    if not APOLLO_API_KEY:
        return {"error": "APOLLO_API_KEY not set"}
    
    url = "https://api.apollo.io/v1/mixed_people/search"
    
    data = {
        "api_key": APOLLO_API_KEY,
        "q_organization_name": company_name,
        "person_titles": PRIORITY_TITLES,
        "per_page": 10,
        "page": 1
    }
    
    # Add location filter if provided
    if location:
        data["person_locations"] = [location]
    
    try:
        response = requests.post(url, json=data, timeout=30)
        
        if response.status_code == 401:
            return {"error": "Invalid API key"}
        if response.status_code == 429:
            return {"error": "Rate limited - wait and retry"}
        
        result = response.json()
        
        if 'people' in result:
            contacts = []
            for p in result['people']:
                contacts.append({
                    "name": f"{p.get('first_name', '')} {p.get('last_name', '')}".strip(),
                    "title": p.get('title', ''),
                    "email": p.get('email', ''),
                    "phone": p.get('sanitized_phone', '') or p.get('phone_number', ''),
                    "linkedin": p.get('linkedin_url', ''),
                    "city": p.get('city', ''),
                    "state": p.get('state', ''),
                    "confidence": 'verified' if p.get('email_status') == 'verified' else 'likely'
                })
            return {"success": True, "contacts": contacts, "total": result.get('pagination', {}).get('total_entries', 0)}
        
        return {"error": "No results found"}
        
    except requests.exceptions.Timeout:
        return {"error": "Request timed out"}
    except Exception as e:
        return {"error": str(e)}


def score_contact(contact: dict) -> int:
    """
    Score a contact based on title relevance.
    Higher = better match for staffing decision maker.
    """
    title = (contact.get('title') or '').lower()
    
    # Priority 1: Procurement (100 points)
    if any(kw in title for kw in ['procurement', 'purchasing', 'sourcing', 'vendor']):
        return 100
    
    # Priority 2: Plant/Facility (90 points)
    if any(kw in title for kw in ['plant manager', 'facility manager', 'site manager', 'warehouse manager']):
        return 90
    
    # Priority 3: Operations (80 points)
    if any(kw in title for kw in ['operations manager', 'operations director', 'vp operations', 'production manager']):
        return 80
    
    # Priority 4: HR (70 points)
    if any(kw in title for kw in ['hr director', 'human resources director', 'talent acquisition']):
        return 70
    
    # Has email = some value
    if contact.get('email'):
        return 30
    
    return 0


def find_best_contact(contacts: list) -> dict:
    """Find the best contact from a list based on scoring."""
    if not contacts:
        return None
    
    # Score all contacts
    scored = [(score_contact(c), c) for c in contacts]
    
    # Filter to those with emails
    with_email = [(s, c) for s, c in scored if c.get('email')]
    
    if not with_email:
        return None
    
    # Sort by score (highest first)
    with_email.sort(key=lambda x: x[0], reverse=True)
    
    # Return best if score >= 50 (has relevant title)
    if with_email[0][0] >= 50:
        return with_email[0][1]
    
    # Otherwise return highest scored with email
    return with_email[0][1]


def get_companies_needing_contacts(limit: int = 10) -> list:
    """Get hot lead companies that need better contacts."""
    supabase = get_supabase()
    if not supabase:
        return []
    
    try:
        # Get hot leads
        sh = supabase.table('signal_history').select(
            'company_id, priority_rank'
        ).eq('score_tier', 'hot').order('priority_rank').limit(200).execute()
        
        company_ids = [s['company_id'] for s in sh.data]
        
        # Get companies with patterns
        cm = supabase.table('company_master').select(
            'id, company_name, city, state, hunter_email_pattern, primary_contact_title'
        ).in_('id', company_ids).not_.is_('hunter_email_pattern', 'null').limit(150).execute()
        
        # Filter to those needing research
        target_keywords = ['procurement', 'purchasing', 'plant', 'facility', 'operations']
        needs_research = []
        
        for c in cm.data:
            title = (c.get('primary_contact_title') or '').lower()
            has_good_contact = any(kw in title for kw in target_keywords)
            if not has_good_contact:
                needs_research.append(c)
        
        # Sort by priority rank
        priority_lookup = {s['company_id']: s['priority_rank'] for s in sh.data}
        needs_research.sort(key=lambda x: priority_lookup.get(x['id'], 999))
        
        return needs_research[:limit]
        
    except Exception as e:
        print(f"⚠️  Database error: {e}")
        return []


def save_contact_to_db(company_name: str, contact: dict) -> bool:
    """Save contact to database."""
    supabase = get_supabase()
    if not supabase:
        return False
    
    try:
        update = {
            'primary_contact_name': contact.get('name'),
            'primary_contact_title': contact.get('title'),
            'primary_contact_email': contact.get('email'),
            'apollo_lookup_date': datetime.now().isoformat(),
        }
        
        if contact.get('phone'):
            update['primary_contact_phone'] = contact['phone']
        if contact.get('linkedin'):
            update['primary_contact_linkedin'] = contact['linkedin']
        
        supabase.table('company_master').update(update).ilike(
            'company_name', f"%{company_name}%"
        ).execute()
        
        return True
    except Exception as e:
        print(f"⚠️  Save error: {e}")
        return False


def enrich_companies(limit: int = 10):
    """
    Main enrichment function - find contacts for companies needing research.
    """
    print("=" * 60)
    print("APOLLO.IO CONTACT ENRICHMENT")
    print("Target: Procurement → Plant Manager → Operations → HR")
    print("=" * 60)
    
    if not APOLLO_API_KEY:
        print("\n❌ APOLLO_API_KEY not set!")
        print("   Add to environment: export APOLLO_API_KEY='your_key'")
        print("   Or add to GitHub Secrets")
        return
    
    companies = get_companies_needing_contacts(limit)
    
    if not companies:
        print("\n⚠️  No companies found needing contact enrichment")
        print("   Either all have good contacts, or database not connected")
        return
    
    print(f"\n📋 Processing {len(companies)} companies\n")
    
    found = 0
    high_quality = 0
    
    for i, co in enumerate(companies, 1):
        name = co['company_name']
        city = co.get('city', '')
        state = co.get('state', '')
        location = f"{city}, {state}" if city and state else None
        
        print(f"  {i}. {name} ({city}, {state})")
        
        # Search Apollo
        result = apollo_search(name, location)
        
        if result.get('success') and result.get('contacts'):
            total = result.get('total', 0)
            best = find_best_contact(result['contacts'])
            
            if best:
                score = score_contact(best)
                quality = "✅ HIGH" if score >= 70 else "🟡 MEDIUM" if score >= 50 else "⚪ LOW"
                
                print(f"      {quality}: {best['name']}")
                print(f"      Title: {best['title']}")
                print(f"      Email: {best['email']}")
                
                if save_contact_to_db(name, best):
                    print(f"      ✓ Saved to database")
                    found += 1
                    if score >= 70:
                        high_quality += 1
            else:
                print(f"      ⚠️ {total} results but no emails found")
        else:
            error = result.get('error', 'Unknown error')
            print(f"      ✗ {error}")
        
        # Rate limiting
        time.sleep(1)
    
    # Summary
    print(f"\n{'=' * 60}")
    print("SUMMARY")
    print("=" * 60)
    print(f"  Companies processed: {len(companies)}")
    print(f"  Contacts found: {found}")
    print(f"  High-quality (Procurement/Plant/Ops): {high_quality}")
    print(f"  Need manual LinkedIn: {len(companies) - found}")


def search_single_company(company_name: str):
    """Search for contacts at a single company."""
    print(f"\n🔍 Searching Apollo for: {company_name}\n")
    
    result = apollo_search(company_name)
    
    if result.get('success'):
        contacts = result.get('contacts', [])
        print(f"Found {len(contacts)} contacts:\n")
        
        for i, c in enumerate(contacts, 1):
            score = score_contact(c)
            quality = "⭐" if score >= 70 else "◆" if score >= 50 else "○"
            
            print(f"  {quality} {i}. {c['name']}")
            print(f"       Title: {c['title']}")
            print(f"       Email: {c['email'] or 'N/A'}")
            print(f"       Phone: {c['phone'] or 'N/A'}")
            print(f"       Location: {c['city']}, {c['state']}")
            print()
    else:
        print(f"❌ Error: {result.get('error')}")


def main():
    parser = argparse.ArgumentParser(description='Apollo.io Contact Enrichment')
    parser.add_argument('--limit', type=int, default=10, help='Number of companies to process')
    parser.add_argument('--company', type=str, help='Search single company')
    
    args = parser.parse_args()
    
    if args.company:
        search_single_company(args.company)
    else:
        enrich_companies(args.limit)


if __name__ == "__main__":
    main()
