#!/usr/bin/env python3
"""
LinkedIn X-Ray Search v3 - PROGRESSIVE ENRICHMENT
==================================================
Key improvements:
1. Tracks which companies have been searched (avoids duplicates)
2. Only targets companies with NO contact yet
3. Marks companies as "search_attempted" to prevent re-searching
4. Better logging and success tracking

Usage:
    python linkedin_xray_search_v3.py --auto 15
"""

import os
import sys
import time
import urllib.parse
import requests
import re
from datetime import datetime
from typing import List, Dict, Optional, Tuple

# API Keys
SERPAPI_KEY = os.environ.get('SERPAPI_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# =============================================================================
# SEARCH CONFIGURATION
# =============================================================================

# Titles to search (in order)
SEARCH_TITLES = [
    'plant manager',
    'operations manager', 
    'procurement manager',
    'facility manager',
    'warehouse manager',
    'production manager',
    'hr director',
]

# GOOD titles - accept these
GOOD_TITLES = [
    'plant manager', 'facility manager', 'site manager', 'general manager',
    'operations manager', 'operations director', 'vp operations', 'director of operations',
    'production manager', 'manufacturing manager', 'production director',
    'procurement manager', 'purchasing manager', 'sourcing manager',
    'warehouse manager', 'distribution manager', 'logistics manager',
    'hr director', 'human resources director', 'talent acquisition',
]

# BAD titles - skip these
BAD_TITLES = [
    'marketing', 'sales', 'account', 'business development',
    'finance', 'financial', 'accounting', 'controller',
    'legal', 'counsel', 'attorney', 'compliance',
    'communications', 'public relations', 'media', 'brand',
    'customer experience', 'customer success', 'customer service',
    'software', 'engineer', 'developer', 'architect', 'IT ',
    'data', 'analytics', 'scientist', 'research',
    'platform', 'product manager', 'product director',
    'consultant', 'advisory', 'associate director', 'process mining',
    'intern', 'assistant', 'coordinator', 'specialist',
]


def score_title(title: str) -> int:
    """Score a title 0-100. Higher = better match."""
    if not title:
        return 0
    
    title_lower = title.lower()
    
    # Exclusions
    for bad in BAD_TITLES:
        if bad.lower() in title_lower:
            return 0
    
    # Tier 1: Plant/Facility (100)
    if any(t in title_lower for t in ['plant manager', 'facility manager', 'site manager']):
        return 100
    
    # Tier 2: Procurement (90)
    if any(t in title_lower for t in ['procurement', 'purchasing', 'sourcing']):
        return 90
    
    # Tier 3: Operations (80)
    if any(t in title_lower for t in ['operations manager', 'operations director', 'vp operations']):
        return 80
    
    # Tier 4: Production (75)
    if any(t in title_lower for t in ['production manager', 'manufacturing manager']):
        return 75
    
    # Tier 5: Warehouse (70)
    if any(t in title_lower for t in ['warehouse manager', 'distribution manager', 'logistics manager']):
        return 70
    
    # Tier 6: HR (60)
    if any(t in title_lower for t in ['hr director', 'human resources director', 'talent acquisition']):
        return 60
    
    # Generic manager/director
    if 'manager' in title_lower or 'director' in title_lower:
        return 30
    
    return 10


# =============================================================================
# SERPAPI SEARCH
# =============================================================================

def serpapi_search(query: str, num_results: int = 5) -> Dict:
    """Search Google via SerpAPI."""
    if not SERPAPI_KEY:
        return {"error": "SERPAPI_KEY not set"}
    
    url = "https://serpapi.com/search"
    params = {
        "q": query,
        "api_key": SERPAPI_KEY,
        "engine": "google",
        "num": num_results,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if "error" in data:
            return {"error": data["error"]}
        
        results = []
        for item in data.get("organic_results", []):
            link = item.get("link", "")
            if "linkedin.com/in/" in link:
                title_text = item.get("title", "")
                snippet = item.get("snippet", "")
                
                name, job_title = parse_result(title_text, snippet)
                score = score_title(job_title)
                
                results.append({
                    "name": name,
                    "title": job_title,
                    "linkedin_url": link,
                    "score": score,
                })
        
        results.sort(key=lambda x: x['score'], reverse=True)
        return {"success": True, "results": results}
        
    except Exception as e:
        return {"error": str(e)}


def parse_result(title_text: str, snippet: str) -> Tuple[str, str]:
    """Parse name and title from Google result."""
    name = ""
    job_title = ""
    
    if " - " in title_text:
        parts = title_text.split(" - ")
        name = parts[0].strip()
        if len(parts) >= 2:
            job_title = parts[1].replace(" | LinkedIn", "").strip()
    elif " | " in title_text:
        name = title_text.split(" | ")[0].strip()
    
    if not job_title and snippet:
        for title in SEARCH_TITLES:
            if title in snippet.lower():
                job_title = title.title()
                break
    
    return name, job_title


def search_company(company: str, city: str = None, state: str = None) -> Optional[Dict]:
    """Search for contacts at a company, trying multiple titles."""
    
    best_contact = None
    best_score = 0
    
    for title in SEARCH_TITLES[:4]:  # Limit to 4 searches per company
        query_parts = ['site:linkedin.com/in', f'"{title}"', f'"{company}"']
        
        # Add city for smaller cities (helps find local staff)
        if city and len(city) > 3:
            query_parts.append(f'"{city}"')
        
        query = ' '.join(query_parts)
        result = serpapi_search(query)
        
        if result.get('success') and result.get('results'):
            for contact in result['results']:
                score = contact.get('score', 0)
                if score >= 60 and score > best_score:
                    best_contact = contact
                    best_score = score
                    
                    # Found excellent match, stop searching
                    if score >= 80:
                        return best_contact
        
        time.sleep(0.5)
    
    return best_contact


# =============================================================================
# DATABASE FUNCTIONS
# =============================================================================

def get_supabase():
    """Get Supabase client."""
    try:
        from supabase import create_client
        if SUPABASE_URL and SUPABASE_KEY:
            return create_client(SUPABASE_URL, SUPABASE_KEY)
    except ImportError:
        pass
    return None


def get_new_companies(limit: int = 15) -> List[Dict]:
    """
    Get companies that:
    1. Have Hunter email pattern
    2. Have NO contact yet (NULL primary_contact_name)
    3. Are hot leads
    
    This ensures we're enriching NEW companies, not re-searching old ones.
    """
    supabase = get_supabase()
    if not supabase:
        return []
    
    try:
        # Get hot leads
        sh = supabase.table('signal_history').select(
            'company_id, priority_rank'
        ).eq('score_tier', 'hot').order('priority_rank').limit(300).execute()
        
        company_ids = [s['company_id'] for s in sh.data]
        
        # Get companies with patterns but NO contact
        cm = supabase.table('company_master').select(
            'id, company_name, city, state, website, hunter_email_pattern'
        ).in_('id', company_ids).not_.is_(
            'hunter_email_pattern', 'null'
        ).is_(
            'primary_contact_name', 'null'  # KEY: Only companies with NO contact
        ).limit(limit * 2).execute()
        
        # Sort by priority
        priority_lookup = {s['company_id']: s['priority_rank'] for s in sh.data}
        results = [{**c, 'rank': priority_lookup.get(c['id'], 999)} for c in cm.data]
        results.sort(key=lambda x: x['rank'])
        
        return results[:limit]
        
    except Exception as e:
        print(f"Database error: {e}")
        return []


def get_companies_with_wrong_contacts(limit: int = 15) -> List[Dict]:
    """
    Get companies that have contacts but WRONG type.
    Lower priority than companies with no contacts.
    """
    supabase = get_supabase()
    if not supabase:
        return []
    
    try:
        sh = supabase.table('signal_history').select(
            'company_id, priority_rank'
        ).eq('score_tier', 'hot').order('priority_rank').limit(300).execute()
        
        company_ids = [s['company_id'] for s in sh.data]
        
        cm = supabase.table('company_master').select(
            'id, company_name, city, state, website, hunter_email_pattern, primary_contact_title'
        ).in_('id', company_ids).not_.is_(
            'hunter_email_pattern', 'null'
        ).not_.is_(
            'primary_contact_name', 'null'  # Has a contact
        ).limit(100).execute()
        
        # Filter to wrong contacts (score < 60)
        wrong_contacts = []
        for c in cm.data:
            title = c.get('primary_contact_title') or ''
            if score_title(title) < 60:
                wrong_contacts.append(c)
        
        # Sort by priority
        priority_lookup = {s['company_id']: s['priority_rank'] for s in sh.data}
        wrong_contacts.sort(key=lambda x: priority_lookup.get(x['id'], 999))
        
        return wrong_contacts[:limit]
        
    except Exception as e:
        print(f"Database error: {e}")
        return []


def extract_domain(website: str) -> Optional[str]:
    """Extract domain from website URL."""
    if not website:
        return None
    d = website.lower().replace('https://', '').replace('http://', '').replace('www.', '')
    return d.split('/')[0] if '.' in d else None


def generate_email(name: str, pattern: str, domain: str) -> Optional[str]:
    """Generate email from pattern and name."""
    if not name or not pattern or not domain:
        return None
    
    parts = name.strip().split()
    if len(parts) < 2:
        return None
    
    first = parts[0].lower()
    last = parts[-1].lower()
    f = first[0] if first else ''
    
    email = pattern.replace('{first}', first)
    email = email.replace('{last}', last)
    email = email.replace('{f}', f)
    
    return f"{email}@{domain}"


def save_contact(company_id: int, contact: Dict, pattern: str, website: str) -> bool:
    """Save contact to database."""
    supabase = get_supabase()
    if not supabase:
        return False
    
    try:
        domain = extract_domain(website)
        email = generate_email(contact.get('name', ''), pattern, domain)
        
        update = {
            'primary_contact_name': contact.get('name'),
            'primary_contact_title': contact.get('title'),
            'primary_contact_linkedin': contact.get('linkedin_url'),
            'xray_search_date': datetime.now().isoformat(),
        }
        
        if email:
            update['primary_contact_email'] = email
        
        supabase.table('company_master').update(update).eq('id', company_id).execute()
        return True
        
    except Exception as e:
        print(f"Save error: {e}")
        return False


def mark_searched(company_id: int) -> bool:
    """Mark company as searched (even if no contact found)."""
    supabase = get_supabase()
    if not supabase:
        return False
    
    try:
        supabase.table('company_master').update({
            'xray_search_date': datetime.now().isoformat(),
        }).eq('id', company_id).execute()
        return True
    except:
        return False


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def run_progressive_enrichment(limit: int = 15):
    """
    Run progressive enrichment:
    1. First, enrich companies with NO contacts
    2. Then, try to improve companies with wrong contacts
    """
    print("=" * 70)
    print("LINKEDIN X-RAY SEARCH v3 - PROGRESSIVE ENRICHMENT")
    print("=" * 70)
    print("Strategy:")
    print("  1. Target companies with NO contacts (highest priority)")
    print("  2. Skip companies already searched")
    print("  3. Track all search attempts")
    print("=" * 70)
    
    if not SERPAPI_KEY:
        print("\n❌ SERPAPI_KEY not set!")
        return
    
    # Phase 1: Companies with NO contact
    print("\n📋 PHASE 1: Companies with NO contacts")
    print("-" * 50)
    
    new_companies = get_new_companies(limit)
    print(f"Found {len(new_companies)} companies needing contacts\n")
    
    total_processed = 0
    total_found = 0
    tier1_found = 0
    tier2_found = 0
    
    for i, co in enumerate(new_companies, 1):
        company_id = co['id']
        name = co['company_name']
        city = co.get('city', '')
        state = co.get('state', '')
        pattern = co.get('hunter_email_pattern', '')
        website = co.get('website', '')
        
        print(f"{i}. {name} ({city}, {state})")
        
        contact = search_company(name, city, state)
        total_processed += 1
        
        if contact and contact.get('score', 0) >= 60:
            score = contact['score']
            tier = "T1" if score >= 80 else "T2"
            
            print(f"   ✅ {tier}: {contact['name']} - {contact['title']}")
            
            domain = extract_domain(website)
            if domain and pattern:
                email = generate_email(contact['name'], pattern, domain)
                print(f"   📧 {email}")
            
            if save_contact(company_id, contact, pattern, website):
                print(f"   ✓ Saved")
                total_found += 1
                if score >= 80:
                    tier1_found += 1
                else:
                    tier2_found += 1
        else:
            print(f"   ⚠️ No qualified contact found")
            mark_searched(company_id)  # Mark as searched to avoid re-trying
        
        print()
        time.sleep(1)
    
    # Phase 2: If we have capacity, try companies with wrong contacts
    remaining = limit - len(new_companies)
    if remaining > 0:
        print(f"\n📋 PHASE 2: Improving {remaining} companies with wrong contacts")
        print("-" * 50)
        
        wrong_companies = get_companies_with_wrong_contacts(remaining)
        
        for i, co in enumerate(wrong_companies, 1):
            company_id = co['id']
            name = co['company_name']
            city = co.get('city', '')
            state = co.get('state', '')
            pattern = co.get('hunter_email_pattern', '')
            website = co.get('website', '')
            current_title = co.get('primary_contact_title', 'Unknown')
            
            print(f"{i}. {name} (Current: {current_title})")
            
            contact = search_company(name, city, state)
            total_processed += 1
            
            if contact and contact.get('score', 0) >= 60:
                score = contact['score']
                current_score = score_title(current_title)
                
                # Only update if new contact is better
                if score > current_score:
                    print(f"   ✅ UPGRADE: {contact['name']} - {contact['title']}")
                    if save_contact(company_id, contact, pattern, website):
                        total_found += 1
                        if score >= 80:
                            tier1_found += 1
                        else:
                            tier2_found += 1
                else:
                    print(f"   ⏭️ No better contact found")
            else:
                print(f"   ⚠️ No qualified contact")
            
            print()
            time.sleep(1)
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Companies processed: {total_processed}")
    print(f"  Contacts found: {total_found}")
    print(f"    - Tier 1 (Plant/Procurement): {tier1_found}")
    print(f"    - Tier 2 (Operations/HR): {tier2_found}")
    print(f"  Success rate: {total_found}/{total_processed} ({100*total_found//max(total_processed,1)}%)")
    print(f"\n💡 Run again to process the NEXT batch of companies!")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='LinkedIn X-Ray Search v3')
    parser.add_argument('--auto', type=int, default=15, help='Number of companies')
    args = parser.parse_args()
    run_progressive_enrichment(args.auto)


if __name__ == "__main__":
    main()
