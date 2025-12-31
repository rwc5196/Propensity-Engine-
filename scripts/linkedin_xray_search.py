#!/usr/bin/env python3
"""
LinkedIn X-Ray Search via Google
=================================
Uses Google to search LinkedIn profiles without touching LinkedIn's API.
Generates search URLs and optionally uses SerpAPI for automation.

Methods:
1. FREE: Generate Google search URLs (manual click)
2. SEMI-AUTO: Use SerpAPI (100 free searches/month)
3. SEMI-AUTO: Use Google Custom Search API (100 free/day)

Usage:
    python linkedin_xray_search.py                    # Generate URLs for top companies
    python linkedin_xray_search.py --company "Cummins" --city "Columbus" --state "IN"
    python linkedin_xray_search.py --auto 10          # Auto-search top 10 (requires API)
"""

import os
import sys
import json
import time
import urllib.parse
import requests
from typing import List, Dict, Optional

# API Keys (optional - for automation)
SERPAPI_KEY = os.environ.get('SERPAPI_KEY')  # https://serpapi.com (100 free/month)
GOOGLE_API_KEY = os.environ.get('GOOGLE_API_KEY')  # Google Custom Search
GOOGLE_CSE_ID = os.environ.get('GOOGLE_CSE_ID')  # Custom Search Engine ID

SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# =============================================================================
# TARGET TITLES FOR STAFFING DECISION MAKERS
# =============================================================================

PRIORITY_TITLES = {
    1: ["procurement manager", "purchasing manager", "sourcing manager", "vendor manager"],
    2: ["plant manager", "facility manager", "site manager", "warehouse manager", "distribution manager"],
    3: ["operations manager", "operations director", "production manager", "manufacturing manager"],
    4: ["hr director", "human resources director", "talent acquisition"],
}

# Titles to search for (in order of priority)
SEARCH_TITLES = [
    "plant manager",
    "operations manager", 
    "procurement manager",
    "facility manager",
    "warehouse manager",
    "hr director",
]


# =============================================================================
# GOOGLE X-RAY SEARCH URL GENERATORS
# =============================================================================

def generate_xray_url(company: str, title: str, city: str = None, state: str = None) -> str:
    """
    Generate a Google X-Ray search URL for LinkedIn profiles.
    
    Example output:
    https://www.google.com/search?q=site:linkedin.com/in+"plant+manager"+"Cummins"+"Columbus"
    """
    # Build search query
    query_parts = [
        'site:linkedin.com/in',
        f'"{title}"',
        f'"{company}"',
    ]
    
    if city:
        query_parts.append(f'"{city}"')
    if state:
        query_parts.append(f'"{state}"')
    
    query = ' '.join(query_parts)
    encoded = urllib.parse.quote(query)
    
    return f"https://www.google.com/search?q={encoded}"


def generate_all_search_urls(company: str, city: str = None, state: str = None) -> List[Dict]:
    """
    Generate X-Ray search URLs for all priority titles.
    """
    urls = []
    for title in SEARCH_TITLES:
        urls.append({
            "title": title,
            "url": generate_xray_url(company, title, city, state),
            "query": f'site:linkedin.com/in "{title}" "{company}"' + (f' "{city}"' if city else '')
        })
    return urls


# =============================================================================
# SERPAPI AUTOMATION (100 free searches/month)
# =============================================================================

def serpapi_search(query: str) -> Dict:
    """
    Search Google via SerpAPI.
    Free tier: 100 searches/month
    Sign up: https://serpapi.com
    """
    if not SERPAPI_KEY:
        return {"error": "SERPAPI_KEY not set"}
    
    url = "https://serpapi.com/search"
    params = {
        "q": query,
        "api_key": SERPAPI_KEY,
        "engine": "google",
        "num": 10,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if "error" in data:
            return {"error": data["error"]}
        
        # Parse organic results
        results = []
        for item in data.get("organic_results", []):
            link = item.get("link", "")
            if "linkedin.com/in/" in link:
                # Extract name from title (usually "Name - Title - Company | LinkedIn")
                title_text = item.get("title", "")
                snippet = item.get("snippet", "")
                
                results.append({
                    "name": title_text.split(" - ")[0] if " - " in title_text else title_text.split(" | ")[0],
                    "title": extract_title_from_snippet(snippet),
                    "linkedin_url": link,
                    "snippet": snippet,
                })
        
        return {"success": True, "results": results}
        
    except Exception as e:
        return {"error": str(e)}


def extract_title_from_snippet(snippet: str) -> str:
    """Extract job title from Google snippet."""
    # Common patterns in LinkedIn snippets
    # "John Smith - Plant Manager - Cummins"
    # "Plant Manager at Cummins"
    
    snippet_lower = snippet.lower()
    
    for title in SEARCH_TITLES:
        if title in snippet_lower:
            return title.title()
    
    # Try to extract from " - Title - " pattern
    parts = snippet.split(" - ")
    if len(parts) >= 2:
        return parts[1].strip()[:50]  # Limit length
    
    return "Unknown"


# =============================================================================
# GOOGLE CUSTOM SEARCH API (100 free/day)
# =============================================================================

def google_cse_search(query: str) -> Dict:
    """
    Search using Google Custom Search API.
    Free tier: 100 queries/day
    Setup: https://developers.google.com/custom-search/v1/introduction
    """
    if not GOOGLE_API_KEY or not GOOGLE_CSE_ID:
        return {"error": "GOOGLE_API_KEY and GOOGLE_CSE_ID not set"}
    
    url = "https://www.googleapis.com/customsearch/v1"
    params = {
        "key": GOOGLE_API_KEY,
        "cx": GOOGLE_CSE_ID,
        "q": query,
        "num": 10,
    }
    
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        
        if "error" in data:
            return {"error": data["error"]["message"]}
        
        results = []
        for item in data.get("items", []):
            link = item.get("link", "")
            if "linkedin.com/in/" in link:
                results.append({
                    "name": item.get("title", "").split(" - ")[0],
                    "title": extract_title_from_snippet(item.get("snippet", "")),
                    "linkedin_url": link,
                    "snippet": item.get("snippet", ""),
                })
        
        return {"success": True, "results": results}
        
    except Exception as e:
        return {"error": str(e)}


# =============================================================================
# DATABASE INTEGRATION
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


def get_companies_needing_contacts(limit: int = 10) -> List[Dict]:
    """Get companies that need better contacts (have pattern, wrong/no contact)."""
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
        
        # Filter to those needing better contacts
        target_keywords = ['procurement', 'purchasing', 'plant', 'facility', 'operations manager', 'operations director']
        needs_research = []
        
        for c in cm.data:
            title = (c.get('primary_contact_title') or '').lower()
            has_good_contact = any(kw in title for kw in target_keywords)
            if not has_good_contact:
                needs_research.append(c)
        
        # Sort by priority
        priority_lookup = {s['company_id']: s['priority_rank'] for s in sh.data}
        needs_research.sort(key=lambda x: priority_lookup.get(x['id'], 999))
        
        return needs_research[:limit]
        
    except Exception as e:
        print(f"Database error: {e}")
        return []


def save_contact(company_name: str, contact: Dict, pattern: str) -> bool:
    """Save contact to database, generating email from pattern."""
    supabase = get_supabase()
    if not supabase:
        return False
    
    try:
        # Generate email from pattern and name
        name = contact.get('name', '')
        email = generate_email_from_pattern(name, pattern, company_name)
        
        update = {
            'primary_contact_name': name,
            'primary_contact_title': contact.get('title', ''),
            'primary_contact_linkedin': contact.get('linkedin_url', ''),
        }
        
        if email:
            update['primary_contact_email'] = email
        
        supabase.table('company_master').update(update).ilike(
            'company_name', f"%{company_name}%"
        ).execute()
        
        return True
    except Exception as e:
        print(f"Save error: {e}")
        return False


def generate_email_from_pattern(full_name: str, pattern: str, company_name: str) -> Optional[str]:
    """Generate email from Hunter pattern and name."""
    if not full_name or not pattern:
        return None
    
    # Parse name
    parts = full_name.strip().split()
    if len(parts) < 2:
        return None
    
    first = parts[0].lower()
    last = parts[-1].lower()
    f = first[0]
    l = last[0]
    
    # Extract domain from company (simplified)
    domain_map = {
        'cummins': 'cummins.com',
        'honeywell': 'honeywell.com',
        'cintas': 'cintas.com',
        'marathon': 'marathonpetroleum.com',
        'wayfair': 'wayfair.com',
        'johnson controls': 'johnsoncontrols.com',
        'international paper': 'internationalpaper.com',
        'novartis': 'novartis.com',
    }
    
    domain = None
    for key, val in domain_map.items():
        if key in company_name.lower():
            domain = val
            break
    
    if not domain:
        return None
    
    # Apply pattern
    email = pattern.replace('{first}', first)
    email = email.replace('{last}', last)
    email = email.replace('{f}', f)
    email = email.replace('{l}', l)
    
    return f"{email}@{domain}"


# =============================================================================
# MAIN FUNCTIONS
# =============================================================================

def generate_research_list(limit: int = 10):
    """
    Generate X-Ray search URLs for companies needing contacts.
    Outputs a list ready for manual searching.
    """
    print("=" * 70)
    print("LINKEDIN X-RAY SEARCH LIST")
    print("=" * 70)
    
    companies = get_companies_needing_contacts(limit)
    
    if not companies:
        print("\n⚠️  No companies found needing contact research")
        print("   Either database not connected or all have good contacts")
        return
    
    print(f"\n📋 {len(companies)} companies need contact research\n")
    print("-" * 70)
    
    for i, co in enumerate(companies, 1):
        name = co['company_name']
        city = co.get('city', '')
        state = co.get('state', '')
        pattern = co.get('hunter_email_pattern', '')
        
        print(f"\n{i}. {name} ({city}, {state})")
        print(f"   Pattern: {pattern}")
        print(f"   Current contact: {co.get('primary_contact_title', 'None')}")
        print(f"\n   🔍 Google X-Ray Search URLs:")
        
        urls = generate_all_search_urls(name, city, state)
        for u in urls[:3]:  # Top 3 title searches
            print(f"   • {u['title'].title()}: {u['url'][:80]}...")
        
        print()


def auto_search(limit: int = 10):
    """
    Automatically search using SerpAPI or Google CSE.
    Requires API key.
    """
    print("=" * 70)
    print("AUTOMATED LINKEDIN X-RAY SEARCH")
    print("=" * 70)
    
    # Check for API keys
    if SERPAPI_KEY:
        search_func = serpapi_search
        print("Using: SerpAPI")
    elif GOOGLE_API_KEY and GOOGLE_CSE_ID:
        search_func = google_cse_search
        print("Using: Google Custom Search API")
    else:
        print("\n❌ No search API configured!")
        print("   Set one of:")
        print("   • SERPAPI_KEY (https://serpapi.com - 100 free/month)")
        print("   • GOOGLE_API_KEY + GOOGLE_CSE_ID (100 free/day)")
        print("\n   Falling back to URL generation...")
        generate_research_list(limit)
        return
    
    companies = get_companies_needing_contacts(limit)
    
    if not companies:
        print("\n⚠️  No companies found needing research")
        return
    
    print(f"\n📋 Processing {len(companies)} companies\n")
    
    found = 0
    
    for i, co in enumerate(companies, 1):
        name = co['company_name']
        city = co.get('city', '')
        state = co.get('state', '')
        pattern = co.get('hunter_email_pattern', '')
        
        print(f"{i}. {name}")
        
        # Search for plant manager first, then operations, then procurement
        for title in ["plant manager", "operations manager", "procurement manager"]:
            query = f'site:linkedin.com/in "{title}" "{name}"'
            if city:
                query += f' "{city}"'
            
            result = search_func(query)
            
            if result.get('success') and result.get('results'):
                best = result['results'][0]
                print(f"   ✅ Found: {best['name']} - {best['title']}")
                print(f"      LinkedIn: {best['linkedin_url'][:60]}...")
                
                # Generate email
                email = generate_email_from_pattern(best['name'], pattern, name)
                if email:
                    print(f"      Email: {email}")
                
                # Save to database
                if save_contact(name, best, pattern):
                    print(f"      ✓ Saved to database")
                    found += 1
                
                break  # Found a contact, move to next company
            
            time.sleep(1)  # Rate limiting
        else:
            print(f"   ⚠️ No contacts found")
        
        time.sleep(2)  # Extra delay between companies
    
    print(f"\n{'=' * 70}")
    print(f"Found contacts for {found}/{len(companies)} companies")


def search_single_company(company: str, city: str = None, state: str = None):
    """Search for contacts at a single company."""
    print(f"\n🔍 X-Ray Search for: {company}\n")
    
    urls = generate_all_search_urls(company, city, state)
    
    print("Google X-Ray Search URLs:\n")
    for u in urls:
        print(f"  {u['title'].title()}:")
        print(f"  {u['url']}\n")
    
    print("-" * 50)
    print("Instructions:")
    print("1. Click each URL above")
    print("2. Look for LinkedIn profiles in results")
    print("3. Note the person's name and verify title")
    print("4. Generate email using your Hunter pattern")


# =============================================================================
# CLI
# =============================================================================

def main():
    import argparse
    
    parser = argparse.ArgumentParser(
        description='LinkedIn X-Ray Search via Google',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python linkedin_xray_search.py                     # Generate URLs for all companies
  python linkedin_xray_search.py --company "Cummins" # Search single company
  python linkedin_xray_search.py --auto 10           # Auto-search (needs API key)
        """
    )
    
    parser.add_argument('--company', type=str, help='Search single company')
    parser.add_argument('--city', type=str, help='City filter')
    parser.add_argument('--state', type=str, help='State filter')
    parser.add_argument('--auto', type=int, metavar='N', help='Auto-search N companies (needs SerpAPI)')
    parser.add_argument('--limit', type=int, default=10, help='Number of companies for URL generation')
    
    args = parser.parse_args()
    
    if args.company:
        search_single_company(args.company, args.city, args.state)
    elif args.auto:
        auto_search(args.auto)
    else:
        generate_research_list(args.limit)


if __name__ == "__main__":
    main()
