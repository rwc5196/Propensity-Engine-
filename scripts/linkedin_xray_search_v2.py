#!/usr/bin/env python3
"""
LinkedIn X-Ray Search v2 - OPTIMIZED
=====================================
Improved targeting for plant managers, operations, and procurement.

Key Improvements:
1. Multiple search attempts per company (plant → ops → procurement)
2. Title validation - skips wrong contact types
3. Industry-specific search terms
4. Location-aware searching (facility city, not HQ)
5. Better result parsing

Usage:
    python linkedin_xray_search_v2.py --auto 10
"""

import os
import sys
import time
import urllib.parse
import requests
import re
from typing import List, Dict, Optional, Tuple

# API Keys
SERPAPI_KEY = os.environ.get('SERPAPI_KEY')
SUPABASE_URL = os.environ.get('SUPABASE_URL')
SUPABASE_KEY = os.environ.get('SUPABASE_KEY')


# =============================================================================
# OPTIMIZED SEARCH CONFIGURATION
# =============================================================================

# Search titles in priority order - will try each until finding a good match
SEARCH_SEQUENCES = [
    # Sequence 1: Plant/Facility focused
    ["plant manager", "facility manager", "site manager"],
    # Sequence 2: Operations focused
    ["operations manager", "operations director", "production manager"],
    # Sequence 3: Procurement/Purchasing
    ["procurement manager", "purchasing manager", "sourcing manager"],
    # Sequence 4: Warehouse/Distribution (for logistics companies)
    ["warehouse manager", "distribution manager", "logistics manager"],
]

# GOOD titles - we want these
GOOD_TITLES = [
    'plant manager', 'facility manager', 'site manager', 'general manager',
    'operations manager', 'operations director', 'vp operations', 'director of operations',
    'production manager', 'manufacturing manager', 'production director',
    'procurement manager', 'purchasing manager', 'sourcing manager', 'vendor manager',
    'warehouse manager', 'distribution manager', 'logistics manager',
    'hr director', 'human resources director', 'talent acquisition',
]

# BAD titles - skip these contacts
BAD_TITLES = [
    'marketing', 'sales', 'account', 'business development',
    'finance', 'financial', 'accounting', 'controller', 'treasurer',
    'legal', 'counsel', 'attorney', 'compliance',
    'communications', 'public relations', 'media', 'brand',
    'customer experience', 'customer success', 'customer service',
    'software', 'engineer', 'developer', 'architect', 'IT ',
    'data', 'analytics', 'scientist', 'research',
    'platform', 'product manager', 'product director',
    'consultant', 'advisory', 'associate director',
    'intern', 'assistant', 'coordinator', 'specialist',
    'process mining',  # Specifically exclude this (from Novartis result)
]

# Industry-specific search preferences
INDUSTRY_SEARCH_MAP = {
    'manufacturing': ['plant manager', 'operations manager', 'production manager'],
    'distribution': ['warehouse manager', 'distribution manager', 'operations manager'],
    'logistics': ['warehouse manager', 'logistics manager', 'operations manager'],
    'food': ['plant manager', 'production manager', 'operations manager'],
    'warehouse': ['warehouse manager', 'facility manager', 'operations manager'],
    'retail': ['distribution manager', 'operations manager', 'procurement manager'],
}


# =============================================================================
# TITLE VALIDATION
# =============================================================================

def is_good_title(title: str) -> bool:
    """Check if a title is relevant for staffing decisions."""
    if not title:
        return False
    
    title_lower = title.lower()
    
    # Check for bad titles first (exclusions)
    for bad in BAD_TITLES:
        if bad.lower() in title_lower:
            return False
    
    # Check for good titles
    for good in GOOD_TITLES:
        if good.lower() in title_lower:
            return True
    
    return False


def score_title(title: str) -> int:
    """
    Score a title for relevance. Higher = better.
    Returns 0 for bad titles.
    """
    if not title:
        return 0
    
    title_lower = title.lower()
    
    # Exclusions - return 0
    for bad in BAD_TITLES:
        if bad.lower() in title_lower:
            return 0
    
    # Tier 1: Direct plant/facility management (100 points)
    tier1 = ['plant manager', 'facility manager', 'site manager', 'general manager plant']
    for t in tier1:
        if t in title_lower:
            return 100
    
    # Tier 2: Procurement (90 points)
    tier2 = ['procurement manager', 'procurement director', 'purchasing manager', 'purchasing director']
    for t in tier2:
        if t in title_lower:
            return 90
    
    # Tier 3: Operations management (80 points)
    tier3 = ['operations manager', 'operations director', 'vp operations', 'director of operations']
    for t in tier3:
        if t in title_lower:
            return 80
    
    # Tier 4: Production/Manufacturing (75 points)
    tier4 = ['production manager', 'manufacturing manager', 'production director']
    for t in tier4:
        if t in title_lower:
            return 75
    
    # Tier 5: Warehouse/Distribution (70 points)
    tier5 = ['warehouse manager', 'distribution manager', 'logistics manager']
    for t in tier5:
        if t in title_lower:
            return 70
    
    # Tier 6: HR (60 points)
    tier6 = ['hr director', 'human resources director', 'talent acquisition director']
    for t in tier6:
        if t in title_lower:
            return 60
    
    # Has "manager" or "director" but unknown type
    if 'manager' in title_lower or 'director' in title_lower:
        return 30
    
    return 10


# =============================================================================
# SERPAPI SEARCH WITH VALIDATION
# =============================================================================

def serpapi_search(query: str, num_results: int = 10) -> Dict:
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
                
                # Parse name and title from Google result
                name, title = parse_linkedin_result(title_text, snippet)
                
                results.append({
                    "name": name,
                    "title": title,
                    "linkedin_url": link,
                    "snippet": snippet,
                    "score": score_title(title),
                })
        
        # Sort by score (highest first)
        results.sort(key=lambda x: x['score'], reverse=True)
        
        return {"success": True, "results": results}
        
    except Exception as e:
        return {"error": str(e)}


def parse_linkedin_result(title_text: str, snippet: str) -> Tuple[str, str]:
    """
    Parse name and job title from Google LinkedIn result.
    
    Title format: "John Smith - Plant Manager - Cummins | LinkedIn"
    Snippet format: "View John Smith's profile... Plant Manager at Cummins..."
    """
    name = ""
    job_title = ""
    
    # Parse from title (most reliable)
    # Format: "Name - Title - Company | LinkedIn"
    if " - " in title_text:
        parts = title_text.split(" - ")
        name = parts[0].strip()
        if len(parts) >= 2:
            # Remove "| LinkedIn" from title
            job_title = parts[1].replace(" | LinkedIn", "").strip()
    elif " | " in title_text:
        name = title_text.split(" | ")[0].strip()
    
    # Try to extract title from snippet if not found
    if not job_title and snippet:
        # Look for patterns like "Plant Manager at Company"
        patterns = [
            r'(plant manager|operations manager|procurement manager|facility manager|warehouse manager|hr director)',
            r'([A-Za-z\s]+manager)',
            r'([A-Za-z\s]+director)',
        ]
        for pattern in patterns:
            match = re.search(pattern, snippet.lower())
            if match:
                job_title = match.group(1).title()
                break
    
    return name, job_title


# =============================================================================
# MULTI-ATTEMPT SEARCH
# =============================================================================

def search_company_contacts(
    company: str, 
    city: str = None, 
    state: str = None,
    industry: str = None
) -> Optional[Dict]:
    """
    Search for contacts at a company using multiple search attempts.
    Returns the best contact found, or None if no good match.
    """
    
    # Determine search sequence based on industry
    if industry:
        industry_lower = industry.lower()
        for key, titles in INDUSTRY_SEARCH_MAP.items():
            if key in industry_lower:
                search_titles = titles
                break
        else:
            search_titles = ['plant manager', 'operations manager', 'procurement manager']
    else:
        search_titles = ['plant manager', 'operations manager', 'procurement manager', 'warehouse manager']
    
    best_contact = None
    best_score = 0
    searches_made = 0
    max_searches = 4  # Limit API calls per company
    
    for title in search_titles:
        if searches_made >= max_searches:
            break
        
        # Build search query
        query_parts = [
            'site:linkedin.com/in',
            f'"{title}"',
            f'"{company}"',
        ]
        
        # Add location if available (helps find local facility staff)
        if city and city.lower() not in ['new york', 'san francisco', 'chicago', 'boston']:
            # Skip major HQ cities, they return corporate staff
            query_parts.append(f'"{city}"')
        
        query = ' '.join(query_parts)
        
        result = serpapi_search(query, num_results=5)
        searches_made += 1
        
        if result.get('success') and result.get('results'):
            for contact in result['results']:
                score = contact.get('score', 0)
                
                # Only consider good contacts (score >= 60)
                if score >= 60 and score > best_score:
                    best_contact = contact
                    best_score = score
                    
                    # If we found a tier 1 or 2 contact (score >= 80), stop searching
                    if score >= 80:
                        return best_contact
        
        time.sleep(0.5)  # Small delay between searches
    
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


def get_companies_needing_research(limit: int = 10) -> List[Dict]:
    """Get companies that need better contacts."""
    supabase = get_supabase()
    if not supabase:
        return []
    
    try:
        # Get hot leads
        sh = supabase.table('signal_history').select(
            'company_id, priority_rank'
        ).eq('score_tier', 'hot').order('priority_rank').limit(200).execute()
        
        company_ids = [s['company_id'] for s in sh.data]
        
        # Get companies with patterns but wrong/missing contacts
        cm = supabase.table('company_master').select(
            'id, company_name, city, state, website, hunter_email_pattern, primary_contact_title, industry'
        ).in_('id', company_ids).not_.is_('hunter_email_pattern', 'null').limit(150).execute()
        
        # Filter to those needing better contacts
        needs_research = []
        for c in cm.data:
            title = c.get('primary_contact_title') or ''
            score = score_title(title)
            
            # Need research if score < 60 (no good contact)
            if score < 60:
                needs_research.append(c)
        
        # Sort by priority
        priority_lookup = {s['company_id']: s['priority_rank'] for s in sh.data}
        needs_research.sort(key=lambda x: priority_lookup.get(x['id'], 999))
        
        return needs_research[:limit]
        
    except Exception as e:
        print(f"Database error: {e}")
        return []


def generate_email(name: str, pattern: str, domain: str) -> Optional[str]:
    """Generate email from Hunter pattern and name."""
    if not name or not pattern:
        return None
    
    parts = name.strip().split()
    if len(parts) < 2:
        return None
    
    first = parts[0].lower()
    last = parts[-1].lower()
    f = first[0] if first else ''
    l = last[0] if last else ''
    
    email = pattern.replace('{first}', first)
    email = email.replace('{last}', last)
    email = email.replace('{f}', f)
    email = email.replace('{l}', l)
    
    return f"{email}@{domain}"


def extract_domain(website: str) -> Optional[str]:
    """Extract domain from website URL."""
    if not website:
        return None
    d = website.lower().replace('https://', '').replace('http://', '').replace('www.', '')
    return d.split('/')[0] if '.' in d else None


def save_contact(company_name: str, contact: Dict, pattern: str, website: str) -> bool:
    """Save contact to database."""
    supabase = get_supabase()
    if not supabase:
        return False
    
    try:
        domain = extract_domain(website)
        email = generate_email(contact.get('name', ''), pattern, domain) if domain else None
        
        update = {
            'primary_contact_name': contact.get('name'),
            'primary_contact_title': contact.get('title'),
            'primary_contact_linkedin': contact.get('linkedin_url'),
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


# =============================================================================
# MAIN EXECUTION
# =============================================================================

def run_optimized_search(limit: int = 10):
    """
    Run optimized X-Ray search with title validation.
    """
    print("=" * 70)
    print("LINKEDIN X-RAY SEARCH v2 - OPTIMIZED")
    print("=" * 70)
    print("Improvements:")
    print("  • Multiple search attempts per company")
    print("  • Title validation (skips wrong contacts)")
    print("  • Industry-specific targeting")
    print("=" * 70)
    
    if not SERPAPI_KEY:
        print("\n❌ SERPAPI_KEY not set!")
        return
    
    companies = get_companies_needing_research(limit)
    
    if not companies:
        print("\n⚠️  No companies found needing research")
        return
    
    print(f"\n📋 Processing {len(companies)} companies\n")
    
    found = 0
    tier1_found = 0  # Plant/Facility managers
    tier2_found = 0  # Procurement/Operations
    skipped = 0
    
    for i, co in enumerate(companies, 1):
        name = co['company_name']
        city = co.get('city', '')
        state = co.get('state', '')
        pattern = co.get('hunter_email_pattern', '')
        website = co.get('website', '')
        industry = co.get('industry', '')
        current_title = co.get('primary_contact_title', 'None')
        
        print(f"{i}. {name} ({city}, {state})")
        print(f"   Current: {current_title}")
        
        # Search for best contact
        contact = search_company_contacts(name, city, state, industry)
        
        if contact and contact.get('score', 0) >= 60:
            score = contact['score']
            tier = "T1" if score >= 80 else "T2" if score >= 60 else "T3"
            
            print(f"   ✅ {tier} FOUND: {contact['name']}")
            print(f"      Title: {contact['title']} (score: {score})")
            print(f"      LinkedIn: {contact['linkedin_url'][:50]}...")
            
            # Generate and show email
            domain = extract_domain(website)
            if domain and pattern:
                email = generate_email(contact['name'], pattern, domain)
                print(f"      Email: {email}")
            
            # Save to database
            if save_contact(name, contact, pattern, website):
                print(f"      ✓ Saved to database")
                found += 1
                if score >= 80:
                    tier1_found += 1
                else:
                    tier2_found += 1
        else:
            print(f"   ⚠️ No qualified contact found (all results were wrong type)")
            skipped += 1
        
        print()
        time.sleep(1)  # Rate limiting between companies
    
    # Summary
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"  Companies processed: {len(companies)}")
    print(f"  Good contacts found: {found}")
    print(f"    - Tier 1 (Plant/Procurement): {tier1_found}")
    print(f"    - Tier 2 (Operations/HR): {tier2_found}")
    print(f"  No qualified contact: {skipped}")
    print(f"  Success rate: {found}/{len(companies)} ({100*found//max(len(companies),1)}%)")


def main():
    import argparse
    parser = argparse.ArgumentParser(description='LinkedIn X-Ray Search v2')
    parser.add_argument('--auto', type=int, default=10, help='Number of companies')
    args = parser.parse_args()
    run_optimized_search(args.auto)


if __name__ == "__main__":
    main()
