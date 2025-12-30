#!/usr/bin/env python3
"""Standalone scoring script for GitHub Actions."""

import json
import os
import sys

def main():
    try:
        from orchestration.scoring_engine import ScoringEngine
        
        engine = ScoringEngine()
        
        # Use the actual method name: score_all
        results = []
        
        if hasattr(engine, 'score_all'):
            results = engine.score_all(limit=1000)
        elif hasattr(engine, 'get_hot_leads'):
            results = engine.get_hot_leads(limit=1000)
        elif hasattr(engine, 'score_company'):
            # Score each company individually
            from database.connection import get_supabase_client
            client = get_supabase_client()
            response = client.table('company_master').select('id').execute()
            for company in response.data:
                try:
                    result = engine.score_company(company['id'])
                    if result:
                        results.append(result)
                except:
                    pass
        else:
            methods = [m for m in dir(engine) if not m.startswith('_')]
            print(f"Available methods: {methods}")
            print("No scoring method found")
            results = []
        
        # Handle different result formats
        if isinstance(results, dict):
            results = [results]
        elif results is None:
            results = []
        
        # Count tiers
        tiers = {'hot': 0, 'warm': 0, 'cool': 0, 'cold': 0}
        
        for r in results:
            if isinstance(r, dict):
                tier = r.get('tier', r.get('score_tier', 'cold'))
            else:
                tier = getattr(r, 'tier', getattr(r, 'score_tier', 'cold'))
            
            if tier:
                tier = tier.lower()
                tiers[tier] = tiers.get(tier, 0) + 1
        
        print(f"Total scored: {len(results)}")
        print(f"Hot: {tiers['hot']}, Warm: {tiers['warm']}, Cool: {tiers['cool']}, Cold: {tiers['cold']}")
        
        # Write results to file
        with open('scoring_results.json', 'w') as f:
            json.dump({
                'total': len(results),
                'hot': tiers['hot'],
                'warm': tiers['warm'],
                'cool': tiers['cool'],
                'cold': tiers['cold']
            }, f)
        
        # Write to GitHub output file
        github_output = os.environ.get('GITHUB_OUTPUT', '')
        if github_output:
            with open(github_output, 'a') as f:
                f.write(f"hot_leads={tiers['hot']}\n")
                f.write(f"total_scored={len(results)}\n")
        
        return 0
        
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    sys.exit(main())
