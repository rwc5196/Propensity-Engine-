#!/usr/bin/env python3
"""Standalone scoring script for GitHub Actions."""

import json
import sys

def main():
    try:
        from orchestration.scoring_engine import ScoringEngine
        
        engine = ScoringEngine()
        
        # Find and call the scoring method
        results = []
        
        if hasattr(engine, 'score_all_companies'):
            results = engine.score_all_companies(limit=1000)
        elif hasattr(engine, 'score_companies'):
            results = engine.score_companies(limit=1000)
        elif hasattr(engine, 'run'):
            results = engine.run(limit=1000)
        elif hasattr(engine, 'calculate_scores'):
            results = engine.calculate_scores(limit=1000)
        else:
            # List all available methods
            methods = [m for m in dir(engine) if not m.startswith('_')]
            print(f"Available methods: {methods}")
            print("No standard scoring method found")
            results = []
        
        # Count tiers
        tiers = {'hot': 0, 'warm': 0, 'cool': 0, 'cold': 0}
        
        for r in results:
            if isinstance(r, dict):
                tier = r.get('tier', 'cold')
            else:
                tier = getattr(r, 'tier', 'cold')
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
        
        # Write to GitHub output file (new method)
        import os
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
