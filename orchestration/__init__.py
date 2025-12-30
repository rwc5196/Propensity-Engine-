"""
🤖 PROPENSITY ENGINE ORCHESTRATION
==================================
AI-powered scoring and outreach automation.

Components:
- ScoringEngine: Calculates propensity scores from all signals
- SalesAgent: Generates personalized outreach using Gemini

Usage:
    from orchestration import ScoringEngine, SalesAgent
    
    # Score companies
    engine = ScoringEngine()
    scores = engine.score_all()
    hot_leads = engine.get_hot_leads(min_score=75)
    
    # Generate outreach
    agent = SalesAgent()
    emails = agent.process_hot_leads()
"""

from .scoring_engine import ScoringEngine
from .sales_agent import SalesAgent

__all__ = [
    "ScoringEngine",
    "SalesAgent",
]
