"""
🎯 PROPENSITY SCORING ENGINE
============================
Combines all 7 pipeline signals into a single propensity score.

HOW IT WORKS:
1. Collects scores from all pipelines
2. Applies configurable weights
3. Multiplies by macro modifier
4. Produces final score (0-100) with tier classification

SCORE INTERPRETATION:
- 80-100: 🔥 HOT LEAD - Contact immediately
- 60-79:  👍 WARM LEAD - Add to outreach list
- 40-59:  🤔 COOL LEAD - Monitor for changes  
- 0-39:   ❄️ COLD - Not ready yet
"""

import sys
from pathlib import Path
from datetime import datetime, date
from typing import Dict, List, Optional
from dataclasses import dataclass

from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings
from database.connection import db


@dataclass
class SignalScores:
    """Container for all signal scores."""
    expansion: float = 0.0      # Pipeline 1: Permits
    distress: float = 0.0       # Pipeline 2: WARN
    macro_modifier: float = 1.0 # Pipeline 3: Economic
    sentiment: float = 0.0      # Pipeline 4: Glassdoor
    job_velocity: float = 0.0   # Pipeline 5: Jobs
    turnover: float = 0.0       # Pipeline 6: Inventory
    market_tightness: float = 0.0  # Pipeline 7: Labor


class ScoringEngine:
    """
    Calculates propensity scores for companies.
    
    The formula:
    
    base_score = (
        expansion * W1 +
        distress * W2 +
        job_velocity * W3 +
        sentiment * W4 +
        market_tightness * W5 +
        turnover * W6
    )
    
    final_score = base_score * macro_modifier
    
    Where W1-W6 are configurable weights that sum to 1.0
    
    Usage:
        engine = ScoringEngine()
        
        # Score a single company
        result = engine.score_company(company_id="uuid-here")
        
        # Score all companies in database
        results = engine.score_all()
    """
    
    def __init__(self):
        """Initialize with configured weights."""
        self.weights = settings.weights
        
        # Validate weights sum to 1.0
        if not self.weights.validate_weights():
            logger.warning("⚠️ Scoring weights don't sum to 1.0!")
            logger.info("Check WEIGHT_* settings in .env")
        
        logger.info("ScoringEngine initialized")
        logger.info(f"  Weights: E={self.weights.expansion}, D={self.weights.distress}, "
                   f"J={self.weights.job_velocity}, S={self.weights.sentiment}, "
                   f"M={self.weights.market_tightness}, T={self.weights.macro}")
    
    def calculate_score(
        self, 
        signals: SignalScores
    ) -> Dict:
        """
        Calculate the final propensity score from individual signals.
        
        Args:
            signals: SignalScores object with all component scores
            
        Returns:
            Dict with final score and breakdown
        """
        # Calculate weighted base score
        base_score = (
            signals.expansion * self.weights.expansion +
            signals.distress * self.weights.distress +
            signals.job_velocity * self.weights.job_velocity +
            signals.sentiment * self.weights.sentiment +
            signals.market_tightness * self.weights.market_tightness +
            signals.turnover * self.weights.macro
        )
        
        # Apply macro modifier
        final_score = base_score * signals.macro_modifier
        
        # Clamp to 0-100
        final_score = min(max(final_score, 0), 100)
        
        # Classify tier
        tier = self._classify_tier(final_score)
        
        return {
            "propensity_score": round(final_score, 1),
            "score_tier": tier,
            "base_score": round(base_score, 1),
            "macro_modifier": signals.macro_modifier,
            "breakdown": {
                "expansion": round(signals.expansion * self.weights.expansion, 1),
                "distress": round(signals.distress * self.weights.distress, 1),
                "job_velocity": round(signals.job_velocity * self.weights.job_velocity, 1),
                "sentiment": round(signals.sentiment * self.weights.sentiment, 1),
                "market_tightness": round(signals.market_tightness * self.weights.market_tightness, 1),
                "turnover": round(signals.turnover * self.weights.macro, 1),
            }
        }
    
    @staticmethod
    def _classify_tier(score: float) -> str:
        """Classify score into tier."""
        if score >= 80:
            return "hot"
        elif score >= 60:
            return "warm"
        elif score >= 40:
            return "cool"
        else:
            return "cold"
    
    def score_company(self, company_id: str) -> Optional[Dict]:
        """
        Calculate propensity score for a specific company.
        
        Fetches the latest signals from the database and calculates score.
        
        Args:
            company_id: UUID of the company
            
        Returns:
            Dict with score and metadata
        """
        # Get company info
        company = db.get_by_id("company_master", company_id)
        if not company:
            logger.warning(f"Company not found: {company_id}")
            return None
        
        # Get latest signal history
        signals_list = db.query(
            "signal_history",
            filters={"company_id": company_id},
            order_by="-record_date",
            limit=1
        )
        
        if signals_list:
            signals_data = signals_list[0]
        else:
            signals_data = {}
        
        # Build SignalScores object
        signals = SignalScores(
            expansion=signals_data.get("expansion_score", 0) or 0,
            distress=signals_data.get("distress_score", 0) or 0,
            sentiment=signals_data.get("sentiment_score", 0) or 0,
            job_velocity=signals_data.get("job_velocity_score", 0) or 0,
            turnover=signals_data.get("turnover_score", 0) or 0,
            market_tightness=signals_data.get("market_tightness_score", 0) or 0,
            macro_modifier=signals_data.get("macro_modifier", 1.0) or 1.0
        )
        
        # Calculate score
        result = self.calculate_score(signals)
        
        # Add company info
        result["company_id"] = company_id
        result["company_name"] = company.get("company_name")
        result["city"] = company.get("city")
        result["state"] = company.get("state")
        result["scored_at"] = datetime.now().isoformat()
        
        # Save to database
        self._save_score(company_id, result, signals)
        
        return result
    
    def _save_score(
        self, 
        company_id: str, 
        result: Dict, 
        signals: SignalScores
    ):
        """Save calculated score to signal_history."""
        try:
            db.save_signal_history(
                company_id=company_id,
                signals={
                    "propensity_score": int(result["propensity_score"]),
                    "score_tier": result["score_tier"],
                    "expansion_score": signals.expansion,
                    "distress_score": signals.distress,
                    "sentiment_score": signals.sentiment,
                    "job_velocity_score": signals.job_velocity,
                    "turnover_score": signals.turnover,
                    "market_tightness_score": signals.market_tightness,
                    "macro_modifier": signals.macro_modifier,
                    "record_date": date.today().isoformat()
                }
            )
        except Exception as e:
            logger.error(f"Error saving score: {e}")
    
    def score_all(self, limit: int = 100) -> List[Dict]:
        """
        Score all companies in the database.
        
        Args:
            limit: Maximum companies to score
            
        Returns:
            List of scored companies
        """
        logger.info("Scoring all companies...")
        
        # Get all companies
        companies = db.query("company_master", limit=limit)
        
        results = []
        for company in companies:
            try:
                result = self.score_company(company["id"])
                if result:
                    results.append(result)
            except Exception as e:
                logger.error(f"Error scoring {company.get('company_name')}: {e}")
                continue
        
        # Sort by score
        results.sort(key=lambda x: x["propensity_score"], reverse=True)
        
        logger.info(f"Scored {len(results)} companies")
        
        return results
    
    def get_hot_leads(self, min_score: int = 75) -> List[Dict]:
        """
        Get all companies above the hot lead threshold.
        
        Args:
            min_score: Minimum propensity score
            
        Returns:
            List of hot lead companies
        """
        # Score all first
        all_scored = self.score_all()
        
        # Filter to hot leads
        hot_leads = [r for r in all_scored if r["propensity_score"] >= min_score]
        
        logger.info(f"Found {len(hot_leads)} hot leads (score >= {min_score})")
        
        return hot_leads
    
    def explain_score(self, result: Dict) -> str:
        """
        Generate a human-readable explanation of a score.
        
        Args:
            result: Score result from score_company()
            
        Returns:
            Formatted explanation string
        """
        breakdown = result.get("breakdown", {})
        
        # Sort factors by contribution
        factors = sorted(
            breakdown.items(),
            key=lambda x: x[1],
            reverse=True
        )
        
        lines = [
            f"📊 PROPENSITY SCORE: {result['propensity_score']}/100 ({result['score_tier'].upper()})",
            f"",
            f"Company: {result.get('company_name', 'Unknown')}",
            f"Location: {result.get('city', '')}, {result.get('state', '')}",
            f"",
            f"📈 Score Breakdown:",
        ]
        
        emoji_map = {
            "expansion": "🏗️",
            "distress": "⚠️",
            "job_velocity": "💼",
            "sentiment": "⭐",
            "market_tightness": "📍",
            "turnover": "📦"
        }
        
        for factor, contribution in factors:
            emoji = emoji_map.get(factor, "•")
            lines.append(f"  {emoji} {factor.replace('_', ' ').title()}: +{contribution:.1f}")
        
        lines.append(f"")
        lines.append(f"🌐 Macro Modifier: {result.get('macro_modifier', 1.0)}x")
        lines.append(f"📅 Scored: {result.get('scored_at', 'Unknown')[:10]}")
        
        return "\n".join(lines)


# ===========================================
# STANDALONE EXECUTION
# ===========================================

def main():
    """Demo the scoring engine."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO"
    )
    
    engine = ScoringEngine()
    
    print("\n" + "=" * 60)
    print("🎯 PROPENSITY SCORING ENGINE DEMO")
    print("=" * 60)
    
    # Example scoring with mock signals
    print("\n📊 Example Score Calculations:\n")
    
    # Example 1: Hot lead (company expanding with tight labor market)
    hot_signals = SignalScores(
        expansion=85,      # New warehouse permit
        distress=70,       # Competitor nearby closing
        job_velocity=80,   # Posting lots of jobs
        sentiment=60,      # Average employee sentiment
        market_tightness=75,  # Tight labor market
        turnover=65,       # Good inventory turnover
        macro_modifier=1.1  # Economy expanding
    )
    
    hot_result = engine.calculate_score(hot_signals)
    print("🔥 HOT LEAD EXAMPLE:")
    print(f"   Score: {hot_result['propensity_score']}/100 ({hot_result['score_tier'].upper()})")
    print(f"   Breakdown: {hot_result['breakdown']}")
    
    # Example 2: Cold lead (no expansion signals)
    cold_signals = SignalScores(
        expansion=10,
        distress=20,
        job_velocity=15,
        sentiment=70,      # Happy employees = low turnover
        market_tightness=30,  # Lots of available workers
        turnover=40,
        macro_modifier=0.95
    )
    
    cold_result = engine.calculate_score(cold_signals)
    print("\n❄️ COLD LEAD EXAMPLE:")
    print(f"   Score: {cold_result['propensity_score']}/100 ({cold_result['score_tier'].upper()})")
    print(f"   Breakdown: {cold_result['breakdown']}")
    
    # Show weight configuration
    print("\n⚖️ CURRENT WEIGHT CONFIGURATION:")
    print(f"   Expansion:        {settings.weights.expansion * 100:.0f}%")
    print(f"   Distress:         {settings.weights.distress * 100:.0f}%")
    print(f"   Job Velocity:     {settings.weights.job_velocity * 100:.0f}%")
    print(f"   Sentiment:        {settings.weights.sentiment * 100:.0f}%")
    print(f"   Market Tightness: {settings.weights.market_tightness * 100:.0f}%")
    print(f"   Macro:            {settings.weights.macro * 100:.0f}%")


if __name__ == "__main__":
    main()
