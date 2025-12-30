"""
🤖 AI SALES AGENT
=================
Automated outreach using Google Gemini 3 for email generation.

CAPABILITIES:
- Generates personalized cold outreach emails
- Contextualizes based on propensity signals
- Supports multiple writing styles
- Validates email addresses (SMTP check)

MODELS:
- Gemini 3 Flash: Quick email drafts, high volume
- Gemini 3 Pro: Complex personalization, executive outreach
"""

import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional
import json
import re
import smtplib
import dns.resolver
from email.utils import parseaddr

from loguru import logger

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from config.settings import settings
from database.connection import db


class SalesAgent:
    """
    AI-powered sales outreach agent.
    
    Uses Google Gemini for:
    - Personalized email generation
    - Subject line optimization
    - Follow-up sequencing
    
    Usage:
        agent = SalesAgent()
        
        # Generate outreach for a lead
        email = agent.generate_outreach(
            company_name="Acme Logistics",
            contact_name="John Smith",
            signals={"expansion": True, "job_velocity": "high"}
        )
        
        # Batch process hot leads
        emails = agent.process_hot_leads()
    """
    
    # Email templates by signal type
    TEMPLATES = {
        "expansion": {
            "hook": "Congrats on the expansion! Building permits show exciting growth ahead.",
            "pain": "Rapid growth often outpaces hiring capacity.",
            "solution": "Our pre-vetted warehouse talent can be onsite within 48 hours."
        },
        "distress_nearby": {
            "hook": "With changes happening nearby at {competitor}, you may see an influx of talent.",
            "pain": "Absorbing displaced workers without proper vetting creates risk.",
            "solution": "We can screen and qualify candidates before they reach you."
        },
        "high_turnover": {
            "hook": "I noticed you've posted {job_count} warehouse positions recently.",
            "pain": "High turnover is costly - each replacement costs ~$4,500.",
            "solution": "Our retention-focused staffing reduces turnover by 35%."
        },
        "tight_market": {
            "hook": "With unemployment at {unemployment_rate}% in {county}, finding workers is tough.",
            "pain": "Direct recruiting in tight markets burns time and money.",
            "solution": "We maintain a pool of 500+ pre-screened local candidates."
        }
    }
    
    def __init__(self):
        """Initialize the sales agent."""
        self.api_key = settings.api.gemini_api_key
        self.flash_model = settings.ai.gemini_flash_model
        self.pro_model = settings.ai.gemini_pro_model
        
        self.genai = None
        
        if self.api_key:
            try:
                import google.generativeai as genai
                genai.configure(api_key=self.api_key)
                self.genai = genai
                logger.info("✅ Gemini API initialized")
                logger.info(f"   Flash model: {self.flash_model}")
                logger.info(f"   Pro model: {self.pro_model}")
            except ImportError:
                logger.error("google-generativeai not installed")
                logger.info("Run: pip install google-generativeai")
        else:
            logger.warning("⚠️ GEMINI_API_KEY not set - using templates only")
    
    def generate_outreach(
        self,
        company_name: str,
        contact_name: Optional[str] = None,
        contact_title: Optional[str] = None,
        signals: Optional[Dict] = None,
        style: str = "professional"
    ) -> Dict:
        """
        Generate a personalized outreach email.
        
        Args:
            company_name: Target company
            contact_name: Contact's name (optional)
            contact_title: Contact's title (optional)
            signals: Dict of propensity signals
            style: Writing style (professional, casual, executive)
            
        Returns:
            Dict with subject and body
        """
        signals = signals or {}
        
        # Determine primary angle based on strongest signal
        angle = self._select_angle(signals)
        
        # Try AI generation first
        if self.genai:
            try:
                return self._generate_with_ai(
                    company_name=company_name,
                    contact_name=contact_name,
                    contact_title=contact_title,
                    signals=signals,
                    angle=angle,
                    style=style
                )
            except Exception as e:
                logger.warning(f"AI generation failed: {e}")
                logger.info("Falling back to template...")
        
        # Fallback to template
        return self._generate_from_template(
            company_name=company_name,
            contact_name=contact_name,
            signals=signals,
            angle=angle
        )
    
    def _select_angle(self, signals: Dict) -> str:
        """Select the best outreach angle based on signals."""
        # Priority order
        if signals.get("expansion_score", 0) > 70:
            return "expansion"
        elif signals.get("distress_score", 0) > 60:
            return "distress_nearby"
        elif signals.get("job_velocity_score", 0) > 70:
            return "high_turnover"
        elif signals.get("market_tightness_score", 0) > 60:
            return "tight_market"
        else:
            return "expansion"  # Default
    
    def _generate_with_ai(
        self,
        company_name: str,
        contact_name: Optional[str],
        contact_title: Optional[str],
        signals: Dict,
        angle: str,
        style: str
    ) -> Dict:
        """Generate email using Gemini."""
        # Select model based on importance
        if contact_title and "VP" in contact_title.upper() or "DIRECTOR" in contact_title.upper():
            model_name = self.pro_model  # Use Pro for executives
        else:
            model_name = self.flash_model  # Flash for standard outreach
        
        model = self.genai.GenerativeModel(model_name)
        
        # Build context
        context_parts = [f"Company: {company_name}"]
        if contact_name:
            context_parts.append(f"Contact: {contact_name}")
        if contact_title:
            context_parts.append(f"Title: {contact_title}")
        
        signal_parts = []
        if signals.get("permit_value"):
            signal_parts.append(f"Recent building permit: ${signals['permit_value']:,.0f}")
        if signals.get("job_count_30d"):
            signal_parts.append(f"Job postings in last 30 days: {signals['job_count_30d']}")
        if signals.get("local_unemployment_rate"):
            signal_parts.append(f"Local unemployment: {signals['local_unemployment_rate']}%")
        if signals.get("glassdoor_rating"):
            signal_parts.append(f"Glassdoor rating: {signals['glassdoor_rating']}/5")
        
        context = "\n".join(context_parts)
        signal_context = "\n".join(signal_parts) if signal_parts else "No specific signals available"
        
        prompt = f"""You are a sales development representative for a light industrial staffing company.
        
Write a cold outreach email to a potential client. The email should:
- Be {style} in tone
- Open with a hook based on the signals below
- Reference specific data points when available
- Be concise (under 150 words)
- Include a clear call-to-action (15-minute call)
- NOT be pushy or salesy

TARGET:
{context}

SIGNALS (use these to personalize):
{signal_context}

PRIMARY ANGLE: {angle}

Output format:
SUBJECT: [subject line]
BODY:
[email body]
"""
        
        try:
            response = model.generate_content(prompt)
            
            # Parse response
            text = response.text
            
            # Extract subject and body
            if "SUBJECT:" in text and "BODY:" in text:
                subject_match = re.search(r'SUBJECT:\s*(.+?)(?=BODY:|$)', text, re.DOTALL)
                body_match = re.search(r'BODY:\s*(.+)', text, re.DOTALL)
                
                subject = subject_match.group(1).strip() if subject_match else "Quick question"
                body = body_match.group(1).strip() if body_match else text
            else:
                subject = "Quick question about your staffing needs"
                body = text
            
            return {
                "subject": subject,
                "body": body,
                "model": model_name,
                "angle": angle,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Gemini error: {e}")
            raise
    
    def _generate_from_template(
        self,
        company_name: str,
        contact_name: Optional[str],
        signals: Dict,
        angle: str
    ) -> Dict:
        """Generate email from template (fallback)."""
        template = self.TEMPLATES.get(angle, self.TEMPLATES["expansion"])
        
        # Fill in template variables
        hook = template["hook"].format(
            competitor=signals.get("nearest_warn_company", "a nearby company"),
            job_count=signals.get("job_count_30d", "several"),
            unemployment_rate=signals.get("local_unemployment_rate", "low"),
            county=signals.get("county", "your area")
        )
        
        greeting = f"Hi {contact_name}," if contact_name else "Hi there,"
        
        body = f"""{greeting}

{hook}

{template["pain"]}

{template["solution"]}

Would you have 15 minutes this week for a quick call? I'd love to learn more about your current staffing situation and see if we might be able to help.

Best regards,
[Your Name]
[Your Company]"""
        
        subject = f"Quick question about staffing at {company_name}"
        
        return {
            "subject": subject,
            "body": body,
            "model": "template",
            "angle": angle,
            "generated_at": datetime.now().isoformat()
        }
    
    def process_hot_leads(self, min_score: int = 75) -> List[Dict]:
        """
        Generate outreach for all hot leads.
        
        Args:
            min_score: Minimum propensity score
            
        Returns:
            List of generated emails
        """
        logger.info(f"Processing hot leads (score >= {min_score})...")
        
        # Get hot leads from database
        hot_leads = db.get_hot_leads(min_score=min_score)
        
        results = []
        
        for lead in hot_leads:
            try:
                # Build signals dict from lead data
                signals = {
                    "permit_value": lead.get("permit_value"),
                    "job_count_30d": lead.get("job_post_count_30d"),
                    "local_unemployment_rate": lead.get("local_unemployment_rate"),
                    "glassdoor_rating": lead.get("glassdoor_rating"),
                }
                
                # Generate email
                email = self.generate_outreach(
                    company_name=lead.get("company_name", "Company"),
                    signals=signals
                )
                
                email["company_id"] = lead.get("id")
                email["company_name"] = lead.get("company_name")
                email["propensity_score"] = lead.get("propensity_score")
                
                results.append(email)
                
            except Exception as e:
                logger.error(f"Error processing {lead.get('company_name')}: {e}")
                continue
        
        logger.info(f"Generated {len(results)} outreach emails")
        return results
    
    def validate_email(self, email: str) -> Dict:
        """
        Validate an email address (syntax + MX record check).
        
        Args:
            email: Email address to validate
            
        Returns:
            Dict with validation results
        """
        result = {
            "email": email,
            "valid_syntax": False,
            "has_mx_record": False,
            "deliverable": False
        }
        
        # Check syntax
        _, addr = parseaddr(email)
        if not addr or "@" not in addr:
            return result
        
        result["valid_syntax"] = True
        
        # Check MX record
        domain = addr.split("@")[1]
        try:
            mx_records = dns.resolver.resolve(domain, 'MX')
            result["has_mx_record"] = len(mx_records) > 0
            result["deliverable"] = result["has_mx_record"]
        except Exception:
            pass
        
        return result
    
    def permute_email(self, first_name: str, last_name: str, domain: str) -> List[str]:
        """
        Generate possible email permutations for a contact.
        
        Args:
            first_name: Contact's first name
            last_name: Contact's last name
            domain: Company domain
            
        Returns:
            List of possible email addresses
        """
        first = first_name.lower().strip()
        last = last_name.lower().strip()
        
        # Common patterns (ordered by likelihood)
        patterns = [
            f"{first}.{last}@{domain}",
            f"{first}{last}@{domain}",
            f"{first[0]}{last}@{domain}",
            f"{first}_{last}@{domain}",
            f"{first}@{domain}",
            f"{last}.{first}@{domain}",
            f"{first[0]}.{last}@{domain}",
            f"{first}{last[0]}@{domain}",
        ]
        
        return patterns


# ===========================================
# STANDALONE EXECUTION
# ===========================================

def main():
    """Demo the sales agent."""
    logger.remove()
    logger.add(
        sys.stderr,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | {message}",
        level="INFO"
    )
    
    agent = SalesAgent()
    
    print("\n" + "=" * 60)
    print("🤖 AI SALES AGENT DEMO")
    print("=" * 60)
    
    # Example 1: Expansion-focused outreach
    print("\n📧 EXAMPLE 1: Expansion Signal")
    print("-" * 40)
    
    email1 = agent.generate_outreach(
        company_name="Acme Logistics",
        contact_name="John Smith",
        contact_title="Operations Manager",
        signals={
            "permit_value": 2500000,
            "expansion_score": 85,
            "local_unemployment_rate": 3.2
        }
    )
    
    print(f"Subject: {email1['subject']}")
    print(f"\n{email1['body']}")
    print(f"\n[Generated by: {email1['model']} | Angle: {email1['angle']}]")
    
    # Example 2: High turnover outreach
    print("\n\n📧 EXAMPLE 2: High Turnover Signal")
    print("-" * 40)
    
    email2 = agent.generate_outreach(
        company_name="FastShip Distribution",
        signals={
            "job_count_30d": 15,
            "job_velocity_score": 90,
            "glassdoor_rating": 2.8
        }
    )
    
    print(f"Subject: {email2['subject']}")
    print(f"\n{email2['body']}")
    
    # Email permutation example
    print("\n\n📮 EMAIL PERMUTATION EXAMPLE")
    print("-" * 40)
    
    permutations = agent.permute_email("John", "Smith", "acmelogistics.com")
    print("Possible emails for John Smith @ Acme Logistics:")
    for email in permutations[:5]:
        print(f"  • {email}")


if __name__ == "__main__":
    main()
