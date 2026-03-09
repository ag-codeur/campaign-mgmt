import json
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate
from loguru import logger

from core.config import get_settings
from core.knowledge_base import query_kb

settings = get_settings()

# --- Prompts ---

_STRATEGY_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an email marketing campaign planning expert.
Create a focused email campaign strategy based on the goal, audience, and audience branches.
This is an EMAIL-ONLY platform - no SMS, push notifications, or social media.
Respond ONLY with valid JSON - no extra text:
{{
  "campaign_name": "...",
  "objective": "...",
  "email_approach": "...",
  "target_segments": ["..."],
  "timeline": "...",
  "key_messages": ["..."],
  "success_metrics": ["open_rate", "click_rate", "conversion_rate"]
}}"""),
    ("human", """Goal: {goal}
Audience: {audience}
Audience Branches: {branches}
Past Campaign Learnings:
{kb_context}"""),
])

_BRANCH_SUGGESTION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an audience segmentation specialist for email marketing.
Suggest 2-4 distinct audience branches/segments for this campaign.
Each branch should have unique messaging tailored to language, geography, and age.
Respond ONLY with valid JSON array - no extra text:
[
  {{
    "branch_name": "...",
    "language": "English|French|Spanish|German|Portuguese|Italian|Dutch|Japanese|Chinese|Arabic|Hindi",
    "country": "US|GB|FR|DE|ES|IT|CA|AU|IN|JP|BR|MX|NL|SG",
    "age_category": "18-25|26-35|36-50|50+|all",
    "custom_query": "email_opt_in = true AND ...",
    "rationale": "..."
  }}
]"""),
    ("human", """Campaign Goal: {goal}
Target Audience Description: {audience}"""),
])

_QUERY_SUGGESTION_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are a database query expert for audience segmentation in email marketing.
Generate a SQL-like audience filter query for the given segment criteria.
The query filters users from a table with columns:
language (string), country (string), age (integer),
customer_tier ('free'|'standard'|'premium'), signup_date (date)
Respond ONLY with the query string - no extra text, no JSON wrapper.
Example: email_opt_in = true AND country = 'US' AND age BETWEEN 26 AND 35"""),
    ("human", """Campaign Goal: {goal}
Audience: {audience}
Branch Criteria:
Language: {language}
Country: {country}
Age Category: {age_category}"""),
])

# --- Planner ---

def run_planner(goal: str, audience: str, branches: list) -> dict:
    """Generate email campaign strategy informed by audience branches and past KB learnings."""
    llm = ChatGroq(api_key=settings.groq_api_key, model="llama-3.1-8b-instant", temperature=0.3)

    past = query_kb(f"campaign goal: {goal}")
    kb_context = "\n".join(past) if past else "No past campaigns found."

    branch_summary = ", ".join(
        [f"{b.get('branch_name', 'Segment')} / {b.get('language', '')} / {b.get('country', '')} / age: {b.get('age_category', 'all')}"
         for b in branches]
    ) if branches else "Not yet defined"

    try:
        response = (_STRATEGY_PROMPT | llm).invoke({
            "goal": goal,
            "audience": audience,
            "branches": branch_summary,
            "kb_context": kb_context,
        })
        text = response.content
        strategy = json.loads(text[text.find("{"):text.rfind("}") + 1])
        logger.info(f"[PLANNER] Strategy ready: {strategy.get('campaign_name')}")
        return strategy
    except Exception as e:
        logger.error(f"[PLANNER] Parse error: {e}")
        return {
            "campaign_name": "Email Campaign",
            "objective": goal,
            "email_approach": "Personalised email per segment",
            "target_segments": [audience],
            "timeline": "2 weeks",
            "key_messages": [goal],
            "success_metrics": ["open_rate", "click_rate", "conversion_rate"],
        }

def suggest_audience_branches(goal: str, audience: str) -> list:
    """AI suggests 2-4 audience branches based on campaign goal and audience description."""
    llm = ChatGroq(api_key=settings.groq_api_key, model="llama-3.1-8b-instant", temperature=0.5)

    try:
        response = (_BRANCH_SUGGESTION_PROMPT | llm).invoke({
            "goal": goal,
            "audience": audience,
        })
        text = response.content
        start, end = text.find("["), text.rfind("]") + 1
        if start >= 0 and end > start:
            branches = json.loads(text[start:end])
            logger.info(f"[PLANNER] Suggested {len(branches)} audience branches")
            return branches
    except Exception as e:
        logger.error(f"[PLANNER] Branch suggestion error: {e}")

    # Fallback
    return [
        {
            "branch_name": "General Audience",
            "language": "English", "country": "US",
            "age_category": "all",
            "custom_query": "email_opt_in = true",
            "rationale": "Broad default segment",
        },
    ]

def suggest_audience_query(
    goal: str,
    audience: str,
    language: str = None,
    country: str = None,
    age_category: str = None,
) -> str:
    """AI suggests an editable SQL-like audience filter query for a specific branch."""
    llm = ChatGroq(api_key=settings.groq_api_key, model="llama-3.1-8b-instant", temperature=0.2)

    try:
        response = (_QUERY_SUGGESTION_PROMPT | llm).invoke({
            "goal": goal,
            "audience": audience,
            "language": language or "any",
            "country": country or "any",
            "age_category": age_category or "all ages",
        })
        query = response.content.strip()
        logger.info(f"[PLANNER] Suggested audience query: {query[:60]}...")
        return query
    except Exception as e:
        logger.error(f"[PLANNER] Query suggestion error: {e}")

    # Fallback - build a simple query from criteria
    filters = ["email_opt_in = true"]
    if language:
        filters.append(f"language = '{language}'")
    if country:
        filters.append(f"country = '{country}'")
    if age_category and age_category != "all":
        parts = age_category.split("-")
        if len(parts) == 2:
            filters.append(f"age BETWEEN {parts[0]} AND {parts[1]}")
        elif age_category.endswith("+"):
            filters.append(f"age >= {age_category[:-1]}")
    return " AND ".join(filters)