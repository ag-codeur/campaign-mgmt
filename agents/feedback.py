import json
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate
from loguru import logger

from core.config import get_settings
from core.knowledge_base import add_to_kb

settings = get_settings()

_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an email campaign performance analyst.
Evaluate the campaign results and extract actionable learnings for future email campaigns.
Respond ONLY with valid JSON - no extra text:
{{
  "performance_score": <integer 1-10>,
  "what_worked": ["..."],
  "what_didnt": ["..."],
  "key_learnings": ["..."],
  "recommendations": ["..."],
  "best_performing_segment": "...",
  "email_insights": ["subject line tips", "body copy tips", "timing tips"]
}}"""),
    ("human", """Campaign Strategy: {strategy}
Audience Branches: {branches}
Execution Results: {results}
Human Rating: {rating}""")
])

def run_feedback(
    campaign_id: str,
    strategy: dict,
    branches: list,
    execution_results: dict,
    rating: str,
) -> dict:
    llm = ChatGroq(api_key=settings.groq_api_key, model="llama3-8b-8192", temperature=0.2)

    branch_summary = [
        {
            "name": b.get("branch_name"),
            "language": b.get("language"),
            "country": b.get("country"),
            "age_category": b.get("age_category"),
        }
        for b in branches
    ]

    response = (_PROMPT | llm).invoke({
        "strategy": json.dumps(strategy),
        "branches": json.dumps(branch_summary),
        "results": json.dumps(execution_results),
        "rating": rating,
    })

    try:
        text = response.content
        evaluation = json.loads(text[text.find("{"):text.rfind("}") + 1])

        # Persist learnings into the knowledge base for future campaigns
        add_to_kb(
            doc_id=f"{campaign_id}_eval",
            content={
                "goal": strategy.get("objective", ""),
                "branches": branch_summary,
                "results": execution_results,
                "evaluation": evaluation,
            },
            metadata={"campaign_id": campaign_id, "rating": rating},
        )

        logger.info(f"[FEEDBACK] Knowledge base updated for campaign {campaign_id}")
        return evaluation

    except Exception as e:
        logger.error(f"[FEEDBACK] Parse error: {e}")
        return {
            "performance_score": 5,
            "what_worked": [],
            "what_didnt": [],
            "key_learnings": ["Email campaign completed"],
            "recommendations": [],
            "best_performing_segment": "",
            "email_insights": [],
        }