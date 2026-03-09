"""
Creator Agent - generates personalised email content for each audience branch.

Supports A/B testing: when a branch contains 'variants' (list of ABVariant dicts)
the agent generates separate subject + body for EVERY variant so the executor can
send each group a distinct message and the analytics tab can compare results.

Parallel execution: supervisor calls run_creator_single() per branch in a
ThreadPoolExecutor so all branches (and their variants) are generated concurrently.
"""

import json
from langchain_groq import ChatGroq
from langchain.prompts import ChatPromptTemplate
from loguru import logger

from core.config import get_settings
from core.knowledge_base import query_kb
from guardrails.rails import validate_email_content

settings = get_settings()

_EMAIL_PROMPT = ChatPromptTemplate.from_messages([
    ("system", """You are an expert email marketing copywriter.
Write a compelling, personalised email for the specified audience segment.
Adapt tone, language, and cultural references to match the branch criteria.
Respond ONLY with valid JSON - no extra text:
{{
  "subject": "...",
  "body": "..."
}}
The body should be plain text with a clear greeting, value proposition,
call-to-action, and sign-off. Keep it concise (150-300 words). No HTML tags."""),
    ("human", """Campaign Strategy: {strategy}

Branch: {branch_name}
Language: {language}
Country: {country}
Age Category: {age_category}
Audience Notes / Query: {custom_query}
A/B Variant: {variant_hint}

Past Content Learnings:
{kb_context}""")
])

def run_creator(strategy: dict, branches: list) -> list:
    """
    Generate email content for every branch (and every A/B variant within each branch).
    Calls run_creator_single per branch - callers may parallelise this with a thread pool.
    """
    llm = ChatGroq(api_key=settings.groq_api_key, model="llama3-8b-8192", temperature=0.7)
    past = query_kb(f"campaign email content: {strategy.get('objective', '')}")
    kb_context = "\n".join(past) if past else "No past content found."

    results = []
    for branch in branches:
        result = run_creator_single(strategy, branch, llm=llm, kb_context=kb_context)
        results.append(result)
    
    logger.info(f"[CREATOR] Email content generated for {len(results)} branches")
    return results

def run_creator_single(
    strategy: dict,
    branch: dict,
    llm=None,
    kb_context: str | None = None,
) -> dict:
    """
    Generate content for a single branch (and all its A/B variants if present).
    """
    branch_id = branch.get("id")
    branch_name = branch.get("branch_name", "General")
    language = branch.get("language") or "English"
    country = branch.get("country") or "Global"
    age_category = branch.get("age_category") or "all"
    custom_query = branch.get("custom_query") or "None specified"
    ab_variants = branch.get("variants", [])

    base_result = {
        "branch_id": branch_id,
        "branch_name": branch_name,
        "subject": "",
        "body": "",
        "guardrail_warnings": None,
        "variants": [],
    }

    if llm is None:
        llm = ChatGroq(api_key=settings.groq_api_key, model="llama3-8b-8192", temperature=0.7)
    if kb_context is None:
        past = query_kb(f"campaign email content: {strategy.get('objective', '')}")
        kb_context = "\n".join(past) if past else "No past content found."

    def _generate_email(variant_hint: str = "No A/B test - single version") -> dict:
        try:
            resp = (_EMAIL_PROMPT | llm).invoke({
                "strategy": json.dumps(strategy),
                "branch_name": branch_name,
                "language": language,
                "country": country,
                "age_category": age_category,
                "custom_query": custom_query,
                "variant_hint": variant_hint,
                "kb_context": kb_context,
            })
            text = resp.content
            email = json.loads(text[text.find("{"):text.rfind("}") + 1])
            is_valid, issues = validate_email_content(email)
            if not is_valid:
                email["guardrail_warnings"] = issues
            return email
        except Exception as e:
            logger.error(f"[CREATOR] LLM error for '{branch_name}' ({variant_hint}): {e}")
            fallback = strategy.get("key_messages", ["Check out our latest update!"])[0]
            return {"subject": "A message for you", "body": fallback}

    if ab_variants:
        variant_results = []
        for v in ab_variants:
            variant_hint = (
                f"This is Variant {v.get('name')} of an A/B test. "
                f"({v.get('split_pct', 100)}% of recipients). "
                "Create a meaningfully different angle compared to other variants."
            )
            email = _generate_email(variant_hint)
            variant_results.append({
                "id": v.get("id"),
                "name": v.get("name"),
                "split_pct": v.get("split_pct", 100),
                "subject": email.get("subject", ""),
                "body": email.get("body", ""),
                "guardrail_warnings": email.get("guardrail_warnings"),
            })
        
        base_result["variants"] = variant_results
        base_result["subject"] = variant_results[0]["subject"]
        base_result["body"] = variant_results[0]["body"]
    else:
        email = _generate_email()
        base_result["subject"] = email.get("subject", "")
        base_result["body"] = email.get("body", "")
        base_result["guardrail_warnings"] = email.get("guardrail_warnings")

    logger.info(f"[CREATOR] Email ready for branch '{branch_name}'")
    return base_result