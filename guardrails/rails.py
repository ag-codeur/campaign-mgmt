import re
from loguru import logger

# --- Categorised guardrail rules ---

GUARDRAIL_CATEGORIES = {
    "PII Detection": {
        "description": "Personally identifiable information must not appear in email content",
        "severity": "HIGH",
        "type": "pattern",
        "rules": [
            {"name": "Social Security Number", "pattern": r"\b\d{3}-\d{2}-\d{4}\b", "example": "123-45-6789"},
            {"name": "Credit Card Number", "pattern": r"\b\d{16}\b", "example": "1234567890123456"},
            {"name": "Phone Number (10-digit)", "pattern": r"\b\d{10}\b", "example": "5551234567"},
            {"name": "Email Address in body", "pattern": r"\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}\b", "example": "user@domain.com"},
            {"name": "IP Address", "pattern": r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b", "example": "192.168.1.1"},
            {"name": "Passport / ID Pattern", "pattern": r"\b[A-Z]{2}\d{6,9}\b", "example": "AB1234567"},
        ],
    },
    "Deceptive / Manipulative": {
        "description": "Phrases that mislead recipients or manipulate their decisions - high regulatory risk",
        "severity": "HIGH",
        "type": "phrase",
        "rules": [
            {"phrase": "guaranteed returns"}, {"phrase": "free money"},
            {"phrase": "you have been selected"}, {"phrase": "you've won"},
            {"phrase": "you have won"}, {"phrase": "congratulations, you've been chosen"},
        ],
    },
    "Urgency / Scarcity Tactics": {
        "description": "Artificial pressure phrases that may violate consumer protection regulations",
        "severity": "MEDIUM",
        "type": "phrase",
        "rules": [
            {"phrase": "act now"}, {"phrase": "limited time offer"},
            {"phrase": "only a few left"}, {"phrase": "expires soon"},
            {"phrase": "last chance"}, {"phrase": "don't miss out"},
        ],
    },
    "Financial Promises": {
        "description": "Unsubstantiated financial guarantees or investment claims",
        "severity": "HIGH",
        "type": "phrase",
        "rules": [
            {"phrase": "no risk"}, {"phrase": "risk-free"},
            {"phrase": "double your money"}, {"phrase": "risk free investment"},
            {"phrase": "100% guaranteed"},
        ],
    },
    "Spam Indicators": {
        "description": "Spam trigger words that harm deliverability and sender reputation",
        "severity": "MEDIUM",
        "type": "phrase",
        "rules": [
            {"phrase": "spam"}, {"phrase": "click here"},
            {"phrase": "buy now"}, {"phrase": "earn money"}, {"phrase": "get paid"},
        ],
    },
    "Health / Medical Claims": {
        "description": "Unverified medical or health benefit claims in email content",
        "severity": "HIGH",
        "type": "phrase",
        "rules": [
            {"phrase": "cures"}, {"phrase": "treats disease"},
            {"phrase": "prevents disease"}, {"phrase": "miracle cure"},
            {"phrase": "clinically proven"},
        ],
    },
}

# --- Internal helpers derived from config ---

_PII_PATTERNS = [
    (r["pattern"], r["name"])
    for r in GUARDRAIL_CATEGORIES["PII Detection"]["rules"]
]

_BANNED_PHRASES = [
    (r["phrase"], cat_name)
    for cat_name, cat in GUARDRAIL_CATEGORIES.items()
    if cat["type"] == "phrase"
    for r in cat["rules"]
]

def _check_pii(text: str) -> list[str]:
    return [
        f"PII detected: {name}"
        for pattern, name in _PII_PATTERNS
        if re.search(pattern, text)
    ]

def _check_brand_safety(text: str) -> list[str]:
    tl = text.lower()
    return [
        f"Brand safety: '{phrase}' ({category})"
        for phrase, category in _BANNED_PHRASES
        if phrase in tl
    ]

def validate_email_content(email: dict) -> tuple[bool, list[str]]:
    """
    Validate a single email dict with keys 'subject' and 'body'.
    Returns (is_valid, list_of_issues).
    """
    issues: list[str] = []
    for field in (email.get("subject", ""), email.get("body", "")):
        issues += _check_pii(str(field))
        issues += _check_brand_safety(str(field))

    issues = list(dict.fromkeys(issues)) # deduplicate, preserve order
    if issues:
        logger.warning(f"[GUARDRAILS] Email violations: {issues}")
    return len(issues) == 0, issues

def validate_content(content: dict) -> tuple[bool, list[str]]:
    """
    Legacy wrapper - accepts a dict with an 'email' key.
    Kept for backward compatibility with existing imports.
    """
    email = content.get("email", {})
    return validate_email_content(email)

def get_guardrail_config() -> dict:
    """Return the full GUARDRAIL_CATEGORIES configuration dict."""
    return GUARDRAIL_CATEGORIES