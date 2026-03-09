from sqlalchemy import (
    create_engine, Column, String, JSON, DateTime, Text, Index, Boolean, Float
)
from sqlalchemy.orm import declarative_base, sessionmaker
from datetime import datetime
import uuid
import hashlib
import secrets

from core.config import get_settings

settings = get_settings()

_connect_args = {"check_same_thread": False} if "sqlite" in settings.database_url else {}
engine = create_engine(settings.database_url, connect_args=_connect_args)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# --- Platform users (campaign creators / managers) ---

class User(Base):
    """Platform user with manager hierarchy and RBAC fields."""
    
    __tablename__ = "users"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    name = Column(String, nullable=False)
    email = Column(String, unique=True, nullable=False)
    manager_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    # Auth fields (added via migration for existing DBs)
    hashed_password = Column(String, nullable=False, default="")
    role = Column(String, nullable=False, default="campaigner") # admin|leader|approver|campaigner
    business_unit = Column(String, nullable=True)              # BU1|BU2|None
    must_change_password = Column(Boolean, default=True)

# --- Email recipients / contact List ---

class Recipient(Base):
    """
    Stores opt-in/unsubscribe state plus segmentation attributes so the executor
    can filter which recipients match a branch's targeting criteria and enforce
    "one email per person per campaign" deduplication.
    """
    
    __tablename__ = "recipients"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    email = Column(String, unique=True, nullable=False)
    first_name = Column(String, nullable=True)
    last_name = Column(String, nullable=True)

    # Segmentation attributes - matched against AudienceBranch targeting criteria
    language = Column(String, nullable=True)    # preferred language (e.g. "Spanish")
    country = Column(String, nullable=True)     # country code (e.g. "ES")
    age_category = Column(String, nullable=True)# "18-25"|"26-35"|"36-50"|"50+"

    # Opt-out state
    is_unsubscribed = Column(Boolean, default=False)
    unsubscribed_at = Column(DateTime, nullable=True)
    # Secure random token embedded in unsubscribe links to prevent CSRF
    unsubscribe_token = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

Index("ix_recipients_email", Recipient.email)
Index("ix_recipients_lang_country", Recipient.language, Recipient.country)

# --- Campaign, branches, A/B variants ---

class AudienceBranch(Base):
    """
    One audience segment within a campaign.
    Default (non-A/B) email content is stored directly on this model.
    When A/B variants are configured, ABVariant rows hold per-variant content
    and AudienceBranch.email_subject/body serves as the control reference only.
    """
    
    __tablename__ = "audience_branches"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    campaign_id = Column(String, nullable=False)
    branch_name = Column(String, nullable=False)

    language = Column(String, nullable=True)
    country = Column(String, nullable=True)
    age_category = Column(String, nullable=True)
    custom_query = Column(Text, nullable=True)

    email_subject = Column(Text, nullable=True)
    email_body = Column(Text, nullable=True)

    scheduled_at = Column(DateTime, nullable=True)
    status = Column(String, default="draft")
    sent_count = Column(String, nullable=True)

    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class ABVariant(Base):
    """
    An A/B message variant within an audience branch.
    
    Each branch can have 2+ variants (A, B, C ...). split_percentage values across
    all variants of a branch must sum to 100. The executor assigns recipients to
    variants proportionally then records which variant each recipient received so
    analytics can compare performance.

    Example (80/20 split):
        Variant A - split_percentage=80 - subject/body: promotional angle
        Variant B - split_percentage=20 - subject/body: informational angle
    """
    
    __tablename__ = "ab_variants"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    branch_id = Column(String, nullable=False)    # FK -> audience_branches.id
    campaign_id = Column(String, nullable=False)  # denormalised for fast queries
    variant_name = Column(String, nullable=False) # "A" | "B" | "Control" | ...
    split_percentage = Column(Float, nullable=False, default=100.0)

    # AI-generated (or manually set) email content for this variant
    email_subject = Column(Text, nullable=True)
    email_body = Column(Text, nullable=True)

    status = Column(String, default="draft")      # draft | ready
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

Index("ix_ab_variants_branch", ABVariant.branch_id)

class Campaign(Base):
    """
    Email-only campaign with full HITL workflow and A/B variant support.
    
    Status flow:
        draft -> submitted -> awaiting_manager_approval -> approved
            -> planning -> awaiting_plan_approval
            -> creating_content -> awaiting_content_approval
            -> scheduled -> executing -> awaiting_rating -> evaluating -> completed | failed
        (rejected at any HITL gate returns to draft)
    """
    
    __tablename__ = "campaigns"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    goal = Column(Text)
    audience = Column(Text)

    strategy = Column(JSON)
    execution_results = Column(JSON)
    evaluation = Column(JSON)
    rejection_feedback = Column(Text)
    rating = Column(String)

    created_by = Column(String, nullable=True)
    approved_by = Column(String, nullable=True)
    submitted_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    manager_rejection_reason = Column(Text, nullable=True)

    status = Column(String, default="draft")
    business_unit = Column(String, nullable=True) # stamped from creator's BU at creation
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

# --- Deduplication: one email per person per campaign ---

class RecipientCampaignSend(Base):
    """
    Tracks which recipients were assigned to which branch/variant for a campaign.
    Guarantees at most one email per recipient per campaign.
    
    Statuses:
        sent                - email delivered
        skipped_unsub       - recipient had unsubscribed before send
        skipped_duplicate   - recipient already assigned to another branch this campaign
    """
    
    __tablename__ = "recipient_campaign_sends"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    recipient_id = Column(String, nullable=False)
    campaign_id = Column(String, nullable=False)
    branch_id = Column(String, nullable=False)
    variant_id = Column(String, nullable=True)      # None if no A/B test on this branch
    status = Column(String, default="sent")
    sent_at = Column(DateTime, default=datetime.utcnow)

Index("ix_rcs_recipient_campaign", RecipientCampaignSend.recipient_id, RecipientCampaignSend.campaign_id)
Index("ix_rcs_campaign", RecipientCampaignSend.campaign_id)

# --- Person-level engagement events ---

class RecipientEvent(Base):
    """
    Per-person engagement event (open, click, unsubscribe).
    
    Granularity: recipient -> campaign -> branch -> variant -> event_type.
    Can be aggregated freely:
        -> by campaign      -> overall efficacy
        -> by branch        -> segment performance
        -> by variant       -> A/B winner determination
        -> by recipient     -> user-level journey
        -> by event_type    -> funnel analysis

    In demo mode these events are simulated by the executor after delivery.
    In production they are created by:
        * tracking pixel endpoint (/track/open/...) for opens
        * redirect endpoint (/track/click/...) for clicks
        * unsubscribe endpoint (/unsubscribe/...) for opt-outs
        * ESP webhook for bounces, spam reports, etc.
    """
    
    __tablename__ = "recipient_events"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    recipient_id = Column(String, nullable=False)   # FK -> recipients.id
    campaign_id = Column(String, nullable=False)
    branch_id = Column(String, nullable=False)
    variant_id = Column(String, nullable=True)      # FK -> ab_variants.id (None = no A/B)
    event_type = Column(String, nullable=False)     # open | click | unsubscribe
    link_url = Column(Text, nullable=True)          # click events only
    user_agent = Column(String, nullable=True)
    ip_hash = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

Index("ix_re_recipient", RecipientEvent.recipient_id)
Index("ix_re_campaign", RecipientEvent.campaign_id)
Index("ix_re_branch", RecipientEvent.branch_id)
Index("ix_re_variant", RecipientEvent.variant_id)
Index("ix_re_event_type", RecipientEvent.event_type)

# --- Legacy campaign-level engagement (kept for backwards compatibility) ---

class EmailEvent(Base):
    """
    Campaign-branch-level engagement (no recipient identity).
    Retained for backwards-compat with tracking pixel / click endpoints.
    New code should prefer RecipientEvent.
    """
    
    __tablename__ = "email_events"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    campaign_id = Column(String, nullable=False)
    branch_id = Column(String, nullable=False)
    event_type = Column(String, nullable=False)
    recipient_token = Column(String, nullable=True)
    link_url = Column(Text, nullable=True)
    user_agent = Column(String, nullable=True)
    ip_hash = Column(String, nullable=True)
    segment_label = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# --- Agent execution monitoring ---

class AgentRun(Base):
    """
    Records every agent execution for the monitoring dashboard.
    
    agent_type values:
        planner     - AI Planner generating campaign strategy
        creator     - AI Creator generating email content (per branch / variant)
        executor    - Executor sending emails
        feedback    - Feedback Agent evaluating results
        chat        - Chat Assistant serving a user query
        translate   - Translate Agent localising email content

    Allows the Agent Monitor tab to show:
        * Which agents are currently running
        * Which campaign / branch / variant each agent is working on
        * Execution duration and output summary
        * Visual pipeline showing campaign progression
    """
    
    __tablename__ = "agent_runs"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    campaign_id = Column(String, nullable=True)
    branch_id = Column(String, nullable=True)
    variant_id = Column(String, nullable=True)
    agent_type = Column(String, nullable=False)
    status = Column(String, default="running")      # running | completed | failed
    input_summary = Column(Text, nullable=True)
    output_summary = Column(Text, nullable=True)
    error_message = Column(Text, nullable=True)
    started_at = Column(DateTime, default=datetime.utcnow)
    ended_at = Column(DateTime, nullable=True)
    duration_ms = Column(String, nullable=True)

Index("ix_agent_runs_campaign", AgentRun.campaign_id)
Index("ix_agent_runs_status", AgentRun.status)

# --- Guardrail check results ---

class GuardrailCheck(Base):
    """
    Records the result of every guardrail validation run during content creation.
    One row per branch (non-A/B) or per variant (A/B test).
    """
    
    __tablename__ = "guardrail_checks"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    campaign_id = Column(String, nullable=True)
    branch_id = Column(String, nullable=True)
    variant_id = Column(String, nullable=True)
    passed = Column(Boolean, default=True)
    pii_issues = Column(JSON, default=list)
    brand_safety_issues = Column(JSON, default=list)
    total_issues = Column(String, default="0")
    checked_at = Column(DateTime, default=datetime.utcnow)

Index("ix_guardrail_checks_campaign", GuardrailCheck.campaign_id)
Index("ix_guardrail_checks_passed", GuardrailCheck.passed)

# --- Chat message history ---

class ChatMessage(Base):
    """Persists AI assistant chat history per user session."""
    
    __tablename__ = "chat_messages"

    id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    user_id = Column(String, nullable=True)
    role = Column(String, nullable=False)
    content = Column(Text, nullable=False)
    campaign_id = Column(String, nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)

# --- Auth session tokens ---

class AuthSession(Base):
    """Server-side session token for user authentication."""
    
    __tablename__ = "auth_sessions"

    id = Column(String, primary_key=True) # token = uuid4
    user_id = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    expires_at = Column(DateTime, nullable=False)
    is_revoked = Column(Boolean, default=False)

Index("ix_auth_sessions_user", AuthSession.user_id)

# --- DB init & demo seed ---

def _migrate_users_table(eng):
    """Add new auth columns to the users table if they don't exist yet."""
    with eng.connect() as conn:
        result = conn.execute(
            __import__("sqlalchemy").text("PRAGMA table_info(users)")
        )
        existing = {row[1] for row in result.fetchall()}
        new_cols = [
            ("hashed_password",     "VARCHAR DEFAULT ''"),
            ("role",                "VARCHAR DEFAULT 'campaigner'"),
            ("business_unit",       "VARCHAR"),
            ("must_change_password", "BOOLEAN DEFAULT 1"),
        ]
        with eng.begin() as conn:
            for col_name, col_def in new_cols:
                if col_name not in existing:
                    conn.execute(
                        __import__("sqlalchemy").text(f"ALTER TABLE users ADD COLUMN {col_name} {col_def}")
                    )

def _migrate_campaigns_table(eng):
    """Add business_unit column to campaigns table if it doesn't exist yet."""
    with eng.connect() as conn:
        result = conn.execute(
            __import__("sqlalchemy").text("PRAGMA table_info(campaigns)")
        )
        existing = {row[1] for row in result.fetchall()}
        if "business_unit" not in existing:
            with eng.begin() as conn:
                conn.execute(
                    __import__("sqlalchemy").text("ALTER TABLE campaigns ADD COLUMN business_unit VARCHAR")
                )

def init_db():
    # Create all tables first, then run migrations to add new columns to existing tables
    Base.metadata.create_all(bind=engine)
    _migrate_users_table(engine)
    _migrate_campaigns_table(engine)
    _seed_users()
    _seed_recipients()

def _seed_users():
    from core.auth import hash_password
    
    db = SessionLocal()
    try:
        seed_data = [
            {
                "id": "user-admin",
                "name": "System Admin",
                "email": "admin@company.com",
                "role": "admin",
                "business_unit": None,
                "manager_id": None,
                "temp_password": "Admin@1234",
            },
            {
                "id": "user-charlie",
                "name": "Charlie Chen",
                "email": "charlie@company.com",
                "role": "leader",
                "business_unit": None,
                "manager_id": None,
                "temp_password": "Leader@1234",
            },
            {
                "id": "user-bob-bu1",
                "name": "Bob Brown",
                "email": "bob.bu1@company.com",
                "role": "approver",
                "business_unit": "BU1",
                "manager_id": "user-charlie",
                "temp_password": "Approver@1234",
            },
            {
                "id": "user-bob-bu2",
                "name": "Bob Smith",
                "email": "bob.bu2@company.com",
                "role": "approver",
                "business_unit": "BU2",
                "manager_id": "user-charlie",
                "temp_password": "Approver@1234",
            },
            {
                "id": "user-alice",
                "name": "Alice Adams",
                "email": "alice@company.com",
                "role": "campaigner",
                "business_unit": "BU1",
                "manager_id": "user-bob-bu1",
                "temp_password": "Camp@1234",
            },
            {
                "id": "user-dave",
                "name": "Dave Davis",
                "email": "dave@company.com",
                "role": "campaigner",
                "business_unit": "BU1",
                "manager_id": "user-bob-bu1",
                "temp_password": "Camp@1234",
            },
            {
                "id": "user-carol",
                "name": "Carol Cox",
                "email": "carol@company.com",
                "role": "campaigner",
                "business_unit": "BU2",
                "manager_id": "user-bob-bu2",
                "temp_password": "Camp@1234",
            },
            {
                "id": "user-evan",
                "name": "Evan Evans",
                "email": "evan@company.com",
                "role": "campaigner",
                "business_unit": "BU2",
                "manager_id": "user-bob-bu2",
                "temp_password": "Camp@1234",
            },
        ]

        for s in seed_data:
            existing = db.query(User).filter(User.id == s["id"]).first()
            if existing:
                # Patch auth fields if password not set yet
                if not existing.hashed_password:
                    existing.hashed_password = hash_password(s["temp_password"])
                    existing.role = s["role"]
                    existing.business_unit = s["business_unit"]
                    existing.must_change_password = True
            else:
                u = User(
                    id=s["id"],
                    name=s["name"],
                    email=s["email"],
                    manager_id=s["manager_id"],
                    hashed_password=hash_password(s["temp_password"]),
                    role=s["role"],
                    business_unit=s["business_unit"],
                    must_change_password=True,
                )
                db.add(u)
        
        db.commit()
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()

def _seed_recipients():
    """
    Seed 80 demo recipients with varied segmentation attributes.
    Covers English/US, Spanish/ES, French/FR, German/DE populations
    across all age categories. A handful are pre-unsubscribed to
    demonstrate suppression logic.
    """
    db = SessionLocal()
    try:
        if db.query(Recipient).count() > 0:
            return

        import random
        random.seed(42)

        profiles = [
            # (language, country, age_category, count)
            ("English", "US", "18-25", 10),
            ("English", "US", "26-35", 12),
            ("English", "US", "36-50", 10),
            ("English", "US", "50+", 5),
            ("English", "GB", "26-35", 6),
            ("English", "GB", "36-50", 4),
            ("Spanish", "ES", "18-25", 6),
            ("Spanish", "ES", "26-35", 8),
            ("Spanish", "MX", "26-35", 4),
            ("French", "FR", "26-35", 6),
            ("French", "FR", "36-50", 4),
            ("German", "DE", "36-50", 5),
        ]

        first_names = ["Alex", "Jordan", "Casey", "Morgan", "Taylor", "Riley",
                      "Drew", "Quinn", "Sage", "Jamie", "Chris", "Sam", "Pat",
                      "Dana", "Robin", "Avery", "Logan", "Blair", "Reese", "Parker"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Davis", "Miller",
                     "Wilson", "Moore", "Anderson", "Thomas", "Jackson", "White",
                     "Harris", "Martin", "Garcia", "Rodriguez", "Lopez", "Lee"]

        all_recipients = []
        idx = 0
        for lang, country, age_cat, count in profiles:
            for _ in range(count):
                fn = random.choice(first_names)
                ln = random.choice(last_names)
                token = secrets.token_urlsafe(16)
                r = Recipient(
                    id=f"rec-{idx:04d}",
                    email=f"{fn.lower()}.{ln.lower()}.{idx}@demo-contacts.com",
                    first_name=fn,
                    last_name=ln,
                    language=lang,
                    country=country,
                    age_category=age_cat,
                    is_unsubscribed=False,
                    unsubscribe_token=token,
                )
                all_recipients.append(r)
                idx += 1

        # Pre-mark ~5% as unsubscribed to demo suppression
        for r in random.sample(all_recipients, max(1, len(all_recipients) // 20)):
            r.is_unsubscribed = True
            r.unsubscribed_at = datetime.utcnow()

        db.add_all(all_recipients)
        db.commit()
    except Exception:
        db.rollback()
    finally:
        db.close()

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()