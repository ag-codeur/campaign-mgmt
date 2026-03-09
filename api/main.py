import uuid
import hashlib
import secrets
from datetime import datetime
from typing import Optional

from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request, Header
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response, RedirectResponse, HTMLResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from loguru import logger

from core.database import (
    get_db, init_db,
    Campaign, User, AudienceBranch, EmailEvent, ChatMessage,
    Recipient, ABVariant, RecipientCampaignSend, RecipientEvent, AgentRun,
    AuthSession, GuardrailCheck,
)
from core.auth import hash_password, verify_password, create_session_token, get_session_user
from agents.supervisor import (
    run_campaign_workflow,
    run_content_creation_sync,
    run_execution_sync,
    run_evaluation_sync,
)
from agents.planner import suggest_audience_branches, suggest_audience_query

app = FastAPI(title="Campaign Management System", version="5.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.on_event("startup")
async def startup():
    init_db()
    logger.info("Campaign Management System v5.0 started - auth enabled")

# --- Request / Response Models ---

class CampaignCreate(BaseModel):
    goal: str
    audience: str
    created_by: Optional[str] = None # ignored; derived from session

class CampaignUpdate(BaseModel):
    goal: Optional[str] = None
    audience: Optional[str] = None

class SubmitRequest(BaseModel):
    pass

class ManagerApprovalRequest(BaseModel):
    approved: bool
    reason: Optional[str] = None
    approver_id: Optional[str] = None # ignored; derived from session

class ApprovalRequest(BaseModel):
    approved: bool
    feedback: Optional[str] = None

class RatingRequest(BaseModel):
    rating: str # good | neutral | poor

class BranchCreate(BaseModel):
    branch_name: str
    language: Optional[str] = None
    country: Optional[str] = None
    age_category: Optional[str] = None
    custom_query: Optional[str] = None
    scheduled_at: Optional[str] = None

class BranchUpdate(BaseModel):
    branch_name: Optional[str] = None
    language: Optional[str] = None
    country: Optional[str] = None
    age_category: Optional[str] = None
    custom_query: Optional[str] = None
    scheduled_at: Optional[str] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None

class SuggestQueryRequest(BaseModel):
    goal: Optional[str] = None
    audience: Optional[str] = None
    language: Optional[str] = None
    country: Optional[str] = None
    age_category: Optional[str] = None

class ChatRequest(BaseModel):
    message: str
    user_id: Optional[str] = None
    campaign_id: Optional[str] = None
    history: list = []

class TranslateRequest(BaseModel):
    text: str
    source_language: str = "Unknown"
    target_language: str = "English"

class ABVariantCreate(BaseModel):
    variant_name: str
    split_percentage: float = 50.0
    email_subject: Optional[str] = None
    email_body: Optional[str] = None

class ABVariantUpdate(BaseModel):
    variant_name: Optional[str] = None
    split_percentage: Optional[float] = None
    email_subject: Optional[str] = None
    email_body: Optional[str] = None

class LoginRequest(BaseModel):
    email: str
    password: str

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

class CreateUserRequest(BaseModel):
    name: str
    email: str
    role: str
    business_unit: Optional[str] = None
    manager_id: Optional[str] = None

# --- 1x1 transparent PNG tracking pixel ---
_TRACKING_PIXEL: bytes = bytes([
    0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
    0x00, 0x00, 0x00, 0x0d, 0x49, 0x48, 0x44, 0x52,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01,
    0x08, 0x06, 0x00, 0x00, 0x00, 0x1f, 0x15, 0xc4,
    0x89, 0x00, 0x00, 0x00, 0x0a, 0x49, 0x44, 0x41,
    0x54, 0x78, 0x9c, 0x62, 0x00, 0x01, 0x00, 0x00,
    0x05, 0x00, 0x01, 0x0d, 0x0a, 0x2d, 0xb4, 0x00,
    0x00, 0x00, 0x00, 0x49, 0x45, 0x4e, 0x44, 0xae,
    0x42, 0x60, 0x82,
])

# Endpoints that do NOT require a valid (non-must_change_password) session
_AUTH_EXEMPT_PATHS = {"/auth/login", "/auth/me", "/auth/logout", "/auth/change-password"}

# --- Auth helpers ---

def get_current_user_any(
    x_session_token: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> User:
    """Validate session token only (no must_change_password check)."""
    user = get_session_user(db, x_session_token)
    if not user:
        raise HTTPException(401, "Invalid or expired session token")
    return user

def get_current_user(
    x_session_token: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
) -> User:
    """Validate session token and enforce must_change_password gate."""
    user = get_session_user(db, x_session_token)
    if not user:
        raise HTTPException(401, "Invalid or expired session token")
    if user.must_change_password:
        raise HTTPException(
            403,
            {
                "message": "You must change your password before accessing this resource.",
                "action": "Use POST /auth/change-password first."
            }
        )
    return user

def _require_role(user: User, *roles: str):
    if user.role not in roles:
        raise HTTPException(403, f"Role '{user.role}' is not allowed. Required: {roles}")

def _campaigns_query(db: Session, user: User):
    """Return a Campaign query filtered by BU for approver/campaigner."""
    q = db.query(Campaign)
    if user.role in ("approver", "campaigner"):
        q = q.filter(Campaign.business_unit == user.business_unit)
    return q

def _mask_recipient(r: Recipient) -> dict:
    return {
        "id": r.id,
        "email": "***@***",
        "first_name": "***",
        "last_name": "***",
        "language": r.language,
        "country": r.country,
        "age_category": r.age_category,
        "is_unsubscribed": r.is_unsubscribed,
    }

def _user_to_dict(u: User) -> dict:
    return {
        "id": u.id,
        "name": u.name,
        "email": u.email,
        "role": u.role,
        "business_unit": u.business_unit,
        "manager_id": u.manager_id,
        "must_change_password": u.must_change_password,
    }

# --- Auth Endpoints ---

@app.post("/auth/login")
def login(req: LoginRequest, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.email == req.email).first()
    if not user or not user.hashed_password:
        raise HTTPException(401, "Invalid email or password")
    if not verify_password(req.password, user.hashed_password):
        raise HTTPException(401, "Invalid email or password")
    
    token = create_session_token(db, user.id)
    return {
        "token": token,
        "user": _user_to_dict(user),
    }

@app.post("/auth/logout")
def logout(
    x_session_token: Optional[str] = Header(default=None),
    db: Session = Depends(get_db),
):
    if x_session_token:
        session = db.query(AuthSession).filter(AuthSession.id == x_session_token).first()
        if session:
            session.is_revoked = True
            db.commit()
    return {"message": "Logged out"}

@app.get("/auth/me")
def get_me(current_user: User = Depends(get_current_user_any)):
    return _user_to_dict(current_user)

@app.post("/auth/change-password")
def change_password(
    req: ChangePasswordRequest,
    current_user: User = Depends(get_current_user_any),
    db: Session = Depends(get_db),
):
    if not verify_password(req.old_password, current_user.hashed_password):
        raise HTTPException(400, "Current password is incorrect")
    if len(req.new_password) < 6:
        raise HTTPException(400, "New password must be at least 6 characters")
    
    current_user.hashed_password = hash_password(req.new_password)
    current_user.must_change_password = False
    db.commit()
    return {"message": "Password changed successfully"}

# --- User Endpoints ---

@app.get("/users")
def list_users(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "admin", "leader", "approver")
    q = db.query(User)
    if current_user.role == "approver":
        q = q.filter(User.business_unit == current_user.business_unit)
    return [_user_to_dict(u) for u in q.all()]

@app.post("/users", status_code=201)
def create_user(
    req: CreateUserRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "admin", "leader", "approver")

    # Approver can only create campaigners in their own BU
    if current_user.role == "approver":
        if req.role != "campaigner":
            raise HTTPException(403, "Approvers can only create campaigner accounts")
        if req.business_unit and req.business_unit != current_user.business_unit:
            raise HTTPException(403, "Approvers can only create users in their own BU")
        effective_bu = current_user.business_unit
    else:
        if req.business_unit and req.business_unit != current_user.business_unit:
             # Logic for leaders/admins to stay in BU or override (partially visible)
             raise HTTPException(403, "Approvers can only create users in their own BU")
        effective_bu = current_user.business_unit
        
        effective_bu = req.business_unit

    # Check email uniqueness
    if db.query(User).filter(User.email == req.email).first():
        raise HTTPException(409, f"Email {req.email} already exists")

    temp_password = secrets.token_urlsafe(10)
    new_user = User(
        id=str(uuid.uuid4()),
        name=req.name,
        email=req.email,
        manager_id=req.manager_id,
        hashed_password=hash_password(temp_password),
        role=req.role,
        business_unit=effective_bu,
        must_change_password=True,
    )

    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    logger.info(f"User {new_user.id} ({req.email}) created by {current_user.id}")
    return {"user_id": new_user.id, "temp_password": temp_password}

@app.get("/users/{user_id}")
def get_user(
    user_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    u = db.query(User).filter(User.id == user_id).first()
    if not u:
        raise HTTPException(404, "User not found")
    
    manager = db.query(User).filter(User.id == u.manager_id).first() if u.manager_id else None
    return {
        "id": u.id, "name": u.name, "email": u.email,
        "role": u.role, "business_unit": u.business_unit,
        "manager_id": u.manager_id,
        "manager_name": manager.name if manager else None,
        "manager_email": manager.email if manager else None,
    }

@app.get("/users/{user_id}/manager")
def get_manager(
    user_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    u = db.query(User).filter(User.id == user_id).first()
    if not u:
        raise HTTPException(404, "User not found")
    if not u.manager_id:
        return {"id": None, "name": None, "email": None}
    
    manager = db.query(User).filter(User.id == u.manager_id).first()
    if not manager:
        raise HTTPException(404, "Manager record not found")
    return {"id": manager.id, "name": manager.name, "email": manager.email}

@app.get("/users/{user_id}/direct-reports")
def get_direct_reports(
    user_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    reports = db.query(User).filter(User.manager_id == user_id).all()
    return [{"id": r.id, "name": r.name, "email": r.email} for r in reports]

# --- Campaign Endpoints ---

@app.post("/campaigns", status_code=201)
def create_campaign(
    req: CampaignCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "campaigner", "admin")
    campaign = Campaign(
        id=str(uuid.uuid4()),
        goal=req.goal,
        audience=req.audience,
        created_by=current_user.id,
        business_unit=current_user.business_unit,
        status="draft",
    )
    db.add(campaign)
    db.commit()
    db.refresh(campaign)
    logger.info(f"Campaign {campaign.id} created by {current_user.id} (BU: {current_user.business_unit})")
    return {"campaign_id": campaign.id, "status": campaign.status}

@app.put("/campaigns/{campaign_id}")
def update_campaign(
    campaign_id: str,
    req: CampaignUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
    if c.status not in ("draft", "rejected"):
        raise HTTPException(400, f"Can only edit campaigns in draft or rejected state (current: {c.status})")
    
    if req.goal is not None:
        c.goal = req.goal
    if req.audience is not None:
        c.audience = req.audience
    
    c.updated_at = datetime.utcnow()
    db.commit()
    return {"message": "Campaign updated"}

@app.get("/campaigns")
def list_campaigns(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    rows = _campaigns_query(db, current_user).order_by(Campaign.created_at.desc()).all()
    return [
        {
            "id": c.id, "goal": c.goal, "status": c.status,
            "created_by": c.created_by, "approved_by": c.approved_by,
            "business_unit": c.business_unit,
            "created_at": c.created_at.isoformat() if c.created_at else None,
        }
        for c in rows
    ]

@app.get("/campaigns/pending-approval/{manager_id}")
def get_pending_for_manager(
    manager_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "approver", "admin", "leader")
    # Validate manager_id against current user (unless admin)
    if current_user.role not in ("admin", "leader") and current_user.id != manager_id:
        raise HTTPException(403, "You can only view your own pending approval queue")

    report_ids_q = db.query(User).filter(User.manager_id == manager_id)
    if current_user.role == "approver":
        report_ids_q = report_ids_q.filter(User.business_unit == current_user.business_unit)
    
    report_ids = [r.id for r in report_ids_q.all()]
    if not report_ids:
        return []

    q = db.query(Campaign).filter(
        Campaign.created_by.in_(report_ids),
        Campaign.status == "awaiting_manager_approval",
    )
    
    if current_user.role == "approver":
        q = q.filter(Campaign.business_unit == current_user.business_unit)

    campaigns = q.order_by(Campaign.submitted_at.desc()).all()
    result = []
    for c in campaigns:
        creator = db.query(User).filter(User.id == c.created_by).first()
        result.append({
            "id": c.id, "goal": c.goal, "status": c.status,
            "created_by": c.created_by,
            "creator_name": creator.name if creator else None,
            "submitted_at": c.submitted_at.isoformat() if c.submitted_at else None,
        })
    return result

@app.get("/campaigns/{campaign_id}")
def get_campaign(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
    
    creator = db.query(User).filter(User.id == c.created_by).first() if c.created_by else None
    approver = db.query(User).filter(User.id == c.approved_by).first() if c.approved_by else None
    
    branches = [
        _branch_to_dict(b, db)
        for b in db.query(AudienceBranch).filter(AudienceBranch.campaign_id == campaign_id).all()
    ]

    return {
        "id": c.id, "goal": c.goal, "audience": c.audience,
        "strategy": c.strategy,
        "execution_results": c.execution_results,
        "evaluation": c.evaluation, "status": c.status,
        "rating": c.rating,
        "rejection_feedback": c.rejection_feedback,
        "created_by": c.created_by,
        "creator_name": creator.name if creator else None,
        "approved_by": c.approved_by,
        "approver_name": approver.name if approver else None,
        "submitted_at": c.submitted_at.isoformat() if c.submitted_at else None,
        "approved_at": c.approved_at.isoformat() if c.approved_at else None,
        "manager_rejection_reason": c.manager_rejection_reason,
        "business_unit": c.business_unit,
        "created_at": c.created_at.isoformat() if c.created_at else None,
        "branches": branches,
    }

# --- Submit / Manager Approval ---

@app.post("/campaigns/{campaign_id}/submit")
def submit_campaign(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
    
    if c.created_by != current_user.id and current_user.role != "admin":
        raise HTTPException(403, "Only the campaign creator can submit this campaign")
    
    if c.status not in ("draft", "rejected"):
        raise HTTPException(400, f"Cannot submit campaign in state: {c.status}")

    branches = db.query(AudienceBranch).filter(AudienceBranch.campaign_id == campaign_id).all()
    if not branches:
        raise HTTPException(400, "Add at least one audience branch before submitting")

    creator = db.query(User).filter(User.id == c.created_by).first()
    if creator and creator.manager_id:
        manager = db.query(User).filter(User.id == creator.manager_id).first()
    
    c.status = "awaiting_manager_approval"
    c.submitted_at = datetime.utcnow()
    c.manager_rejection_reason = None
    c.updated_at = datetime.utcnow()
    db.commit()

    logger.info(f"Campaign {campaign_id} submitted - awaiting approval from {manager.name if manager else 'no manager'}")
    return {
        "message": "Campaign submitted for manager approval",
        "manager_name": manager.name if manager else "No manager assigned",
        "manager_email": manager.email if manager else None,
    }

@app.post("/campaigns/{campaign_id}/manager-approve")
async def manager_approve_campaign(
    campaign_id: str,
    req: ManagerApprovalRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "approver", "admin")
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
    if c.status != "awaiting_manager_approval":
        raise HTTPException(400, f"Campaign not awaiting manager approval (current: {c.status})")
    
    # Verify the approver is the campaign creator's actual manager
    creator = db.query(User).filter(User.id == c.created_by).first()
    if creator and creator.manager_id and creator.manager_id != current_user.id:
        if current_user.role != "admin":
            raise HTTPException(403, "Only the campaign creator's direct manager can approve this campaign")
    if req.approved:
        c.status = "approved"
        c.approved_by = current_user.id
        c.approved_at = datetime.utcnow()
        c.updated_at = datetime.utcnow()
        db.commit()
        
        background_tasks.add_task(run_campaign_workflow, campaign_id)
        logger.info(f"Campaign {campaign_id} approved by {current_user.id} - AI planning started")
        return {"message": "Campaign approved - AI planning has started"}
    else:
        c.status = "rejected"
        c.manager_rejection_reason = req.reason
        c.updated_at = datetime.utcnow()
        db.commit()
        
        logger.info(f"Campaign {campaign_id} rejected by manager: {req.reason}")
        return {"message": "Campaign rejected - creator has been notified"}

# --- AI HITL Approval (Plan & Content) ---

@app.post("/campaigns/{campaign_id}/approve")
async def approve_campaign(
    campaign_id: str,
    req: ApprovalRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")

    logger.info(f"[APPROVE] Campaign {campaign_id} in status {c.status}, approved={req.approved}")

    if c.status == "awaiting_plan_approval":
        if req.approved:
            logger.info(f"[APPROVE] Approving strategy for {campaign_id} - updating to creating_content")
            c.status = "creating_content"
            c.updated_at = datetime.utcnow()
            db.commit()
            logger.info(f"[APPROVE] Status committed for {campaign_id}")
            background_tasks.add_task(run_content_creation_sync, campaign_id)
            logger.info(f"[APPROVE] Background task queued for {campaign_id}")
            return {"message": "Strategy approved - generating email content per branch"}
        else:
            logger.info(f"[APPROVE] Rejecting strategy for {campaign_id} - returning to draft")
            c.status = "draft"
            c.rejection_feedback = req.feedback
            c.updated_at = datetime.utcnow()
            db.commit()
            return {"message": "Strategy rejected - campaign returned to draft for revisions"}
    elif c.status == "awaiting_content_approval":
        if req.approved:
            branches = db.query(AudienceBranch).filter(AudienceBranch.campaign_id == campaign_id).all()
            has_schedule = any(b.scheduled_at for b in branches)
            
            if not has_schedule:
                # No scheduled time - execute immediately
                c.status = "executing"
                c.updated_at = datetime.utcnow()
                db.commit()
                logger.info(f"[APPROVE] Content approved, executing immediately for {campaign_id}")
                background_tasks.add_task(run_execution_sync, campaign_id)
                return {"message": "Content approved - delivering emails now"}
            else:
                # Has scheduled times - keep for batch scheduler
                c.status = "scheduled"
                c.updated_at = datetime.utcnow()
                db.commit()
                logger.info(f"[APPROVE] Content approved, scheduled for {campaign_id}")
                return {"message": "Content approved - emails will be delivered at scheduled times"}
        else:
            # Rejection case
            c.status = "draft"
            c.rejection_feedback = req.feedback
            c.updated_at = datetime.utcnow()
            db.commit()
            logger.info(f"[APPROVE] Content rejected for {campaign_id}")
            return {"message": "Content rejected - campaign returned to draft for revisions"}

    raise HTTPException(400, f"Campaign not in an approval state (current: {c.status})")

@app.post("/campaigns/{campaign_id}/execute")
async def execute_campaign(
    campaign_id: str,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
    if c.status != "scheduled":
        raise HTTPException(400, f"Campaign must be in 'scheduled' state (current: {c.status})")
    
    background_tasks.add_task(run_execution_sync, campaign_id)
    return {"message": "Execution triggered"}

@app.post("/campaigns/{campaign_id}/rate")
async def rate_campaign(
    campaign_id: str,
    req: RatingRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
    if c.status != "awaiting_rating":
        raise HTTPException(400, f"Campaign not awaiting rating (current: {c.status})")
    
    logger.info(f"[RATE] Saving rating '{req.rating}' for campaign {campaign_id}")
    c.rating = req.rating
    c.updated_at = datetime.utcnow()
    db.commit()
    
    # Verify it was saved
    db.refresh(c)
    if not c.execution_results:
        logger.warning(f"[RATE] Campaign {campaign_id} has no execution_results before evaluation")
    logger.info(f"[RATE] Rating verified in DB: {c.rating} for campaign {campaign_id}. Has exec_results: {bool(c.execution_results)}")
    background_tasks.add_task(run_evaluation_sync, campaign_id, req.rating)
    return {"message": f"Rating '{req.rating}' submitted - running evaluation", "rating_saved": c.rating}

# --- Audience Branch Endpoints ---

@app.get("/campaigns/{campaign_id}/branches")
def get_branches(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first():
        raise HTTPException(404, "Campaign not found")
    
    branches = db.query(AudienceBranch).filter(AudienceBranch.campaign_id == campaign_id).all()
    return [_branch_to_dict(b, db) for b in branches]

@app.get("/campaigns/{campaign_id}/branches/{branch_id}/audience-estimate")
def estimate_branch_audience(
    campaign_id: str,
    branch_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    """Estimate audience size for a branch based on filter criteria."""
    if not _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first():
        raise HTTPException(404, "Campaign not found")
    
    branch = db.query(AudienceBranch).filter(
        AudienceBranch.id == branch_id,
        AudienceBranch.campaign_id == campaign_id
    ).first()
    if not branch:
        raise HTTPException(404, "Branch not found")
    
    # If the campaign has already been executed, return the actual sent count
    if branch.sent_count is not None:
        try:
            return {"estimated_audience": int(branch.sent_count), "source": "actual_sent"}
        except ValueError:
            pass
    
    # Try to match real recipients
    real_recipients = db.query(Recipient).filter(Recipient.unsubscribe_token.is_(None))
    
    if branch.language:
        real_recipients = real_recipients.filter(Recipient.language == branch.language)
    if branch.country:
        real_recipients = real_recipients.filter(Recipient.country == branch.country)
    if branch.age_category:
        real_recipients = real_recipients.filter(Recipient.age_category == branch.age_category)
    
    real_count = real_recipients.count()
    
    # If actual recipients match, return that count; otherwise estimate
    if real_count > 0:
        return {"estimated_audience": real_count, "source": "actual_match"}
    
    # Fallback: estimate based on base population and filter reduction percentages
    base_population = 10000
    estimate = float(base_population)
    
    if branch.language:
        estimate *= 0.60  # Language reduces to 60% of base
    if branch.country:
        estimate *= 0.50  # Country narrows further to 50%
    if branch.age_category:
        estimate *= 0.40  # Age category narrows to 40%
    if branch.custom_query:
        estimate *= 0.70  # Custom query typically has 70% match
    
    estimate = max(int(estimate), 100)  # Minimum 100 estimated recipients
    return {"estimated_audience": estimate, "source": "estimated"}

@app.post("/campaigns/{campaign_id}/branches", status_code=201)
def add_branch(
    campaign_id: str,
    req: BranchCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first():
        raise HTTPException(404, "Campaign not found")
    
    branch = AudienceBranch(
        id=str(uuid.uuid4()),
        campaign_id=campaign_id,
        branch_name=req.branch_name,
        language=req.language,
        country=req.country,
        age_category=req.age_category,
        custom_query=req.custom_query,
        scheduled_at=parse_scheduled_at(req.scheduled_at),
    )
    db.add(branch)
    db.commit()
    db.refresh(branch)
    return {"branch_id": branch.id, "message": "Branch added"}

@app.put("/campaigns/{campaign_id}/branches/{branch_id}")
def update_branch(
    campaign_id: str,
    branch_id: str,
    req: BranchUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first():
        raise HTTPException(404, "Campaign not found")
    
    b = db.query(AudienceBranch).filter(
        AudienceBranch.id == branch_id, 
        AudienceBranch.campaign_id == campaign_id
    ).first()
    
    if not b:
        raise HTTPException(404, "Branch not found")

    if req.branch_name is not None: b.branch_name = req.branch_name
    if req.language is not None: b.language = req.language
    if req.country is not None: b.country = req.country
    if req.age_category is not None: b.age_category = req.age_category
    if req.custom_query is not None: b.custom_query = req.custom_query
    if req.scheduled_at is not None: b.scheduled_at = parse_scheduled_at(req.scheduled_at)
    if req.email_subject is not None: b.email_subject = req.email_subject
    if req.email_body is not None: b.email_body = req.email_body

    b.updated_at = datetime.utcnow()
    db.commit()
    return {"message": "Branch updated"}

@app.delete("/campaigns/{campaign_id}/branches/{branch_id}")
def delete_branch(
    campaign_id: str,
    branch_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first():
        raise HTTPException(404, "Campaign not found")
    
    b = db.query(AudienceBranch).filter(
        AudienceBranch.id == branch_id, 
        AudienceBranch.campaign_id == campaign_id
    ).first()
    
    if not b:
        raise HTTPException(404, "Branch not found")
        
    db.delete(b)
    db.commit()
    return {"message": "Branch deleted"}

# --- AI Suggestion Endpoints ---

@app.post("/campaigns/{campaign_id}/suggest-branches")
def suggest_branches_endpoint(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
        
    suggestions = suggest_audience_branches(c.goal, c.audience)
    return {"suggestions": suggestions}

@app.post("/campaigns/{campaign_id}/suggest-query")
def suggest_query_endpoint(
    campaign_id: str,
    req: SuggestQueryRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")
        
    query = suggest_audience_query(
        goal=c.goal, 
        audience=c.audience,
        language=req.language,
        country=req.country,
        age_category=req.age_category
    )
    return {"suggested_query": query}

# --- Health ---

@app.get("/health")
def health():
    return {"status": "ok", "version": "5.0.0", "channel": "email-only"}

# --- RAI Evaluation Endpoints (admin-only) ---

@app.get("/rai/summary")
def rai_summary(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "admin")

    # Content Safety - from GuardrailCheck
    checks = db.query(GuardrailCheck).all()
    total_checks = len(checks)
    passed_checks = sum(1 for c in checks if c.passed)
    pass_rate = round(passed_checks / total_checks * 100, 1) if total_checks > 0 else 100.0
    pii_violations = sum(len(c.pii_issues or []) for c in checks)
    brand_violations = sum(len(c.brand_safety_issues or []) for c in checks)

    # Process Compliance - HITL completion rate
    all_campaigns = db.query(Campaign).all()
    total_campaigns = len(all_campaigns)
    full_hitl_statuses = {
        "awaiting_content_approval", "scheduled", "executing",
        "awaiting_rating", "evaluating", "completed",
    }
    full_hitl_count = sum(
        1 for c in all_campaigns if c.status in full_hitl_statuses
    )
    hitl_rate = round(full_hitl_count / total_campaigns * 100, 1) if total_campaigns > 0 else 0.0

    # Manager Oversight - approval/rejection tracking
    reviewed = db.query(Campaign).filter(Campaign.approved_by.isnot(None)).all()
    total_reviewed = len(reviewed)
    rejected = db.query(Campaign).filter(Campaign.status == "rejected").count()
    rejection_rate = round(rejected / total_reviewed * 100, 1) if total_reviewed > 0 else 0.0

    # Audience Satisfaction - unsubscribe rate
    total_sends = db.query(RecipientEvent).filter(
        RecipientEvent.event_type == "open"
    ).count()
    unsubscribes = db.query(RecipientEvent).filter(
        RecipientEvent.event_type == "unsubscribe"
    ).count()
    unsubscribe_rate = round(unsubscribes / total_sends * 100, 2) if total_sends > 0 else 0.0

    return {
        "content_safety": {
            "total_checks": total_checks,
            "passed_checks": passed_checks,
            "pass_rate": pass_rate,
            "pii_violations": pii_violations,
            "brand_safety_violations": brand_violations,
        },
        "process_compliance": {
            "total_campaigns": total_campaigns,
            "full_hitl_count": full_hitl_count,
            "hitl_rate": hitl_rate,
        },
        "manager_oversight": {
            "total_reviewed": total_reviewed,
            "rejected": rejected,
            "rejection_rate": rejection_rate,
        },
        "audience_satisfaction": {
            "total_sends": total_sends,
            "unsubscribes": unsubscribes,
            "unsubscribe_rate": unsubscribe_rate,
        },
    }

@app.get("/rai/guardrail-config")
def rai_guardrail_config(
    current_user: User = Depends(get_current_user),
):
    _require_role(current_user, "admin")
    from guardrails.rails import get_guardrail_config
    return get_guardrail_config()

@app.get("/rai/checks")
def rai_checks(
    limit: int = 50,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "admin")
    rows = (
        db.query(GuardrailCheck)
        .order_by(GuardrailCheck.checked_at.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": r.id,
            "campaign_id": r.campaign_id,
            "branch_id": r.branch_id,
            "variant_id": r.variant_id,
            "passed": r.passed,
            "pii_issues": r.pii_issues or [],
            "brand_safety_issues": r.brand_safety_issues or [],
            "total_issues": r.total_issues,
            "checked_at": r.checked_at.isoformat() if r.checked_at else None,
        }
        for r in rows
    ]

@app.get("/rai/campaign/{campaign_id}")
def rai_campaign_checks(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    _require_role(current_user, "admin")
    rows = (
        db.query(GuardrailCheck)
        .filter(GuardrailCheck.campaign_id == campaign_id)
        .order_by(GuardrailCheck.checked_at.desc())
        .all()
    )
    return [
        {
            "id": r.id,
            "branch_id": r.branch_id,
            "variant_id": r.variant_id,
            "passed": r.passed,
            "pii_issues": r.pii_issues or [],
            "brand_safety_issues": r.brand_safety_issues or [],
            "checked_at": r.checked_at.isoformat() if r.checked_at else None,
        }
        for r in rows
    ]

# --- Chat Assistant -----------------------------------------------------------

@app.post("/chat")
def chat_endpoint(
    req: ChatRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    from agents.chat_agent import run_chat
    from core.config import get_settings

    user_id = current_user.id
    context: dict = {}
    user = current_user
    mgr = db.query(User).filter(User.id == user.manager_id).first() if user.manager_id else None
    context["user"] = {
        "id": user.id, "name": user.name, "email": user.email,
        "manager_name": mgr.name if mgr else None,
    }
    context["campaigns"] = [
        {"id": c.id, "goal": c.goal, "status": c.status}
        for c in _campaigns_query(db, current_user).filter(Campaign.created_by == user_id).all()[:8]
    ]
    context["pending_count"] = (
        _campaigns_query(db, current_user)
        .filter(
            Campaign.created_by.in_(
                [r.id for r in db.query(User).filter(User.manager_id == user_id).all()]
            ),
            Campaign.status == "awaiting_manager_approval",
        )
        .count()
    )

    if req.campaign_id:
        c = db.query(Campaign).filter(Campaign.id == req.campaign_id).first()
        if c:
            branches = db.query(AudienceBranch).filter(
                AudienceBranch.campaign_id == req.campaign_id
            ).all()
            context["current_campaign"] = {
                "id": c.id, "goal": c.goal, "audience": c.audience,
                "status": c.status, "execution_results": c.execution_results,
                "branches": [{"id": b.id, "branch_name": b.branch_name} for b in branches],
            }

    run = AgentRun(
        id=str(uuid.uuid4()), agent_type="chat", campaign_id=req.campaign_id,
        status="running", input_summary=req.message[:200], started_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()

    reply = run_chat(req.message, context, req.history, run_id=run.id)

    try:
        db.add(ChatMessage(id=str(uuid.uuid4()), user_id=user_id, role="user",
                           content=req.message, campaign_id=req.campaign_id))
        db.add(ChatMessage(id=str(uuid.uuid4()), user_id=user_id, role="assistant",
                           content=reply, campaign_id=req.campaign_id))
        db.commit()
    except Exception:
        pass

    return {"response": reply}

# --- Translation --------------------------------------------------------------

@app.post("/translate")
def translate_endpoint(
    req: TranslateRequest,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    from agents.translate_agent import run_translate

    run = AgentRun(
        id=str(uuid.uuid4()), agent_type="translate",
        status="running",
        input_summary=f"{req.source_language}->{req.target_language}: {req.text[:100]}",
        started_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()

    try:
        translated = run_translate(req.text, req.source_language, req.target_language,
                                   run_id=run.id)
        return {"translated_text": translated, "target_language": req.target_language}
    except Exception as e:
        raise HTTPException(500, f"Translation failed: {e}")

# --- Email Engagement Tracking (unauthenticated - called by email clients) ---

@app.get("/track/open/{campaign_id}/{branch_id}")
def track_open(
    campaign_id: str,
    branch_id: str,
    r: Optional[str] = None,
    request: Request = None,
    db: Session = Depends(get_db),
):
    ua = (request.headers.get("user-agent", "") if request else "")[:500]
    ip = (request.client.host if request and request.client else "")
    ip_hash = hashlib.sha256(ip.encode()).hexdigest()[:16] if ip else None

    branch = db.query(AudienceBranch).filter(AudienceBranch.id == branch_id).first()
    segment_label = (
        f"{branch.language or 'Any'}/{branch.country or 'Any'}/{branch.age_category or 'all'}"
        if branch else None
    )

    db.add(EmailEvent(
        id=str(uuid.uuid4()),
        campaign_id=campaign_id,
        branch_id=branch_id,
        event_type="open",
        recipient_token=r,
        user_agent=ua or None,
        ip_hash=ip_hash,
        segment_label=segment_label,
    ))
    db.commit()

    return Response(content=TRACKING_PIXEL, media_type="image/png",
                    headers={"Cache-Control": "no-cache, no-store, must-revalidate"})

@app.get("/track/click/{campaign_id}/{branch_id}")
def track_click(
    campaign_id: str,
    branch_id: str,
    url: str,
    r: Optional[str] = None,
    request: Request = None,
    db: Session = Depends(get_db),
):
    ua = (request.headers.get("user-agent", "") if request else "")[:500]
    ip = (request.client.host if request and request.client else "")
    ip_hash = hashlib.sha256(ip.encode()).hexdigest()[:16] if ip else None

    branch = db.query(AudienceBranch).filter(AudienceBranch.id == branch_id).first()
    segment_label = (
        f"{branch.language or 'Any'}/{branch.country or 'Any'}/{branch.age_category or 'all'}"
        if branch else None
    )

    db.add(EmailEvent(
        id=str(uuid.uuid4()),
        campaign_id=campaign_id,
        branch_id=branch_id,
        event_type="click",
        recipient_token=r,
        link_url=url[:2000],
        user_agent=ua or None,
        ip_hash=ip_hash,
        segment_label=segment_label,
    ))
    db.commit()

    return RedirectResponse(url=url, status_code=302)

# --- Engagement Analytics -----------------------------------------------------

@app.get("/campaigns/{campaign_id}/engagement")
def get_campaign_engagement(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")

    branches = db.query(AudienceBranch).filter(
        AudienceBranch.campaign_id == campaign_id
    ).all()
    events = db.query(EmailEvent).filter(
        EmailEvent.campaign_id == campaign_id
    ).all()

    branch_stats: dict = {}
    for b in branches:
        sent = int(b.sent_count or 0)
        b_events = [e for e in events if e.branch_id == b.id]
        opens = len([e for e in b_events if e.event_type == "open"])
        clicks = len([e for e in b_events if e.event_type == "click"])
        branch_stats[b.id] = {
            "branch_name":          b.branch_name,
            "language":             b.language,
            "country":              b.country,
            "age_category":         b.age_category,
            "segment_label":        f"{b.language or 'Any'}/{b.country or 'Any'}/{b.age_category or 'all'}",
            "sent_count":           sent,
            "opens":                opens,
            "clicks":               clicks,
            "open_rate":            round(opens / sent * 100, 1) if sent > 0 else 0.0,
            "click_rate":           round(clicks / sent * 100, 1) if sent > 0 else 0.0,
            "click_to_open_rate":   round(clicks / opens * 100, 1) if opens > 0 else 0.0,
        }

    total_sent  = sum(s["sent_count"] for s in branch_stats.values())
    total_opens = sum(s["opens"]      for s in branch_stats.values())
    total_clicks = sum(s["clicks"]     for s in branch_stats.values())

    best = max(branch_stats.values(), key=lambda x: x["open_rate"], default=None)

    return {
        "campaign_id":          campaign_id,
        "campaign_goal":        c.goal,
        "total_sent":           total_sent,
        "total_opens":          total_opens,
        "total_clicks":         total_clicks,
        "overall_open_rate":    round(total_opens / total_sent * 100, 1) if total_sent > 0 else 0.0,
        "overall_click_rate":   round(total_clicks / total_sent * 100, 1) if total_sent > 0 else 0.0,
        "best_branch":          best["branch_name"] if best else None,
        "best_open_rate":       best["open_rate"] if best else 0.0,
        "branches":             branch_stats,
    }

@app.get("/engagement/summary")
def get_all_engagement(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    events = db.query(EmailEvent).all()
    total_opens = len([e for e in events if e.event_type == "open"])
    total_clicks = len([e for e in events if e.event_type == "click"])

    seg_stats: dict = {}
    for e in events:
        label = e.segment_label or "Unknown"
        if label not in seg_stats:
            seg_stats[label] = {"opens": 0, "clicks": 0}
        seg_stats[label][e.event_type + "s"] += 1

    campaigns_with_data = len({e.campaign_id for e in events})

    return {
        "total_opens":          total_opens,
        "total_clicks":         total_clicks,
        "campaigns_with_data":  campaigns_with_data,
        "segment_breakdown":    seg_stats,
    }

# --- A/B Variant Endpoints ----------------------------------------------------

@app.get("/campaigns/{campaign_id}/branches/{branch_id}/ab-variants")
def list_ab_variants(
    campaign_id: str,
    branch_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first():
        raise HTTPException(404, "Campaign not found")
    b = (
        db.query(AudienceBranch)
        .filter(AudienceBranch.id == branch_id, AudienceBranch.campaign_id == campaign_id)
        .first()
    )
    if not b:
        raise HTTPException(404, "Branch not found")
    variants = db.query(ABVariant).filter(ABVariant.branch_id == branch_id).all()
    return [
        {
            "id":               v.id,
            "variant_name":     v.variant_name,
            "split_percentage": v.split_percentage,
            "email_subject":    v.email_subject,
            "email_body":       v.email_body,
            "status":           v.status,
            "created_at":       v.created_at.isoformat() if v.created_at else None,
        }
        for v in variants
    ]

@app.post("/campaigns/{campaign_id}/branches/{branch_id}/ab-variants", status_code=201)
def create_ab_variant(
    campaign_id: str,
    branch_id: str,
    req: ABVariantCreate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    if not _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first():
        raise HTTPException(404, "Campaign not found")
    b = (
        db.query(AudienceBranch)
        .filter(AudienceBranch.id == branch_id, AudienceBranch.campaign_id == campaign_id)
        .first()
    )
    if not b:
        raise HTTPException(404, "Branch not found")
    variant = ABVariant(
        id=str(uuid.uuid4()),
        branch_id=branch_id,
        campaign_id=campaign_id,
        variant_name=req.variant_name,
        split_percentage=req.split_percentage,
        email_subject=req.email_subject,
        email_body=req.email_body,
        status="pending",
    )
    db.add(variant)
    db.commit()
    db.refresh(variant)
    return {"variant_id": variant.id, "message": "A/B variant created"}

@app.put("/campaigns/{campaign_id}/branches/{branch_id}/ab-variants/{variant_id}")
def update_ab_variant(
    campaign_id: str,
    branch_id: str,
    variant_id: str,
    req: ABVariantUpdate,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    v = (
        db.query(ABVariant)
        .filter(
            ABVariant.id == variant_id,
            ABVariant.branch_id == branch_id,
            ABVariant.campaign_id == campaign_id,
        )
        .first()
    )
    if not v:
        raise HTTPException(404, "A/B variant not found")
    
    if req.variant_name is not None:
        v.variant_name = req.variant_name
    if req.split_percentage is not None:
        v.split_percentage = req.split_percentage
    if req.email_subject is not None:
        v.email_subject = req.email_subject
    if req.email_body is not None:
        v.email_body = req.email_body
        
    v.updated_at = datetime.utcnow()
    db.commit()
    return {"message": "A/B variant updated"}

@app.delete("/campaigns/{campaign_id}/branches/{branch_id}/ab-variants/{variant_id}")
def delete_ab_variant(
    campaign_id: str,
    branch_id: str,
    variant_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    v = (
        db.query(ABVariant)
        .filter(
            ABVariant.id == variant_id,
            ABVariant.branch_id == branch_id,
            ABVariant.campaign_id == campaign_id,
        )
        .first()
    )
    if not v:
        raise HTTPException(404, "A/B variant not found")
    
    db.delete(v)
    db.commit()
    return {"message": "A/B variant deleted"}

# --- Unsubscribe Endpoints (unauthenticated - linked in emails) ---

@app.get("/unsubscribe/{unsubscribe_token}", response_class=HTMLResponse)
def unsubscribe_page(unsubscribe_token: str, db: Session = Depends(get_db)):
    r = db.query(Recipient).filter(Recipient.unsubscribe_token == unsubscribe_token).first()
    if not r:
        return HTMLResponse(content="""
<html><body style="font-family:sans-serif;text-align:center;padding:60px">
<h2>⚠️ Invalid unsubscribe link</h2>
<p>This link is invalid or has already been used.</p>
</body></html>""", status_code=404)

    if r.is_unsubscribed:
        return HTMLResponse(content="""
<html><body style="font-family:sans-serif;text-align:center;padding:60px">
<h2>✅ Already unsubscribed</h2>
<p>You are already opted out of our emails.</p>
</body></html>""")

    return HTMLResponse(content=f"""
<html><body style="font-family:sans-serif;text-align:center;padding:60px;max-width:500px;margin:auto">
<h2>✉️ Unsubscribe</h2>
<p>Do you want to unsubscribe from all future emails?</p>
<form method="POST" action="/unsubscribe">
<input type="hidden" name="token" value="{unsubscribe_token}">
<button type="submit" 
style="background:#e53e3e;color:white;border:none;padding:12px 24px;
font-size:16px;border-radius:6px;cursor:pointer">
Yes, unsubscribe me
</button>
</form>
    <p style="margin-top:20px"><a href="/">No thanks, take me back</a></p>
</body></html>""")

@app.post("/unsubscribe", response_class=HTMLResponse)
async def process_unsubscribe(request: Request, db: Session = Depends(get_db)):
    form = await request.form()
    token = form.get("token", "")
    r = db.query(Recipient).filter(Recipient.unsubscribe_token == token).first()
    if not r:
        return HTMLResponse(content="""
<html><body style="font-family:sans-serif;text-align:center;padding:60px">
<h2>⚠️ Invalid token</h2></body></html>""", status_code=400)

    r.is_unsubscribed = True
    r.unsubscribed_at = datetime.utcnow()
    db.commit()

    logger.info(f"[UNSUBSCRIBE] recipient {r.id} opted out via token")
    return HTMLResponse(content="""
<html><body style="font-family:sans-serif;text-align:center;padding:60px">
<h2>✅ Successfully unsubscribed</h2>
<p>You have been removed from our mailing list.</p>
<p style="color:#666;font-size:14px">
You will not receive any further emails from us.<br>
If this was a mistake, please contact support.
</p>
</body></html>""")

@app.post("/unsubscribe/api")
def api_unsubscribe(recipient_id: str, db: Session = Depends(get_db)):
    """Programmatic unsubscribe - for use by ESP webhooks (unauthenticated)"""
    r = db.query(Recipient).filter(Recipient.id == recipient_id).first()
    if not r:
        raise HTTPException(404, "Recipient not found")
    r.is_unsubscribed = True
    r.unsubscribed_at = datetime.utcnow()
    db.commit()
    return {"message": "Unsubscribed"}

# --- Recipients Endpoint ------------------------------------------------------

@app.get("/recipients")
def list_recipients(
    language: Optional[str] = None,
    country: Optional[str] = None,
    age_category: Optional[str] = None,
    unsubscribed: Optional[bool] = None,
    limit: int = 100,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    q = db.query(Recipient)
    if language:
        q = q.filter(Recipient.language == language)
    if country:
        q = q.filter(Recipient.country == country)
    if age_category and age_category != "all":
        q = q.filter(Recipient.age_category == age_category)
    if unsubscribed is not None:
        q = q.filter(Recipient.is_unsubscribed == unsubscribed)
    total = q.count()
    rows = q.limit(limit).all()
    return {
        "total": total,
        "shown": len(rows),
        "recipients": [_mask_recipient(r) for r in rows],
    }

# --- Person-level Engagement (RecipientEvent) ---------------------------------

@app.get("/campaigns/{campaign_id}/engagement/v2")
def get_campaign_engagement_v2(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    c = _campaigns_query(db, current_user).filter(Campaign.id == campaign_id).first()
    if not c:
        raise HTTPException(404, "Campaign not found")

    branches = db.query(AudienceBranch).filter(
        AudienceBranch.campaign_id == campaign_id
    ).all()
    events = db.query(RecipientEvent).filter(
        RecipientEvent.campaign_id == campaign_id
    ).all()
    sends = db.query(RecipientCampaignSend).filter(
        RecipientCampaignSend.campaign_id == campaign_id,
        RecipientCampaignSend.status == "sent",
    ).all()

    branch_stats: dict = {}
    for b in branches:
        b_sends = [s for s in sends if s.branch_id == b.id]
        b_events = [e for e in events if e.branch_id == b.id]

        sent = len(b_sends)
        opens = len([e for e in b_events if e.event_type == "open"])
        clicks = len([e for e in b_events if e.event_type == "click"])
        unsubs = len([e for e in b_events if e.event_type == "unsubscribe"])

        variants = db.query(ABVariant).filter(ABVariant.branch_id == b.id).all()
        variant_stats = {}
        for v in variants:
            v_sends = len([s for s in b_sends if s.variant_id == v.id])
            v_events = [e for e in b_events if e.variant_id == v.id]
            v_opens = len([e for e in v_events if e.event_type == "open"])
            v_clicks = len([e for e in v_events if e.event_type == "click"])
            v_unsub = len([e for e in v_events if e.event_type == "unsubscribe"])
            variant_stats[v.id] = {
                "variant_name":     v.variant_name,
                "split_pct":        v.split_percentage,
                "sent":             v_sends,
                "opens":            v_opens,
                "clicks":           v_clicks,
                "unsubs":           v_unsub,
                "open_rate":        round(v_opens / v_sends * 100, 1) if v_sends > 0 else 0.0,
                "click_rate":       round(v_clicks / v_sends * 100, 1) if v_sends > 0 else 0.0,
            }

        branch_stats[b.id] = {
            "branch_name":  b.branch_name,
            "language":     b.language,
            "country":      b.country,
            "age_category": b.age_category,
            "sent":         sent,
            "opens":        opens,
            "clicks":       clicks,
            "unsubs":       unsubs,
            "open_rate":    round(opens / sent * 100, 1) if sent > 0 else 0.0,
            "click_rate":   round(clicks / sent * 100, 1) if sent > 0 else 0.0,
            "ctor":         round(clicks / opens * 100, 1) if opens > 0 else 0.0,
            "unsub_rate":   round(unsubs / sent * 100, 2) if sent > 0 else 0.0,
            "ab_variants":  variant_stats,
        }

    total_sent = sum([s["sent"] for s in branch_stats.values()])
    total_opens = sum([s["opens"] for s in branch_stats.values()])
    total_clicks = sum([s["clicks"] for s in branch_stats.values()])
    total_unsubs = sum([s["unsubs"] for s in branch_stats.values()])

    return {
        "campaign_id":          campaign_id,
        "campaign_goal":        c.goal,
        "total_sent":           total_sent,
        "total_opens":          total_opens,
        "total_clicks":         total_clicks,
        "total_unsubscribes":   total_unsubs,
        "overall_open_rate":    round(total_opens / total_sent * 100, 1) if total_sent > 0 else 0.0,
        "overall_click_rate":   round(total_clicks / total_sent * 100, 1) if total_sent > 0 else 0.0,
        "overall_unsub_rate":   round(total_unsubs / total_sent * 100, 2) if total_sent > 0 else 0.0,
        "branches":             branch_stats,
    }

# --- Agent Run Monitoring -----------------------------------------------------

@app.get("/agent-runs/active")
def get_active_agent_runs(
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    runs = (
        db.query(AgentRun)
        .filter(AgentRun.status == "running")
        .order_by(AgentRun.started_at.desc())
        .all()
    )
    return [_run_to_dict(r) for r in runs]

@app.get("/agent-runs/campaign/{campaign_id}")
def get_campaign_agent_runs(
    campaign_id: str,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    runs = (
        db.query(AgentRun)
        .filter(AgentRun.campaign_id == campaign_id)
        .order_by(AgentRun.started_at.desc())
        .all()
    )
    return [_run_to_dict(r) for r in runs]

@app.get("/agent-runs")
def get_recent_agent_runs(
    limit: int = 50,
    current_user: User = Depends(get_current_user),
    db: Session = Depends(get_db),
):
    runs = (
        db.query(AgentRun)
        .order_by(AgentRun.started_at.desc())
        .limit(limit)
        .all()
    )
    return [_run_to_dict(r) for r in runs]

def _run_to_dict(r: AgentRun) -> dict:
    return {
        "id":               r.id,
        "agent_type":       r.agent_type,
        "status":           r.status,
        "campaign_id":      r.campaign_id,
        "branch_id":        r.branch_id,
        "variant_id":       r.variant_id,
        "input_summary":    r.input_summary,
        "output_summary":   r.output_summary,
        "error_message":    r.error_message,
        "duration_ms":      r.duration_ms,
        "started_at":       r.started_at.isoformat() if r.started_at else None,
        "ended_at":         r.ended_at.isoformat() if r.ended_at else None,
    }

# --- Shared helpers -----------------------------------------------------------

def _branch_to_dict(b: AudienceBranch, db: Session = None) -> dict:
    variants = []
    if db:
        variants = [
            {
                "id":               v.id,
                "variant_name":     v.variant_name,
                "split_percentage": v.split_percentage,
                "email_subject":    v.email_subject,
                "email_body":       v.email_body,
                "status":           v.status,
            }
            for v in db.query(ABVariant).filter(ABVariant.branch_id == b.id).all()
        ]

    return {
        "id":               b.id,
        "branch_name":      b.branch_name,
        "language":         b.language,
        "country":          b.country,
        "age_category":     b.age_category,
        "custom_query":     b.custom_query,
        "email_subject":    b.email_subject,
        "email_body":       b.email_body,
        "scheduled_at":     b.scheduled_at.isoformat() if b.scheduled_at else None,
        "status":           b.status,
        "sent_count":       b.sent_count,
        "variants":         variants,
    }

def parse_scheduled_at(value: Optional[str]) -> Optional[datetime]:
    if not value or value.strip() == "":
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        raise HTTPException(400, f"Invalid scheduled_at: '{value}'. Use ISO format e.g. 2026-03-15T09:00:00")