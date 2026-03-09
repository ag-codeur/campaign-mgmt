"""
Supervisor - orchestrates the email campaign workflow.

Stage flow (after manager approval):
1. run_campaign_workflow -> planning         -> awaiting_plan_approval
2. run_content_creation  -> creating_content -> awaiting_content_approval
   (branches processed in parallel via asyncio + ThreadPoolExecutor)
3. run_execution         -> executing        -> awaiting_rating
4. run_evaluation        -> evaluating       -> completed

Every stage is instrumented with AgentRun records so the monitoring dashboard
can show which agents are active for which campaigns/branches/variants in real time.
"""

import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from loguru import logger

from agents.planner import run_planner
from agents.creator import run_creator_single
from agents.executor import run_executor
from agents.feedback import run_feedback
from core.database import (
    SessionLocal, Campaign, AudienceBranch, ABVariant, AgentRun, GuardrailCheck,
)

# --- AgentRun helpers ---

def _begin_run(db, agent_type, campaign_id=None, branch_id=None, variant_id=None,
               input_summary=None) -> AgentRun:
    run = AgentRun(
        id=str(uuid.uuid4()),
        campaign_id=campaign_id,
        branch_id=branch_id,
        variant_id=variant_id,
        agent_type=agent_type,
        status="running",
        input_summary=str(input_summary)[:500] if input_summary else None,
        started_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()
    return run

def _end_run(db, run: AgentRun, status="completed", output=None, error=None):
    run.status = status
    run.ended_at = datetime.utcnow()
    if run.started_at:
        run.duration_ms = str(int((run.ended_at - run.started_at).total_seconds() * 1000))
    if output:
        run.output_summary = str(output)[:500]
    if error:
        run.error_message = str(error)[:500]
    db.commit()

# --- Internal helpers ---

def _get_campaign(db, campaign_id: str) -> Campaign:
    c = db.query(Campaign).filter(Campaign.id == campaign_id).first()
    if not c:
        raise ValueError(f"Campaign {campaign_id} not found")
    return c

def _update(db, campaign, **kwargs):
    for k, v in kwargs.items():
        setattr(campaign, k, v)
    campaign.updated_at = datetime.utcnow()
    db.commit()

def _get_branches_as_dicts(db, campaign_id: str) -> list:
    """
    Return all branches for a campaign as plain dicts.
    Includes A/B variants for each branch so creator and executor
    can act on them without re-querying.
    """
    rows = (
        db.query(AudienceBranch)
        .filter(AudienceBranch.campaign_id == campaign_id)
        .all()
    )
    result = []
    for b in rows:
        variants = [
            {
                "id": v.id,
                "name": v.variant_name,
                "split_pct": float(v.split_percentage or 100),
                "subject": v.email_subject,
                "body": v.email_body,
            }
            for v in db.query(ABVariant).filter(ABVariant.branch_id == b.id).all()
        ]
        result.append({
            "id": b.id,
            "branch_name": b.branch_name,
            "language": b.language,
            "country": b.country,
            "age_category": b.age_category,
            "custom_query": b.custom_query,
            "email_subject": b.email_subject,
            "email_body": b.email_body,
            "scheduled_at": b.scheduled_at,
            "status": b.status,
            "variants": variants, # populated only when A/B test configured
        })
    return result

# --- Workflow stages ---

async def run_campaign_workflow(campaign_id: str):
    """Stage 1 - AI Planner generates strategy (triggered after manager approval)."""
    db = SessionLocal()
    run = None
    try:
        c = _get_campaign(db, campaign_id)
        _update(db, c, status="planning")
        run = _begin_run(db, "planner", campaign_id=campaign_id,
                         input_summary=f"goal={c.goal[:80]}")
        
        branches = _get_branches_as_dicts(db, campaign_id)
        strategy = run_planner(c.goal, c.audience, branches)

        _update(db, c, strategy=strategy, status="awaiting_plan_approval")
        _end_run(db, run, output=f"name={strategy.get('campaign_name', '')[:60]}")
        logger.info(f"[SUPERVISOR] Plan ready for campaign {campaign_id}")

    except Exception as e:
        logger.error(f"[SUPERVISOR] Planning failed for {campaign_id}: {e}")
        if run:
            _end_run(db, run, status="failed", error=str(e))
        db2 = SessionLocal()
        try:
            _update(db2, _get_campaign(db2, campaign_id), status="failed")
        finally:
            db2.close()
    finally:
        db.close()

async def run_content_creation(campaign_id: str):
    """
    Stage 2 - AI Creator generates personalised email per branch (and per A/B variant).
    
    Branches are processed in parallel using a ThreadPoolExecutor so campaigns
    with many segments finish content generation faster.
    """
    db = SessionLocal()
    top_run = None
    try:
        c = _get_campaign(db, campaign_id)
        _update(db, c, status="creating_content")
        top_run = _begin_run(db, "creator", campaign_id=campaign_id,
                             input_summary=f"{c.goal[:60]}")
        
        branches = _get_branches_as_dicts(db, campaign_id)
        strategy = c.strategy or {}

        # --- Parallel per-branch content generation ---
        from langchain_groq import ChatGroq
        from core.knowledge_base import query_kb
        from core.config import get_settings
        _s = get_settings()
        llm = ChatGroq(api_key=_s.groq_api_key, model="llama3-8b-8192", temperature=0.7)
        past = query_kb(f"campaign email content: {strategy.get('objective', '')}")
        kb_context = "\n".join(past) if past else "No past content found."

        loop = asyncio.get_event_loop()
        
        def _gen_branch(branch):
            return run_creator_single(strategy, branch, llm=llm, kb_context=kb_context)

        with ThreadPoolExecutor(max_workers=min(len(branches), 4)) as pool:
            tasks = [loop.run_in_executor(pool, _gen_branch, b) for b in branches]
            branch_contents = await asyncio.gather(*tasks)

        # --- Persist generated content ---
        db2 = SessionLocal() # fresh session after async gap
        try:
            for bc in branch_contents:
                branch = db2.query(AudienceBranch).filter(
                    AudienceBranch.id == bc["branch_id"]
                ).first()
                if branch:
                    branch.email_subject = bc.get("subject", "")
                    branch.email_body = bc.get("body", "")
                    branch.updated_at = datetime.utcnow()

                # Persist per-variant content to ABVariant rows
                for vc in bc.get("variants", []):
                    variant = db2.query(ABVariant).filter(ABVariant.id == vc.get("id")).first()
                    if variant:
                        variant.email_subject = vc.get("subject", "")
                        variant.email_body = vc.get("body", "")
                        variant.status = "ready"
                        variant.updated_at = datetime.utcnow()

            c2 = db2.query(Campaign).filter(Campaign.id == campaign_id).first()
            if c2:
                c2.status = "awaiting_content_approval"
                c2.updated_at = datetime.utcnow()
            
            # --- Persist guardrail check results ---
            for bc in branch_contents:
                b_id = bc["branch_id"]
                entries = []
                if bc.get("variants"):
                    for vc in bc["variants"]:
                        entries.append((b_id, vc.get("id"), vc.get("guardrail_warnings") or []))
                else:
                    entries.append((b_id, None, bc.get("guardrail_warnings") or []))
                
                for bid, vid, issues in entries:
                    pii = [i for i in issues if i.startswith("PII")]
                    brand = [i for i in issues if i.startswith("Brand safety")]
                    db2.add(GuardrailCheck(
                        campaign_id=campaign_id, branch_id=bid, variant_id=vid,
                        passed=len(issues) == 0,
                        pii_issues=pii, brand_safety_issues=brand,
                        total_issues=str(len(issues)),
                        checked_at=datetime.utcnow(),
                    ))

            db2.commit()
            
            run2 = db2.query(AgentRun).filter(AgentRun.id == top_run.id).first()
            if run2:
                _end_run(db2, run2, 
                         output=f"{len(branch_contents)} branches processed")
        finally:
            db2.close()

        logger.info(f"[SUPERVISOR] Email content ready for all branches of {campaign_id}")

    except Exception as e:
        logger.error(f"[SUPERVISOR] Content creation failed for {campaign_id}: {e}")
        if top_run:
            try:
                _end_run(db, top_run, status="failed", error=str(e))
            except Exception:
                pass
        db2 = SessionLocal()
        try:
            _update(db2, _get_campaign(db2, campaign_id), status="failed")
        finally:
            db2.close()
    finally:
        db.close()

async def run_execution(campaign_id: str):
    """Stage 3 - Executor sends emails per branch (respects scheduled_at and A/B variants)."""
    db = SessionLocal()
    run = None
    try:
        c = _get_campaign(db, campaign_id)
        _update(db, c, status="executing")
        run = _begin_run(db, "executor", campaign_id=campaign_id,
                         input_summary=f"branches={len(_get_branches_as_dicts(db, campaign_id))}")
        db.close() # executor opens its own session

        results = run_executor(campaign_id, branches=[])

        db3 = SessionLocal()
        try:
            # Update branch sent_counts
            for branch_id, br in results.get("branch_results", {}).items():
                b = db3.query(AudienceBranch).filter(AudienceBranch.id == branch_id).first()
                if b:
                    b.status = "sent"
                    b.sent_count = str(br.get("sent_count", 0))
                    b.updated_at = datetime.utcnow()
            
            c3 = db3.query(Campaign).filter(Campaign.id == campaign_id).first()
            if c3:
                c3.status = "awaiting_rating"
                c3.updated_at = datetime.utcnow()
            db3.commit()

            run3 = db3.query(AgentRun).filter(AgentRun.id == run.id).first()
            if run3:
                _end_run(db3, run3, output=f"total_sent={results.get('total_sent', 0)}")
        finally:
            db3.close()

        logger.info(f"[SUPERVISOR] Execution complete for campaign {campaign_id}")

    except Exception as e:
        logger.error(f"[SUPERVISOR] Execution failed for {campaign_id}: {e}")
        if run:
            db_e = SessionLocal()
            run_e = db_e.query(AgentRun).filter(AgentRun.id == run.id).first()
            if run_e:
                _end_run(db_e, run_e, status="failed", error=str(e))
            db_e.close()
    finally:
        pass

async def run_evaluation(campaign_id: str, rating: str):
    """Stage 4 - Feedback Agent evaluates results and updates knowledge base."""
    db = SessionLocal()
    run = None
    try:
        c = _get_campaign(db, campaign_id)
        _update(db, c, status="evaluating")
        run = _begin_run(db, "feedback", campaign_id=campaign_id,
                         input_summary=f"rating={rating}")
        
        branches = _get_branches_as_dicts(db, campaign_id)
        evaluation = run_feedback(campaign_id, c.strategy, branches, rating)

        _update(db, c, evaluation=evaluation, status="completed")
        _end_run(db, run, output=f"score={evaluation.get('performance_score')}")
        logger.info(f"[SUPERVISOR] Campaign {campaign_id} completed successfully")

    except Exception as e:
        logger.error(f"[SUPERVISOR] Evaluation failed for {campaign_id}: {e}")
        if run:
            _end_run(db, run, status="failed", error=str(e))
        db2 = SessionLocal()
        _update(db2, _get_campaign(db2, campaign_id), status="failed")
        db2.close()
    finally:
        db.close()