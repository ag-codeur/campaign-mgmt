"""
Chat Agent - context-aware AI assistant for campaign management guidance.

Provides answers grounded in live database state:
• Campaign status, branch configuration, engagement metrics
• Workflow navigation ("what do I do next?")
• Data Q&A ("what's the open rate on my latest campaign?")
• A/B test results comparison.

Logs every invocation to the AgentRun table so the monitor dashboard can
display chat agent activity alongside the planning/creation/execution pipeline.
"""

import uuid
from datetime import datetime
from loguru import logger

from core.config import get_settings

settings = get_settings()

def run_chat(
    message: str,
    context: dict,
    history: list,
    run_id: str | None = None,
) -> str:
    """
    Generate a response to the user's message.

    context keys (all optional):
        user             - dict with id, name, email, manager_name
        campaigns        - list of campaign summary dicts
        current_campaign - full campaign dict (if one is selected)
        pending_count    - int, number of approvals pending for this user
        engagement       - dict, engagement summary for selected campaign

    history - list of {"role": "user"|"assistant", "content": "..."} (last N turns)
    run_id - AgentRun.id for logging progress back to the DB
    """
    from langchain_groq import ChatGroq
    from core.database import SessionLocal, AgentRun

    db = SessionLocal()

    def _update_run(status, output=None, error=None):
        if run_id:
            try:
                run = db.query(AgentRun).filter(AgentRun.id == run_id).first()
                if run:
                    run.status = status
                    run.ended_at = datetime.utcnow()
                    if run.started_at:
                        run.duration_ms = str(
                            int((run.ended_at - run.started_at).total_seconds() * 1000)
                        )
                    if output:
                        run.output_summary = str(output)[:500]
                    if error:
                        run.error_message = str(error)[:500]
                    db.commit()
            except Exception:
                pass

    try:
        ctx_lines: list[str] = [
            "You are a helpful AI assistant embedded in a Campaign Management System.",
            "Be concise, friendly, and use bullet points for step-by-step guidance.",
            "",
            "=== CAMPAIGN WORKFLOW ===",
            "Draft -> Branches -> Schedule -> Submit -> Manager Approval",
            "-> AI Planning (HITL review) -> AI Content per Branch/Variant (HITL review)",
            "-> Execute -> Rate -> Feedback Agent evaluates + updates Knowledge Base",
            "",
            "Status glossary:",
            "  draft                    - being created",
            "  awaiting_manager_approval - submitted, manager must approve",
            "  awaiting_plan_approval    - AI strategy ready for review",
            "  awaiting_content_approval - AI emails ready for review",
            "  scheduled                - approved, waiting for send time",
            "  executing                - emails being sent",
            "  awaiting_rating          - user must rate results",
            "  completed                - evaluated and done",
            ""
        ]

        # Inject live user context
        user = context.get("user")
        if user:
            ctx_lines += [
                "=== CURRENT USER ===",
                f"Name: {user.get('name')} | Email: {user.get('email')}",
                f"Manager: {user.get('manager_name') or 'None (top of hierarchy)'}",
                "",
            ]

        pending = context.get("pending_count", 0)
        if pending:
            ctx_lines.append(f"PENDING APPROVALS: {pending} campaign(s) awaiting your approval.")

        # Inject campaigns list
        campaigns = context.get("campaigns", [])
        if campaigns:
            ctx_lines.append(f"=== USER CAMPAIGNS ({len(campaigns)}) ===")
            for c in campaigns[:8]:
                ctx_lines.append(
                    f"  [{c.get('status')}] {c.get('goal', '')[:55]} (ID: {c.get('id', '')[:8]}...)"
                )
            ctx_lines.append("")

        # Inject selected campaign detail
        cc = context.get("current_campaign")
        if cc:
            ctx_lines += [
                "=== SELECTED CAMPAIGN ===",
                f"ID: {cc.get('id')}",
                f"Goal: {cc.get('goal')}",
                f"Status: {cc.get('status')}",
                f"Branches: {len(cc.get('branches', []))}",
            ]

            if cc.get("execution_results"):
                ctx_lines.append(f"Emails sent: {cc['execution_results'].get('total_sent', 0)}")
            
            eng = context.get("engagement")
            if eng:
                ctx_lines += [
                    f"Opens: {eng.get('total_opens', 0)} ({eng.get('overall_open_rate', 0):.1f}%)",
                    f"Clicks: {eng.get('total_clicks', 0)} ({eng.get('overall_click_rate', 0):.1f}%)",
                    f"Unsubscribes: {eng.get('total_unsubscribes', 0)}",
                ]
            
            ab = eng.get("ab_results", {}) if eng else {}
            if ab:
                ctx_lines.append("A/B results:")
                for vid, vr in ab.items():
                    ctx_lines.append(
                        f"  Variant {vr.get('variant_name')}: "
                        f"open {vr.get('open_rate', 0):.1f}% click {vr.get('click_rate', 0):.1f}%"
                    )
            ctx_lines.append("")

        ctx_lines.append(
            "Answer based on the data above. If data is missing say you don't have it yet."
        )

        system_prompt = "\n".join(ctx_lines)
        messages = [("system", system_prompt)]
        
        for h in history[-8:]:
            role = "human" if h.get("role") == "user" else "ai"
            if h.get("content"):
                messages.append((role, h["content"]))
        
        messages.append(("human", message))

        llm = ChatGroq(api_key=settings.groq_api_key, model="llama-3.1-8b-instant", temperature=0.5)
        response = llm.invoke(messages)
        reply = response.content

        _update_run("completed", output=reply[:200])
        return reply

    except Exception as e:
        logger.error(f"[CHAT_AGENT] Error: {e}")
        _update_run("failed", error=str(e))
        return "I'm having trouble connecting right now. Please try again shortly."
    finally:
        db.close()