# Campaign Management System — Agentic AI

Multi-agent email campaign management system powered by **LangGraph + Groq + ChromaDB**.

## Key Features

| Feature | Description |
| --- | --- |
| **Authentication & RBAC** | Username + password login, server-side session tokens, 4-role model (admin / leader / approver / campaigner). |
| **Business Unit Isolation** | Approvers and campaigners see only their own BU's campaigns; admins and leaders see all. |
| **Secure Onboarding** | Approvers can create campaigners in their BU; admins can create any role; all new users must change password on first login. |
| **PII Masking** | Audience contact emails and names are masked (`***@***`, `***`) in all API responses. |
| **Email-only Simulation** | Single-channel focus - currently implements **simulated delivery** for local development. |
| **Audience Branching** | Segment by language, country, age, or custom SQL query. |
| **AI Suggestions** | Agents suggest branches and audience queries — all editable. |
| **Per-branch Scheduler** | Each audience segment can be targeted at a different send time. |
| **Submit -> Manager Approval** | Campaigns go to the creator's line manager (from DB) before AI starts. |
| **AI HITL Review** | Human reviews AI strategy and per-branch email content. |
| **Email Language Translation** | Reviewers select their language to understand any-language email before approving. |
| **A/B Testing** | Split branches across multiple email variants (e.g. 80:20) — AI generates distinct content per variant. |
| **Agent Monitor** | Live pipeline visualization — see which agents are running for which campaign/branch/variant. |
| **Engagement Analytics** | Open/click/unsubscribe rates per branch and A/B variant; best-segment comparison. |
| **Guardrails** | 6 rule categories (PII Detection, Deceptive/Manipulative, Urgency/Scarcity, Financial Promises, Spam Indicators, Health Claims) — validated on every AI-generated email. |
| **RAI Evaluation** | Admin-only dashboard: content safety score, HITL compliance rate, manager oversight rate, audience unsubscribe rate — backed by persistent `GuardrailCheck` records. |
| **Feedback Loop** | AI learns from every campaign via ChromaDB knowledge base. |

## Technical Stack

*   **API**: FastAPI (v5.0 with session-based authentication)
*   **UI**: Streamlit (10+ tabs including Monitoring, AI Review, History, and Analytics)
*   **LLM**: Groq (Llama3) via LangChain/LangGraph
*   **Database**: SQLite (SQLAlchemy ORM)
*   **Knowledge Base**: ChromaDB (Vector store for agent feedback loop)

## Folder Structure

```text
campaign-mgmt/
├── agents/
│   ├── supervisor.py       # Orchestrates workflow + AgentRun instrumentation
│   ├── planner.py          # Strategy generation + branch/query suggestions
│   ├── creator.py          # Per-branch + per-variant email content generation
│   ├── executor.py         # Simulated delivery with dedup, A/B split, unsub check
│   ├── feedback.py         # Evaluation + knowledge base update
│   ├── chat_agent.py       # AI chat assistant (runs via AgentRun)
│   └── translate_agent.py  # Email translation (runs via AgentRun)
├── api/
│   └── main.py             # FastAPI - 50+ endpoints (v5.0 with auth)
├── core/
│   ├── auth.py             # Password hashing (bcrypt) + session token management
│   ├── config.py           # Settings via .env
│   ├── database.py         # SQLAlchemy models + migrations + seed users
│   └── knowledge_base.py   # ChromaDB vector store
├── guardrails/
│   └── rails.py            # PII detection + brand-safety checks
├── ui/
│   └── app.py              # Streamlit UI - 10 tabs + auth gate + sidebar chat
└── deploy/
    └── local/              # Docker Compose for local API + UI deployment
```

## Campaign Workflow

1.  **Sign In**: Email + password (must change temp password first).
2.  **Create Draft**: Goal + Audience (campaigner or admin only).
3.  **Audience Branches**:
    *   Language / Country / Age / Custom Query.
    *   AI suggests -> User edits and accepts.
    *   **A/B Variants**: Add variants (e.g. 80% Variant A, 20% Variant B).
4.  **Schedule**: Per-branch send time (optional).
5.  **Submit**: Goes to creator's line manager (from DB).
6.  **Manager Approves**: AI Planning starts (approver or admin).
7.  **[HITL] Review Strategy**: Approve / Reject AI plan.
8.  **[HITL] Review Content**: Approve / Reject per-branch emails.
9.  **Execute**: Emails processed per branch/variant (currently simulated).
10. **Rate Results**: good / neutral / poor.
11. **Feedback Agent**: Evaluates + updates Knowledge Base.

## Database Models

| Model | Purpose |
| --- | --- |
| **User** | Platform user – name, email, role, business_unit, hashed_password, manager hierarchy. |
| **AuthSession** | Server-side session tokens (uuid4, 8h TTL, revocable). |
| **Campaign** | Email campaign with full status lifecycle + business_unit stamp. |
| **AudienceBranch** | Audience segment with targeting + generated email content. |
| **ABVariant** | A/B variant within a branch (split %, AI-generated email). |
| **Recipient** | Audience contact – segmentation attributes + unsubscribe state (PII masked in API). |
| **RecipientCampaignSend** | Dedup record – one row per recipient per campaign. |
| **RecipientEvent** | Per-person engagement event (open / click / unsubscribe). |
| **AgentRun** | Agent execution record for monitoring dashboard. |
| **ChatMessage** | AI assistant chat history per user session. |
| **GuardrailCheck** | Per-email guardrail result (PII + brand-safety issues, pass/fail). |

## Run Locally (Quick Start)

### 1. Setup

```bash
cd campaign-mgmt
cp .env.example .env
# Edit .env — add your GROQ_API_KEY and LANGSMITH_API_KEY
pip install -r requirement.txt
```

### 2. Start servers

**Terminal 1 – API**

```bash
uvicorn api.main:app --reload --port 8000
```

**Terminal 2 – UI**

```bash
streamlit run ui/app.py
```

### 3. Access

*   **UI**: [http://localhost:8501](http://localhost:8501)
*   **API**: [http://localhost:8000](http://localhost:8000)
*   **API Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)

## Keys to Get (Free)

| Key | URL |
| --- | --- |
| **Groq API** | [https://console.groq.com](https://console.groq.com) |
| **LangSmith** | [https://smith.langchain.com](https://smith.langchain.com) |

---
