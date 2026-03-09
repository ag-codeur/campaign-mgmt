"""
Campaign Management System – Streamlit UI (v5.0)

Email-only campaigns with:
• Username + password authentication with server-side session tokens
• 4-role model: admin, leader, approver, campaigner
• BU isolation: approver/campaigner see only their own BU
• Must-change-password gate on first login
• Audience branching by language / country / age / custom SQL query
• AI branch suggestions (editable)
• Per-branch email scheduler
• Submit -> Manager Approval workflow
• AI HITL review for strategy and email content
• Email language translation
• 🤖 AI Chat Assistant
• 🖋️ A/B Testing
• ✅ Engagement Analytics
• 🕵️ Agent Monitor
• 👤 User Management (admin + approver)
"""
import time
import requests
import streamlit as st
from datetime import datetime, timedelta

API = "http://localhost:8000"

st.set_page_config(
    page_title="Campaign Management",
    page_icon="📧",
    layout="wide",
)

# --- Session state initialisation -----------------------------------------

if "auth_token" not in st.session_state:
    st.session_state["auth_token"] = None
if "auth_user" not in st.session_state:
    st.session_state["auth_user"] = None


# --- API helpers ----------------------------------------------------------

def _auth_headers() -> dict:
    token = st.session_state.get("auth_token")
    return {"X-Session-Token": token} if token else {}


def _clear_auth():
    st.session_state["auth_token"] = None
    st.session_state["auth_user"] = None


def api_get(path: str, timeout: int = 10):
    try:
        r = requests.get(f"{API}{path}", headers=_auth_headers(), timeout=timeout)
        if r.status_code == 401 and st.session_state.get("auth_token"):
            _clear_auth()
            st.rerun()
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error ({path}): {e}")
        return None


def api_post(path: str, payload: dict = None, timeout: int = 30):
    try:
        r = requests.post(f"{API}{path}", json=payload or {}, headers=_auth_headers(), timeout=timeout)
        if r.status_code == 401 and st.session_state.get("auth_token"):
            _clear_auth()
            st.rerun()
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error ({path}): {e}")
        return None


def api_put(path: str, payload: dict = None, timeout: int = 10):
    try:
        r = requests.put(f"{API}{path}", json=payload or {}, headers=_auth_headers(), timeout=timeout)
        if r.status_code == 401 and st.session_state.get("auth_token"):
            _clear_auth()
            st.rerun()
        r.raise_for_status()
        return r.json()
    except Exception as e:
        st.error(f"API error ({path}): {e}")
        return None

def api_delete(path: str, timeout: int = 10):
    try:
        r = requests.delete(f"{API}{path}", headers=_auth_headers(), timeout=timeout)
        if r.status_code == 401 and st.session_state.get("auth_token"):
            _clear_auth()
            st.rerun()
        r.raise_for_status()
        return r.json()
    except Exception as e:
       st.error(f"API error ({path}): {e}")
       return None


# --- Display helpers ------------------------------------------------------

STATUS_ICONS = {
   "draft": "📂", "submitted": "📧", "awaiting_manager_approval": "🕒",
   "approved": "✅", "rejected": "❌", "planning": "⏳",
   "awaiting_plan_approval": "🕒", "creating_content": "🏗️",
   "awaiting_content_approval": "🕒", "scheduled": "📅",
   "executing": "🚀", "awaiting_rating": "🕒",
   "evaluating": "📊", "completed": "🟢", "failed": "🔴",
}

LANGUAGE_OPTIONS = [
   "", "English", "French", "Spanish", "German", "Portuguese",
   "Italian", "Dutch", "Japanese", "Chinese", "Arabic", "Hindi",
]

COUNTRY_OPTIONS = [
   "", "US", "GB", "FR", "DE", "ES", "IT", "CA", "AU",
   "IN", "JP", "BR", "MX", "NL", "SG",
]

AGE_OPTIONS = ["all", "18-25", "26-35", "36-50", "50+"]

def status_badge(status: str) -> str:
   icon = STATUS_ICONS.get(status, "⚪")
   label = status.replace("_", " ").title()
   return f"{icon} {label}"

def fmt_dt(dt_str):
   if not dt_str or dt_str == "None":
       return "-"
   try:
       return datetime.fromisoformat(dt_str[:19]).strftime("%Y-%m-%d %H:%M")
   except Exception:
       return dt_str[:16]

def render_agent_run(r: dict):
   """Render an agent run record as an expander."""
   status_colors = {"running": "🟡", "completed": "🟢", "failed": "🔴"}
   icon = status_colors.get(r.get("status"), "⚪️")
   agent_icons = {
       "planner": "📝", "creator": "🎨", "executor": "🚀",
       "feedback": "📊", "chat": "💬", "translate": "🌐",
   }
   a_icon = agent_icons.get(r.get("agent_type"), "🤖")
   dur = f"{r['duration_ms']}ms" if r.get("duration_ms") else "running..."
   started = fmt_dt(r.get("started_at"))
   label = (
       f"{icon} {a_icon} **{r.get('agent_type', '?').title()}** — "
       f"{r.get('status', '?')} • {dur} • {started}"
   )
   with st.expander(label, expanded=(r.get("status") == "running")):
       c1, c2, c3 = st.columns(3)
       c1.caption(f"**Campaign:** `{r.get('campaign_id', '-')[:20]}`")
       c2.caption(f"**Branch:** `{r.get('branch_id', '-') or '-'}`")
       c3.caption(f"**Variant:** `{r.get('variant_id', '-') or '-'}`")
       if r.get("input_summary"):
           st.markdown(f"**Input:** {r['input_summary']}")
       if r.get("output_summary"):
           st.success(f"**Output:** {r['output_summary']}")
       if r.get("error_message"):
           st.error(f"**Error:** {r['error_message']}")


# --- RENDERING GATE - LOGIN / CHANGE-PASSWORD / APP -----------------------

auth_token = st.session_state.get("auth_token")
auth_user = st.session_state.get("auth_user")

# --- LOGIN SCREEN ---------------------------------------------------------
if not auth_token:
   st.title("📧 Campaign Management")
   st.subheader("Sign In")
   with st.form("login_form"):
       email = st.text_input("Email", placeholder="alice@company.com")
       password = st.text_input("Password", type="password")
       login_btn = st.form_submit_button("Sign In", type="primary")

   if login_btn:
       if not email.strip() or not password:
           st.warning("Please enter your email and password.")
       else:
           try:
               resp = requests.post(
                   f"{API}/auth/login",
                   json={"email": email.strip(), "password": password},
                   timeout=10,
               )
               if resp.status_code == 401:
                   st.error("Invalid email or password.")
               elif resp.ok:
                   data = resp.json()
                   st.session_state["auth_token"] = data["token"]
                   st.session_state["auth_user"] = data["user"]
                   st.rerun()
               else:
                   st.error(f"Login failed: {resp.text}")
           except Exception as e:
               st.error(f"Cannot reach API. Is the server running? ({e})")
   st.stop()

# --- CHANGE PASSWORD SCREEN -----------------------------------------------
if auth_user and auth_user.get("must_change_password"):
   st.title("🔑 Change Your Password")
   st.info(
       f"Welcome, **{auth_user['name']}**! "
       "You must set a new password before continuing."
   )
   with st.form("change_pw_form"):
       old_pw = st.text_input("Current (temporary) password", type="password")
       new_pw = st.text_input("New password", type="password")
       confirm_pw = st.text_input("Confirm new password", type="password")
       change_btn = st.form_submit_button("Change Password", type="primary")

   if change_btn:
       if not old_pw or not new_pw or not confirm_pw:
           st.warning("Please fill in all fields.")
       elif new_pw != confirm_pw:
           st.error("New passwords do not match.")
       elif len(new_pw) < 6:
           st.error("New password must be at least 6 characters.")
       else:
           try:
               resp = requests.post(
                   f"{API}/auth/change-password",
                   json={"old_password": old_pw, "new_password": new_pw},
                   headers={"X-Session-Token": auth_token},
                   timeout=10,
               )
               if resp.ok:
                   st.session_state["auth_user"]["must_change_password"] = False
                   st.success("Password changed! Loading the app...")
                   time.sleep(1)
                   st.rerun()
               else:
                   detail = resp.json().get("detail", resp.text)
                   st.error(f"Error: {detail}")
           except Exception as e:
               st.error(f"Request failed: {e}")
   st.stop()

# --- MAIN APP (authenticated + password already changed) ------------------

current_user = auth_user # dict: {id, name, email, role, business_unit, ...}
user_role = current_user.get("role", "campaigner")
user_bu = current_user.get("business_unit")

# --- Sidebar --------------------------------------------------------------

with st.sidebar:
    st.title("📧 Campaign Mgmt")
    st.caption("Email Marketing Platform")
    st.divider()

    # User identity
    bu_label = user_bu if user_bu else "Cross-BU"
    st.markdown(f"**{current_user['name']}**")
    st.caption(f"{user_role.title()} • {bu_label}")

    # Sign out
    if st.button("🚪 Sign Out", use_container_width=True):
       try:
           requests.post(
               f"{API}/auth/logout",
               headers={"X-Session-Token": auth_token},
               timeout=5,
           )
       except Exception:
           pass
       _clear_auth()
       st.rerun()

    st.divider()

   # Pending approvals (only for approver / admin / leader)
    if user_role in ["approver", "admin", "leader"]:
       pending_approvals = api_get(f"/campaigns/pending-approval/{current_user['id']}") or []
       if pending_approvals:
           st.warning(f"🔔 **{len(pending_approvals)}** campaign(s) awaiting your approval")
       else:
           st.success("✅ No pending approvals")

    # My campaigns that need my action
    all_cmp = api_get("/campaigns") or []
    my_action = [
        c for c in all_cmp
        if c.get("created_by") == current_user["id"]
        and c["status"] in (
            "rejected", "awaiting_plan_approval",
            "awaiting_content_approval", "awaiting_rating",
        )
    ]
    if my_action:
        st.info(f"🔔 **{len(my_action)}** campaign(s) need your attention")

    # -- AI Chat Assistant ----------------------------------------------------
    st.divider()
    st.markdown("### 🤖 AI Assistant")
    st.caption("Ask about campaigns, data, or workflow steps.")

    if "chat_messages" not in st.session_state:
        st.session_state.chat_messages = []

    chat_display = st.container()
    with chat_display:
        recent = st.session_state.chat_messages[-6:]
        if not recent:
            st.caption("_No messages yet. Try: 'Walk me through creating a campaign'_")
        for msg in recent:
            if msg["role"] == "user":
                st.markdown(
                    f"<div style='background:#e8f4f8;padding:6px 10px;border-radius:8px;"
                    f"margin:4px 0;font-size:0.85em'><b>You:</b> {msg['content']}</div>",
                    unsafe_allow_html=True,
                )
            else:
                st.markdown(
                    f"<div style='background:#f0f7f0;padding:6px 10px;border-radius:8px;"
                    f"margin:4px 0;font-size:0.85em'><b>AI:</b> {msg['content']}</div>",
                    unsafe_allow_html=True,
                )

    chat_input = st.text_area(
        "chat_input_area",
        height=68,
        placeholder="Ask anything.. e.g, 'What's the status of my campaigns?'",
        label_visibility="collapsed",
        key="chat_input_area",
    )
    c_send, c_clear = st.columns([3, 1])
    if c_send.button("Send", key="chat_send_btn", use_container_width=True):
        msg_text = chat_input.strip()
        if msg_text:
            with st.spinner("Thinking..."):
                resp = None
                try:
                    resp = requests.post(
                        f"{API}/chat",
                        json={
                            "message": msg_text,
                            "user_id": current_user["id"],
                            "campaign_id": st.session_state.get("selected_campaign_id"),
                            "history": st.session_state.chat_messages[-8:],
                        },
                        headers=_auth_headers(),
                        timeout=30,
                    )
                    resp.raise_for_status()
                    data = resp.json()
                except Exception as e:
                    data = {"response": f"Error: {e}"}

                st.session_state.chat_messages.append({"role": "user", "content": msg_text})
                st.session_state.chat_messages.append({"role": "assistant", "content": data.get("response", "")})
                st.rerun()

    if c_clear.button("Clear", key="chat_clear_btn", use_container_width=True):
        st.session_state.chat_messages = []
        st.rerun()

# -- Fetch campaigns + pre-compute shared state ---------------------------

all_campaigns = api_get("/campaigns") or []
my_campaigns = [
    c for c in all_campaigns
    if c.get("created_by") == current_user["id"]
]
my_cmp_ids = [c["id"] for c in my_campaigns]

sel_options: dict = {}
if my_cmp_ids:
    sel_options = {
        c["id"]: f"{c['goal'][:55]}... ({c['status']})" for c in my_campaigns
    }

if "selected_campaign_id" not in st.session_state:
    st.session_state.selected_campaign_id = my_cmp_ids[0] if my_cmp_ids else None
elif st.session_state.selected_campaign_id not in sel_options and my_cmp_ids:
    st.session_state.selected_campaign_id = my_cmp_ids[0]

# -- Role-gated tab list --------------------------------------------------

show_create    = user_role in ("campaigner", "admin")
show_manager   = user_role in ("approver", "admin", "leader")
show_users     = user_role in ("admin", "approver")
show_rai       = user_role == "admin"

tab_labels = ["🆕 Create Campaign", "🚀 Audience Branches", "📅 Schedule & Submit",
              "✅ Manager Queue", "🤖 AI Review", "📊 Engagement",
              "🕵️ Agent Monitor", "📜 History", "👥 Users", "🤖 RAI Evaluation"]

tabs = st.tabs(tab_labels)
(tab_create, tab_branches, tab_schedule, tab_manager, tab_ai,
tab_engagement, tab_monitor, tab_history, tab_users, tab_rai) = tabs

# --------------------------------------------------------------------------
# TAB 1 - CREATE CAMPAIGN
# --------------------------------------------------------------------------

with tab_create:
    if not show_create:
        st.info("Campaign creation is available to campaigners and admins.")
    else:
        st.header("Create New Email Campaign")
        st.info(
            "**Email-only platform.** "
            "After saving a draft, go to **Audience Branches** to configure segments, "
            "then **Schedule & Submit** to send for manager approval."
        )

        with st.form("create_campaign_form"):
            goal = st.text_area(
                "Campaign Goal *",
                placeholder="e.g, Increase newsletter sign-ups by 20% in Q1 among urban professionals",
                height=100,
            )
            audience = st.text_area(
                "Target Audience Description *",
                placeholder="e.g. 25-45 year old urban professionals interested in productivity tools",
                height=80,
            )

            save_btn = st.form_submit_button("📂 Save as Draft", type="primary")

        if save_btn:
            if not goal.strip() or not audience.strip():
                st.warning("Please fill in both fields.")
            else:
                res = api_post("/campaigns", {
                    "goal": goal.strip(),
                    "audience": audience.strip(),
                })
                if res:
                    st.success(f"✅ Draft saved! Campaign ID: `{res['campaign_id']}`")
                    st.session_state.selected_campaign_id = res["campaign_id"]
                    st.info("Next: configure **Audience Branches**")
                    st.rerun()

        st.divider()
        st.subheader("My Campaigns")

        my_campaigns_fresh = [
            c for c in api_get("/campaigns") or []
            if c.get("created_by") == current_user["id"]
        ]
        if not my_campaigns_fresh:
            st.info("No campaigns yet. Create your first one above.")
        else:
            for c in my_campaigns_fresh:
                col_goal, col_status, col_btn = st.columns([5, 2, 1])
                col_goal.write(f"**{c['goal'][:65]}...**" if len(c['goal']) > 65 else f"**{c['goal']}**")
                col_goal.caption(f"ID: `{c['id']}` • {fmt_dt(c.get('created_at')) or ''} {fmt_dt(c.get('updated_at')) or ''}")
                col_status.write(status_badge(c["status"]))
                
                # Check if this campaign is currently selected
                is_selected = st.session_state.selected_campaign_id == c["id"]
                btn_label = "✓ Selected" if is_selected else "Manage"
                
                if col_btn.button(btn_label, key=f"manage_{c['id']}", disabled=is_selected):
                    st.session_state.selected_campaign_id = c["id"]
                    st.success(f"Campaign selected! Go to **🚀 Audience Branches** tab to manage it.")
                    st.rerun()

# --------------------------------------------------------------------------
# TAB 2 - AUDIENCE BRANCHES
# --------------------------------------------------------------------------

with tab_branches:
    if not show_create:
        st.info("Audience branch management is available to campaigners and admins.")
    else:
        st.header("Audience Branches")
        st.caption(
            "Define who gets which email. Each branch has its own language, country, age range, "
            "and optional SQL-like filter query. Agents can suggest branches - you edit them."
        )

        if not my_cmp_ids:
            st.info("Create a campaign first.")
        else:
            sel_id = st.selectbox(
                "Campaign",
                options=list(sel_options.keys()),
                format_func=lambda x: sel_options.get(x, x),
                index=(
                    list(sel_options.keys()).index(st.session_state.selected_campaign_id)
                    if st.session_state.selected_campaign_id in sel_options
                    else 0
                ),
                key="branch_campaign_sel",
            )
            if sel_id:
                st.session_state.selected_campaign_id = sel_id

                campaign = api_get(f"/campaigns/{sel_id}")
                if not campaign:
                    st.error("Could not load campaign.")
                else:
                    is_editable = campaign["status"] in ("draft", "rejected")

                    if not is_editable:
                        st.warning(
                            f"Campaign is in **{campaign['status']}** status - branches are read-only. "
                            "Only draft or rejected campaigns can be edited."
                        )

                    # -- AI Branch Suggestions ------------------------------------
                    if is_editable:
                        with st.expander("🪄 AI Branch Suggestions", expanded=not campaign.get("branches")):
                            st.caption(
                                "Click the button to let AI suggest audience segments. "
                                "Review each suggestion, then accept the ones you want."
                            )
                            if st.button("🪄 Suggest Audience Branches", type="secondary"):
                                with st.spinner("AI is analysing your campaign..."):
                                    sug_res = api_post(f"/campaigns/{sel_id}/suggest-branches")
                                    if sug_res and sug_res.get("suggestions"):
                                        st.session_state[f"sug_res_{sel_id}"] = sug_res["suggestions"]
                                        st.success(f"AI suggested {len(sug_res['suggestions'])} branches.")

                            if f"sug_res_{sel_id}" in st.session_state:
                                for i, sug in enumerate(st.session_state[f"sug_res_{sel_id}"]):
                                    with st.container():
                                        c1, c2 = st.columns([5, 1])
                                        with c1:
                                            st.markdown(
                                                f"**{sug.get('branch_name')}** -- "
                                                f"'{sug.get('language', '-')}' / {sug.get('country', '-')} / "
                                                f"Age: {sug.get('age_category', 'all')}"
                                            )
                                            if sug.get("custom_query"):
                                                st.code(sug["custom_query"], language="sql")
                                            if sug.get("rationale"):
                                                st.caption(f"Rationale: {sug['rationale']}")
                                        with c2:
                                            if st.button("Accept", key=f"acc_sug_{i}_{sel_id}"):
                                                res = api_post(f"/campaigns/{sel_id}/branches", {
                                                    "branch_name": sug.get("branch_name"),
                                                    "language": sug.get("language"),
                                                    "country": sug.get("country"),
                                                    "age_category": sug.get("age_category"),
                                                    "custom_query": sug.get("custom_query"),
                                                })
                                                if res:
                                                    st.success("Branch added!")
                                                    st.rerun()

                                        st.divider()

                    # -- Configured Branches --------------------------------------
                    st.subheader("Configured Branches")
                    branches = campaign.get("branches", [])

                    if not branches:
                        st.info("No branches yet. Use AI suggestions above or add one manually below.")
                    else:
                        for branch in branches:
                            with st.expander(
                                f"**{branch['branch_name']}** -- "
                                f"{branch.get('language') or 'Any lang'} / {branch.get('country') or 'Any country'} / "
                            f"Age {branch.get('age_category') or 'all'} "
                            f"({branch.get('status', 'draft').title()})",
                            expanded=is_editable,
                            ):
                                if is_editable:
                                    with st.form(f"branch_edit_{branch['id']}"):
                                        r1c1, r1c2, r1c3 = st.columns(3)
                                        new_name = r1c1.text_input(
                                            "Branch Name *", value=branch["branch_name"]
                                        )
                                        new_lang = r1c2.selectbox(
                                            "Language", LANGUAGE_OPTIONS,
                                            index=LANGUAGE_OPTIONS.index(branch.get("language") or "")
                                        )
                                        country_idx = COUNTRY_OPTIONS.index(branch.get("country") or "")
                                        new_country = r1c3.selectbox(
                                            "Country", COUNTRY_OPTIONS,
                                            index=country_idx, key=f"country_branch_{branch['id']}",
                                        )
                                        
                                        age_idx = (
                                            AGE_OPTIONS.index(branch.get("age_category") or "all")
                                            if (branch.get("age_category") or "all") in AGE_OPTIONS
                                            else 0
                                        )
                                        new_age = st.selectbox(
                                            "Age Category", AGE_OPTIONS,
                                            index=age_idx, key=f"age_branch_{branch['id']}",
                                        )

                                        st.markdown("**Audience Query** (editable SQL-like filter)")
                                        new_query = st.text_area(
                                            "custom_query",
                                            value=branch.get("custom_query") or "",
                                            placeholder="e.g. email_opt_in = true AND customer_tier = 'premium'",
                                            height=80,
                                            label_visibility="collapsed",
                                        )
                                        
                                        save_branch = st.form_submit_button("💾 Save Branch", type="primary")
                                        if save_branch:
                                            res = api_put(f"/campaigns/{sel_id}/branches/{branch['id']}", {
                                                "branch_name": new_name,
                                                "language": new_lang or None,
                                                "country": new_country or None,
                                                "age_category": new_age,
                                                "custom_query": new_query or None,
                                            })
                                            if res:
                                                st.success("Branch saved!")
                                                st.rerun()

                                    sug_key = f"sug_q_{branch['id']}"
                                    col_ai, col_del = st.columns([1, 1])
                                    if col_ai.button("🪄 AI Suggest Query", key=f"suggest_q_btn_{branch['id']}"):
                                        with st.spinner("Generating audience query..."):
                                            qres = api_post(f"/campaigns/{sel_id}/suggest-query", {
                                                "goal": campaign["goal"],
                                                "audience": campaign["audience"],
                                                "language": branch.get("language"),
                                                "country": branch.get("country"),
                                                "age_category": branch.get("age_category"),
                                            })
                                            if qres:
                                                st.session_state[sug_key] = qres["suggested_query"]
                                    
                                    if sug_key in st.session_state:
                                        st.markdown(f"**AI suggested query:** (copy into the field above to use)")
                                        st.code(st.session_state[sug_key], language="sql")

                                    if col_del.button("🗑️ Delete Branch", key=f"del_branch_{branch['id']}"):
                                        api_delete(f"/campaigns/{sel_id}/branches/{branch['id']}")
                                        st.rerun()

                                    # --- A/B Variants ----------------------------------------------------
                                    st.divider()
                                    st.markdown("### 🧪 A/B Variants")
                                    st.caption(
                                        "Split this branch's audience across different email variants. "
                                        "Split percentages should add up to 100%."
                                    )
                                    
                                    ab_variants = branch.get("variants", [])
                                    total_split = sum(v.get("split_percentage", 0) for v in ab_variants)
                                    if ab_variants:
                                        split_ok = abs(total_split - 100.0) < 1.0
                                        if not split_ok:
                                            st.warning(
                                                f"⚠️ Split percentages sum to **{total_split:.0f}%**. "
                                                "Adjust variants so they total 100%."
                                            )
                                        
                                        for v in ab_variants:
                                            vc1, vc2, vc3, vc4 = st.columns([3, 2, 2, 1])
                                            vc1.write(f"**{v['variant_name']}**")
                                            vc2.write(f"**{v.get('split_percentage', 0):.0f}%** of audience")
                                            vc3.write(v.get("status", "pending").title())
                                            if vc4.button("🗑️", key=f"del_var_{v['id']}", help="Delete"):
                                                api_delete(
                                                    f"/campaigns/{sel_id}/branches/{branch['id']}/"
                                                    f"ab-variants/{v['id']}"
                                                )
                                                st.rerun()
                                            if v.get("email_subject"):
                                                st.caption(f"Subject: {v['email_subject'][:80]}")
                                    else:
                                        st.info(
                                            "No A/B variants - all recipients in this branch get the same email. "
                                            "Add variants below to run a split test."
                                        )

                                    st.divider()
                                    st.subheader("➕ Add A/B Variant")
                                    with st.form(f"add_variant_{branch['id']}"):
                                        nv1, nv2 = st.columns(2)
                                        nv_name = nv1.text_input(
                                            "Variant Name",
                                            placeholder="e.g. Variant A",
                                            key=f"nv_name_{branch['id']}",
                                        )
                                        nv_split = nv2.number_input(
                                            "Split %",
                                            min_value=1.0, max_value=100.0,
                                            value=50.0 if not ab_variants else max(1.0, 100.0 - total_split),
                                            step=5.0,
                                            key=f"nv_split_{branch['id']}",
                                        )
                                        add_var_btn = st.form_submit_button("➕ Add Variant")
                                        if add_var_btn:
                                            if not nv_name.strip():
                                                st.warning("Variant name is required.")
                                            else:
                                                res = api_post(
                                                    f"/campaigns/{sel_id}/branches/{branch['id']}/ab-variants",
                                                    {
                                                        "variant_name": nv_name.strip(),
                                                        "split_percentage": float(nv_split),
                                                    }
                                                )
                                                if res:
                                                    st.success(f"Variant '{nv_name.strip()}' added!")
                                                    st.rerun()
                                else:
                                    cols = st.columns(4)
                                    cols[0].metric("Language", branch.get("language") or "Any")
                                    cols[1].metric("Country", branch.get("country") or "Any")
                                    cols[2].metric("Age", branch.get("age_category") or "All")
                                    cols[3].metric("Status", branch.get("status", "draft").title())
                                    if branch.get("custom_query"):
                                        st.code(branch["custom_query"], language="sql")
                                    if branch.get("email_subject"):
                                        st.markdown(f"**Subject:** {branch['email_subject']}")

                    if is_editable:
                        with st.expander("➕ Add Branch Manually"):
                            with st.form("add_branch_form"):
                                ac1, ac2, ac3 = st.columns(3)
                                m_name = ac1.text_input("Branch Name *", placeholder="e.g. English-US-Young")
                                m_lang = ac2.selectbox("Language", LANGUAGE_OPTIONS, key="manual_lang")
                                m_country = ac3.selectbox("Country", COUNTRY_OPTIONS, key="manual_country")
                                m_age = st.selectbox("Age Category", AGE_OPTIONS, key="manual_age")
                                m_query = st.text_area(
                                    "Custom Query (optional)",
                                    placeholder="e.g. email_opt_in = true AND customer_tier = 'premium'",
                                    height=70,
                                )
                                add_btn = st.form_submit_button("➕ Add Branch", type="primary")
                                if add_btn:
                                    if not m_name.strip():
                                        st.warning("Branch name is required.")
                                    else:
                                        res = api_post(f"/campaigns/{sel_id}/branches", {
                                            "branch_name": m_name.strip(),
                                            "language": m_lang or None,
                                            "country": m_country or None,
                                            "age_category": m_age,
                                            "custom_query": m_query.strip() or None,
                                        })
                                        if res:
                                            st.success("Branch added!")
                                            st.rerun()
# --------------------------------------------------------------------------
# TAB 3 - SCHEDULE & SUBMIT
# --------------------------------------------------------------------------

with tab_schedule:
    if not show_create:
       st.info("Campaign scheduling is available to campaigners and admins.")
    else:
        st.header("Schedule & Submit")
        st.caption(
           "Set send times for each audience branch. "
           "Branches without a schedule send immediately after execution is triggered. "
           "Then submit the campaign for your manager's approval."
        )

        if not my_cmp_ids:
            st.info("No campaigns found. Create one first.")
        else:
            sel_id_sched = st.selectbox(
               "Campaign",
               options=list(sel_options.keys()),
               format_func=lambda x: sel_options.get(x, x),
               index=(
                   list(sel_options.keys()).index(st.session_state.selected_campaign_id)
                   if st.session_state.selected_campaign_id in sel_options
                   else 0
               ),
               key="sched_campaign_sel",
            )
            if sel_id_sched:
                st.session_state.selected_campaign_id = sel_id_sched
            campaign_s = api_get(f"/campaigns/{sel_id_sched}")

            if not campaign_s:
                st.error("Could not load campaign.")
            else:
                is_editable_s = campaign_s["status"] in ("draft", "rejected")
                branches_s = campaign_s.get("branches", [])

                st.subheader("Send Schedule")
                if not branches_s:
                    st.warning("No audience branches configured. Go to **Audience Branches** tab first.")
                else:
                    for b in branches_s:
                        with st.expander(
                            f"📧 **{b['branch_name']}** -- "
                            f"{b.get('language') or 'Any'} / {b.get('country') or 'Any'} / "
                            f"Age {b.get('age_category') or 'all'}",
                            expanded=True,
                        ):
                            cur_sched = b.get("scheduled_at")
                            if cur_sched and cur_sched != "None":
                                st.success(f"📅 Scheduled: {fmt_dt(cur_sched)} UTC")
                            else:
                                st.info("Not scheduled - will send immediately on execution.")

                            if is_editable_s:
                                sc1, sc2, sc3, sc4 = st.columns([2, 2, 1, 1])
                                default_date = (datetime.utcnow() + timedelta(days=1)).date()
                                sched_date = sc1.date_input(
                                    "Date", value=default_date,
                                    key=f"sd_{b['id']}",
                                )
                                sched_time = sc2.time_input(
                                    "Time (UTC)",
                                    value=datetime.strptime("09:00", "%H:%M").time(),
                                    key=f"st_{b['id']}",
                                )
                                if sc3.button("Set", key=f"set_s_{b['id']}"):
                                    dt = datetime.combine(sched_date, sched_time)
                                    res = api_put(
                                        f"/campaigns/{sel_id_sched}/branches/{b['id']}",
                                        {"scheduled_at": dt.isoformat()},
                                    )
                                    if res:
                                        st.success(f"Scheduled: {dt.strftime('%Y-%m-%d %H:%M')} UTC")
                                        st.rerun()
                                if sc4.button("Clear", key=f"clr_s_{b['id']}"):
                                    api_put(
                                        f"/campaigns/{sel_id_sched}/branches/{b['id']}",
                                        {"scheduled_at": None},
                                    )
                                    st.rerun()

                st.divider()
                st.subheader("Submit for Manager Approval")

                if campaign_s["status"] == "rejected":
                    st.error(
                        f"❌ **Rejected by manager:** "
                        f"'{campaign_s.get('manager_rejection_reason') or 'No reason given'}'"
                    )
                    st.info("Edit the campaign and branches, then resubmit.")

                if is_editable_s and branches_s:
                    total = len(branches_s)
                    scheduled = sum(
                        1 for b in branches_s if b.get("scheduled_at") and b["scheduled_at"] != "None"
                    )
                    m1, m2, m3 = st.columns(3)
                    m1.metric("Branches", total)
                    m2.metric("Scheduled", scheduled)
                    m3.metric("Immediate", total - scheduled)

                    mgr_info = api_get(f"/users/{current_user['id']}/manager")
                    if mgr_info and mgr_info.get("name"):
                        st.info(
                            f"📬 Submitting to: **{mgr_info['name']}** "
                            f"({mgr_info.get('email', '')})"
                        )

                        if st.button(
                            "📩 Submit for Manager Approval",
                            type="primary",
                            use_container_width=True,
                            key="submit_btn",
                        ):
                            res = api_post(f"/campaigns/{sel_id_sched}/submit")
                            if res:
                                st.success(f"✅ {res.get('message', 'Submitted!')}")
                                st.info(f"Approval requested from: **{res['manager_name']}**")
                                time.sleep(1)
                                st.rerun()
                    else:
                        st.warning("⚠️ No manager assigned - campaign can still be submitted.")

                elif not is_editable_s:
                    st.info(f"Campaign is **{status_badge(campaign_s['status'])}**")
                    st.info("No action required here.")

                elif not branches_s:
                        st.warning("Add at least one audience branch before submitting.")

# --------------------------------------------------------------------------
# TAB 4 - MANAGER QUEUE
# --------------------------------------------------------------------------

with tab_manager:
    if not show_manager:
        st.info("The manager queue is available to approvers, admins, and leaders.")
    else:
        st.header("Manager Approval Queue")
        st.caption(
            "Campaigns submitted by your direct reports, awaiting your approval."
        )

        col_refresh = st.columns([5, 1])[1]
        if col_refresh.button("🔄 Refresh", key="refresh_mgr"):
            st.rerun()

        pending = api_get(f"/campaigns/pending-approval/{current_user['id']}") or []

        if not pending:
            st.success("✅ No campaigns pending your approval.")
        else:
            for pc in pending:
                detail = api_get(f"/campaigns/{pc['id']}")
                if not detail:
                    continue

                with st.expander(
                    f"📋 **{detail['goal'][:70]}...**" if len(detail['goal']) > 70 else f"**{detail['goal']}**",
                    expanded=True,
                ):
                    st.markdown(f"**Goal:** {detail['goal']}")
                    st.markdown(f"**Audience:** {detail['audience']}")
                    st.caption(
                        f"Submitted by: **{detail.get('creator_name') or pc.get('creator_name', '-')}** "
                        f"at {fmt_dt(detail.get('submitted_at'))}"
                    )

                    branches_pc = detail.get("branches", [])
                    if branches_pc:
                        st.markdown(f"**Audience Branches ({len(branches_pc)}):**")
                        for b in branches_pc:
                            sched_str = fmt_dt(b.get("scheduled_at")) if b.get("scheduled_at") else "Immediate"
                            # Fetch actual audience count for manager
                            est_pc = api_get(f"/campaigns/{pc['id']}/branches/{b['id']}/audience-estimate")
                            match_count_pc = est_pc.get("estimated_audience", 0) if est_pc else 0
                            
                            st.write(
                                f"  * **{b['branch_name']}** -- "
                                f"{b.get('language') or 'Any'} / {b.get('country') or 'Any'} / "
                                f"Age {b.get('age_category') or 'all'} -- "
                                f"**Recipients: {match_count_pc}**"
                            )
                            st.write(f"    Send: {sched_str}")
                            if b.get("custom_query"):
                                st.code(b["custom_query"], language="sql")

                    st.divider()
                    col_approve, col_reject = st.columns(2)

                    with col_approve:
                        if st.button(
                            "✅ Approve Campaign",
                            key=f"mgr_app_{pc['id']}",
                            type="primary",
                            use_container_width=True,
                        ):
                            res = api_post(f"/campaigns/{pc['id']}/manager-approve", {
                                "approved": True,
                            })
                            if res:
                                st.success(res.get("message", "Approved!"))
                                time.sleep(1)
                                st.rerun()

                    with col_reject:
                        rej_reason = st.text_input(
                            "Rejection reason (required)",
                            placeholder="Explain why this campaign needs revision...",
                            key=f"rej_reason_{pc['id']}",
                        )
                        if st.button(
                            "❌ Reject Campaign",
                            key=f"mgr_rej_{pc['id']}",
                            use_container_width=True,
                        ):
                            if not rej_reason.strip():
                                st.warning("Please provide a rejection reason.")
                            else:
                                res = api_post(f"/campaigns/{pc['id']}/manager-approve", {
                                    "approved": False,
                                    "reason": rej_reason.strip(),
                                })
                                if res:
                                    st.warning(res.get("message", "Rejected."))
                                    time.sleep(1)
                                    st.rerun()

# --------------------------------------------------------------------------
# TAB 5 - AI REVIEW (HITL)
# --------------------------------------------------------------------------

with tab_ai:
    st.header("AI Review")
    st.caption(
        "Review AI-generated campaign strategy and per-branch email content. "
        "Approve to proceed or reject with feedback so the AI regenerates."
    )

    if st.button("🔄 Refresh", key="refresh_ai"):
        st.rerun()

    ai_campaigns = [
        c for c in api_get("/campaigns") or []
        if c.get("created_by") == current_user["id"]
        and c["status"] in (
            "awaiting_plan_approval", "awaiting_content_approval",
            "scheduled", "awaiting_rating",
        )
    ]

    if not ai_campaigns:
        st.info("No campaigns awaiting AI review.")
    else:
        for ac in ai_campaigns:
            detail = api_get(f"/campaigns/{ac['id']}")
            if not detail:
                continue

            with st.expander(
                f"{status_badge(ac['status'])} -- {ac['goal'][:65]}...",
                expanded=True,
            ):
                st.caption(f"ID: `{ac['id']}`")

                if detail["status"] == "awaiting_plan_approval" and detail.get("strategy"):
                    st.markdown("### AI Campaign Strategy")
                    s = detail["strategy"]
                    st.markdown(f"**Name:** {s.get('campaign_name', '_')}")
                    st.markdown(f"**Objective:** {s.get('objective', '_')}")
                    st.markdown(f"**Email Approach:** {s.get('email_approach', '_')}")
                    st.markdown(f"**Timeline:** {s.get('timeline', '_')}")
                    if s.get("key_messages"):
                        st.markdown("**Key Messages:**")
                        for msg in s["key_messages"]:
                            st.write(f"  • {msg}")
                    if s.get("success_metrics"):
                        st.markdown(f"**Success Metrics:** {', '.join(s['success_metrics'])}")

                    col_ap, col_rj = st.columns(2)
                    with col_ap:
                        if st.button("✅ Approve Strategy", key=f"ap_{ac['id']}", type="primary"):
                            api_post(f"/campaigns/{ac['id']}/approve", {"approved": True})
                            st.success("Strategy approved - generating email content per branch...")
                            time.sleep(1)
                            st.rerun()
                    with col_rj:
                        fb = st.text_input("Rejection reason", key=f"rf_{ac['id']}")
                        if st.button("❌ Reject Strategy", key=f"rp_{ac['id']}"):
                            api_post(f"/campaigns/{ac['id']}/approve", {"approved": False, "feedback": fb})
                            st.warning("Strategy rejected - replanning...")
                            time.sleep(1)
                            st.rerun()

                elif detail["status"] == "awaiting_content_approval":
                    st.markdown("### AI Generated Emails - Per Branch")

                    st.info(
                        "**Email Language Understanding:** If a branch email is written in a "
                        "language you don't read, select your preferred language below and click "
                        "**Translate** to see an AI translation before approving."
                    )
                    col_lang_sel, _ = st.columns([2, 3])
                    view_lang = col_lang_sel.selectbox(
                        "Understand emails in",
                        ["(show original)"] + [l for l in LANGUAGE_OPTIONS if l],
                        key=f"view_lang_{ac['id']}",
                    )

                    for b in detail.get("branches", []):
                        with st.container():
                            # Fetch actual audience count
                            est = api_get(f"/campaigns/{ac['id']}/branches/{b['id']}/audience-estimate")
                            match_count = est.get("estimated_audience", 0) if est else 0
                            branch_lang = b.get("language") or "Unknown"
                            
                            st.markdown(
                                f"**Branch: {b['branch_name']}** "
                                f"({branch_lang} / {b.get('country') or 'Any'} / "
                                f"Age: {b.get('age_category') or 'all'})"
                            )
                            st.markdown(f"👥 **Matched Recipients:** `{match_count}`")
                            
                            new_subject = st.text_input(
                                "Subject", value=b.get("email_subject", ""),
                                key=f"edit_sub_{b['id']}",
                            )
                            new_body = st.text_area(
                                "Body", value=b.get("email_body", ""), height=150,
                                key=f"edit_body_{b['id']}",
                            )
                            
                            if st.button("💾 Save Changes", key=f"save_b_{b['id']}"):
                                res = api_put(
                                    f"/campaigns/{ac['id']}/branches/{b['id']}",
                                    {"email_subject": new_subject, "email_body": new_body},
                                )
                                if res:
                                    st.success("Changes saved!")
                                    time.sleep(0.5)
                                    st.rerun()

                            if view_lang != "(show original)" and b.get("email_body"):
                                trans_key = f"trans_{b['id']}_{view_lang}"
                                col_t1, col_t2 = st.columns([2, 3])
                                if col_t1.button(f"Translate to {view_lang}", key=f"translate_btn_{b['id']}"):
                                    with st.spinner(f"Translating to {view_lang}..."):
                                        try:
                                            tr = requests.post(
                                                f"{API}/translate",
                                                json={
                                                    "text": f"Subject: {b.get('email_subject', '')}\n\n{b['email_body']}",
                                                    "source_language": branch_lang,
                                                    "target_language": view_lang,
                                                },
                                                headers=auth_headers(),
                                                timeout=30,
                                            )
                                            tr.raise_for_status()
                                            st.session_state[trans_key] = tr.json().get(
                                                "translated_text", "Translation unavailable."
                                            )
                                        except Exception as e:
                                            st.session_state[trans_key] = f"Translation error: {e}"

                                if trans_key in st.session_state:
                                    st.text_area(
                                        f"AI Translation ({view_lang})",
                                        value=st.session_state[trans_key],
                                        height=150, disabled=True,
                                        key=f"trans_area_{b['id']}",
                                    )
                                    st.caption(
                                        "AI-generated translation for understanding purposes only. "
                                        f"Original {branch_lang} version will be sent to recipients."
                                    )

                            ab_branch_variants = b.get("variants", [])
                            if ab_branch_variants:
                                st.markdown(
                                    f"**{len(ab_branch_variants)} A/B Variants** - "
                                    f"AI generated distinct email per variant:"
                                )
                                for v in ab_branch_variants:
                                    with st.expander(
                                        f"Variant {v.get('variant_name')} - "
                                        f"{v.get('split_percentage', 0)}% of audience",
                                        expanded=True,
                                    ):
                                        # Editable variant content
                                        v_sub = st.text_input(
                                            "Subject", value=v.get("email_subject", ""),
                                            key=f"v_sub_edit_{v['id']}",
                                        )
                                        v_body = st.text_area(
                                            "Body", value=v.get("email_body", ""), height=120,
                                            key=f"v_body_edit_{v['id']}",
                                        )
                                        
                                        if st.button("💾 Save Variant Changes", key=f"v_save_{v['id']}"):
                                            res = api_put(
                                                f"/campaigns/{ac['id']}/branches/{b['id']}/ab-variants/{v['id']}",
                                                {"email_subject": v_sub, "email_body": v_body},
                                            )
                                            if res:
                                                st.success("Variant changes saved!")
                                                time.sleep(0.5)
                                                st.rerun()
                                        else:
                                            st.info("No content generated yet for this variant.")

                            st.divider()

                    col_ap2, col_rj2 = st.columns(2)
                    with col_ap2:
                        if st.button("✅ Approve Email Content", key=f"ac_{ac['id']}", type="primary"):
                            res = api_post(f"/campaigns/{ac['id']}/approve", {"approved": True})
                            if res:
                                st.success(res.get("message", "Content approved!"))
                                time.sleep(1)
                                st.rerun()
                    with col_rj2:
                        fb2 = st.text_input("Rejection reason", key=f"rcf_{ac['id']}")
                        if st.button("❌ Reject Content", key=f"rc_{ac['id']}"):
                            api_post(f"/campaigns/{ac['id']}/approve", {"approved": False, "feedback": fb2})
                            st.warning("Content rejected - regenerating...")
                            time.sleep(1)
                            st.rerun()
                elif detail["status"] == "scheduled":
                    st.markdown("### Campaign Scheduled - Ready to Execute")
                    for b in detail.get("branches", []):
                        sched_str = fmt_dt(b.get("scheduled_at")) if b.get("scheduled_at") else "Immediate"
                        st.write(f"• **{b['branch_name']}** -> {sched_str}")

                    if st.button("🚀 Execute Now", key=f"exec_{ac['id']}", type="primary"):
                        res = api_post(f"/campaigns/{ac['id']}/execute")
                        if res:
                            st.success(res.get("message", "Executing..."))
                            time.sleep(1)
                            st.rerun()

                elif detail["status"] == "awaiting_rating":
                    st.markdown("### Campaign Complete - Rate the Results")
                    results = detail.get("execution_results") or {}
                    col_m1, col_m2 = st.columns(2)
                    col_m1.metric("Total Emails Sent", results.get("total_sent", 0))
                    col_m2.metric("Branches Skipped", len(results.get("branches_skipped", [])))

                    if results.get("branch_results"):
                        st.markdown("**Results per Branch:**")
                        for bid, br in results["branch_results"].items():
                            st.write(
                                f"  • **{br.get('branch_name', bid)}** — "
                                f"{br.get('sent_count', 0)} sent "
                                f"({br.get('language', '')} / {br.get('country', '')})"
                            )

                    rating = st.radio(
                        "Rate this campaign", ["good", "neutral", "poor"],
                        horizontal=True, key=f"rate_{ac['id']}",
                    )
                    if st.button("Submit Rating", key=f"sr_{ac['id']}", type="primary"):
                        res = api_post(f"/campaigns/{ac['id']}/rate", {"rating": rating})
                        if res:
                            st.success(f"✅ Rating '{res.get('rating_saved', rating)}' saved - running evaluation...")
                            time.sleep(1)
                            st.rerun()

# ------------------------------------------------------------------------------
# TAB 6 - ENGAGEMENT ANALYTICS
# ------------------------------------------------------------------------------
with tab_engagement:
    st.header("Email Engagement Analytics")
    st.caption(
        "Track open rates, click rates, and segment-level performance across campaigns. "
        "In demo mode, events are simulated at execution time."
    )

    col_eng_ref = st.columns([5, 1])[1]
    if col_eng_ref.button("🔄 Refresh", key="refresh_eng"):
        st.rerun()

    all_eng_campaigns = [
        c for c in api_get("/campaigns") or []
        if c["status"] in ("executing", "awaiting_rating", "evaluating", "completed")
    ]

    if not all_eng_campaigns:
        st.info(
            "No executed campaigns yet. Run a campaign end-to-end and engagement "
            "data will appear here once emails are sent."
        )
    else:
        eng_options = {c["id"]: f"{c['goal'][:60]}... [{c['status']}]" for c in all_eng_campaigns}
        sel_eng_id = st.selectbox(
            "Select Campaign",
            options=list(eng_options.keys()),
            format_func=lambda x: eng_options.get(x, x),
            key="eng_campaign_sel",
        )

        eng_data = api_get(f"/campaigns/{sel_eng_id}/engagement/v2")
        if not eng_data:
            st.error("Could not load engagement data.")
        else:
            st.subheader("Campaign Summary")
            m1, m2, m3, m4, m5, m6 = st.columns(6)
            m1.metric("Emails Sent", f"{eng_data.get('total_sent', 0):,}")
            m2.metric("Total Opens", f"{eng_data.get('total_opens', 0):,}")
            m3.metric("Total Clicks", f"{eng_data.get('total_clicks', 0):,}")
            m4.metric("Unsubscribes", f"{eng_data.get('total_unsubscribes', 0):,}")
            m5.metric("Open Rate", f"{eng_data.get('overall_open_rate', 0):.1f}%")
            m6.metric("Unsub Rate", f"{eng_data.get('overall_unsub_rate', 0):.2f}%")

            st.divider()
            st.subheader("Per-Branch Performance")
            branches_eng = eng_data.get("branches", {})

            if not branches_eng:
                st.info("No branch data available.")
            else:
                rows = []
                for bid, b in branches_eng.items():
                    rows.append({
                        "_bid": bid,
                        "Branch": b["branch_name"],
                        "Segment": f"{b.get('language') or 'Any'}/{b.get('country') or 'Any'}/{b.get('age_category') or 'all'}",
                        "Sent": b.get("sent", 0),
                        "Opens": b.get("opens", 0),
                        "Clicks": b.get("clicks", 0),
                        "Unsubscribes": b.get("unsubscribes", 0),
                        "Open Rate %": b.get("open_rate", 0),
                        "Click Rate %": b.get("click_rate", 0),
                        "CTOR %": b.get("ctor", 0),
                        "Unsub Rate %": b.get("unsub_rate", 0),
                        "ab_variants": b.get("ab_variants", {}),
                    })

                rows.sort(key=lambda r: r["Open Rate %"], reverse=True)

                for i, row in enumerate(rows):
                    with st.expander(
                        f"{'🔥' if i == 0 else '📧'} **{row['Branch']}** — "
                        f"Open: {row['Open Rate %']}%  |  Click: {row['Click Rate %']}%",
                        expanded=(i == 0),
                    ):
                        rc1, rc2, rc3, rc4, rc5, rc6 = st.columns(6)
                        rc1.metric("Segment", row["Segment"])
                        rc2.metric("Sent", row["Sent"])
                        rc3.metric("Opens", row["Opens"], f"{row['Open Rate %']}%")
                        rc4.metric("Clicks", row["Clicks"], f"{row['Click Rate %']}%")
                        rc5.metric("CTOR", f"{row['CTOR %']}%", help="Click-to-Open Rate")
                        rc6.metric("Unsubscribes", row["Unsubscribes"], f"{row['Unsub Rate %']}%")

                        ab_data = row["ab_variants"]
                        if ab_data:
                            st.markdown("**A/B Variant Results:**")
                            ab_rows = []
                            for vid, vd in ab_data.items():
                                ab_rows.append({
                                    "Variant": vd.get("variant_name", vid),
                                    "Split": f"{vd.get('split_pct', 0):.0f}%",
                                    "Sent": vd.get("sent", 0),
                                    "Opens": vd.get("opens", 0),
                                    "Clicks": vd.get("clicks", 0),
                                    "Open Rate": f"{vd.get('open_rate', 0):.1f}%",
                                    "Click Rate": f"{vd.get('click_rate', 0):.1f}%",
                                    "Unsubscribes": vd.get("unsubscribes", 0),
                                })

                            try:
                                import pandas as pd
                                st.dataframe(pd.DataFrame(ab_rows), use_container_width=True)
                            except Exception:
                                for ab_row in ab_rows:
                                    st.write(
                                        f"**{ab_row['Variant']}** — {ab_row['Split']} | "
                                        f"Sent: {ab_row['Sent']} | "
                                        f"Open: {ab_row['Open Rate']} | "
                                        f"Click: {ab_row['Click Rate']}"
                                    )

                st.divider()
                st.subheader("Open Rate by Segment")
                try:
                    for row in sorted(rows, key=lambda r: r["Open Rate %"], reverse=True):
                        cols = st.columns([3, 7])
                        cols[0].write(row["Branch"][:30])
                        cols[1].progress(
                            min(row["Open Rate %"] / 100, 1.0),
                            text=f"{row['Open Rate %']:.1f}%",
                        )
                except Exception:
                    pass

# ------------------------------------------------------------------------------
# TAB 7 - AGENT MONITOR
# ------------------------------------------------------------------------------
with tab_monitor:
    st.header("🤖 Agent Monitor")
    st.caption(
        "Real-time view of all AI agent executions - see which agents are running for "
        "which campaigns and branches."
    )
    with st.expander("📊 Campaign Pipeline Overview", expanded=True):
            st.markdown("""
    | | |
    |---|---|
    | Manager Approval | |
    | ↓ | |
    | **Planner** | ↳ Generates campaign strategy |
    | ↓ | |
    | strategy approved | |
    | ↓ | |
    | **Creator** (parallel per branch - up to 4 threads) | |
    | Branch A ↳ [Variant A content, Variant B content, ...] | |
    | Branch B ↳ [default content] | |
    | ↓ | |
    | content approved | |
    | ↓ | |
    | **Executor** | |
    | • Matches recipients by language / country / age | |
    | • Deduplicates (one email per person per campaign) | |
    | • Splits audience across A/B variants by configured % | |
    | ↓ | |
    | user rates results | |
    | ↓ | |
    | **Feedback** | ↳ Evaluates results, stores learnings in knowledge base |
            """)

    col_ref_mon, col_view = st.columns([4, 2])
    if col_ref_mon.button("🔄 Refresh", key="refresh_monitor"):
        st.rerun()
    view_mode = col_view.selectbox(
        "View", ["Active Runs", "All Recent (50)", "By Campaign"],
        key="monitor_view",
    )

    if view_mode == "Active Runs":
        active_runs = api_get("/agent-runs/active") or []
        if not active_runs:
            st.success("✅ No agents currently running.")
        else:
            st.warning(f"**{len(active_runs)}** agent(s) currently running:")
            for r in active_runs:
                render_agent_run(r)

    elif view_mode == "All Recent (50)":
        recent_runs = api_get("/agent-runs?limit=50") or []
        if not recent_runs:
            st.info("No agent runs yet. Run a campaign to see activity here.")
        else:
            st.markdown(f"Showing **{len(recent_runs)}** most recent runs:")
            for r in recent_runs:
                render_agent_run(r)

    else:
        mon_campaigns = api_get("/campaigns") or []
        executed = [
            c for c in mon_campaigns 
            if ["status"] not in ["draft", "waiting_manager_approval"]
        ]
        if not executed:
            st.info("No campaigns have been processed yet.")
        else:
            mon_options = {f"{c['id']}: {c['goal'][:55]}... [{c['status']}]": c for c in executed}
            sel_mon_id = st.selectbox(
                "Campaign",
                options=list(mon_options.keys()),
                format_func=lambda x: mon_options.get(x, x),
                key="monitor_campaign_sel",
            )
            camp_runs = api_get(f"/agent-runs/campaign/{sel_mon_id}") or []
            if not camp_runs:
                st.info("No agent run records for this campaign.")
            else:
                by_type: dict = {}
                for r in camp_runs:
                    by_type.setdefault(r["agent_type"], []).append(r)
                
                agent_order = ["planner", "creator", "executor", "feedback", "chat", "translate"]
                for agent_type in agent_order + [t for t in by_type if t not in agent_order]:
                    runs_for_type = by_type.get(agent_type, [])
                    if not runs_for_type:
                        continue
                    
                    agent_icons = {
                        "planner": "📝", "creator": "🎨", "executor": "🚀",
                        "feedback": "📊", "chat": "💬", "translate": "🌐",
                    }
                    icon = agent_icons.get(agent_type, "🤖")
                    st.markdown(f"### {icon} {agent_type.title()} Agent ({len(runs_for_type)} runs)")
                    for r in runs_for_type:
                        render_agent_run(r)
                    st.divider()

# 
# TAB 8 - HISTORY
# 

with tab_history:
    st.header("Campaign History")
    col_filter, col_ref, col_auto = st.columns([3, 1, 1])
    status_filter = col_filter.selectbox(
        "Filter by status",
        [
            "all", "draft", "submitted", "waiting_manager_approval", "approved",
            "rejected", "planning", "waiting_plan_approval", "creating_content",
            "waiting_content_approval", "scheduled", "executing",
            "awaiting_rating", "evaluating", "completed", "failed",
        ],
        key="hist_filter",
    )
    if col_ref.button("🔄 Refresh", key="refresh_hist"):
        st.rerun()
    
    # Auto-refresh every 3 seconds when campaign is in progress
    if col_auto.checkbox("Auto-refresh", value=False, key="auto_refresh_hist"):
        import time
        time.sleep(3)
        st.rerun()

    all_hist = api_get("/campaigns") or []
    if status_filter != "all":
        all_hist = [c for c in all_hist if c["status"] == status_filter]

    if not all_hist:
        st.info("No campaigns found.")
    else:
        for c in all_hist:
            detail = api_get(f"/campaigns/{c['id']}")
            if not detail:
                continue
            with st.expander(
                f"{status_badge(c['status'])} — {c['goal'][:70]}{'...' if len(c['goal']) > 70 else ''}",
                expanded=False,
            ):
                mc1, mc2, mc3, mc4 = st.columns(4)
                mc1.metric("Status", detail["status"].replace("_", " ").title())
                mc2.metric("Branches", len(detail.get("branches", [])))
                branches_h_data = detail.get("branches", [])
                total_sent_h = sum(int(b.get("sent_count") or 0) for b in branches_h_data)
                mc3.metric("Audience Count", f"{total_sent_h:,}")
                mc4.metric("Rating", detail.get("rating") or "-")

                st.caption(
                    f"**ID:** `{c['id']}` | Created: {fmt_dt(c.get('created_at'))} | "
                    f"By: {detail.get('creator_name') or '-'}"
                )
                if detail.get("approved_by"):
                    st.caption(f"Approved by: {detail.get('approver_name') or detail['approved_by']}")
                if detail.get("submitted_at"):
                    st.caption(f"Submitted: {fmt_dt(detail['submitted_at'])}")
                if detail.get("approved_at"):
                    st.caption(f"Approved: {fmt_dt(detail.get('approved_at'))}")

                if detail.get("manager_rejection_reason"):
                    st.error(f"Manager rejection: {detail['manager_rejection_reason']}")
                if detail.get("rejection_feedback"):
                    st.warning(f"AI review rejection: {detail['rejection_feedback']}")

                branches_h = detail.get("branches", [])
                if branches_h:
                    st.markdown("**Branches:**")
                    for b in branches_h:
                        sched_str = fmt_dt(b.get("scheduled_at")) if b.get("scheduled_at") else "Immediate"
                        st.write(
                            f"• **{b['branch_name']}** — "
                            f"{b.get('language') or 'Any'} / {b.get('country') or 'Any'} — "
                            f"Send: {sched_str}"
                        )

                if detail.get("evaluation"):
                    ev = detail["evaluation"]
                    ec1, ec2 = st.columns(2)
                    ec1.metric("Score", f"{ev.get('performance_score', '-')}/10")
                    if ev.get("best_performing_segment"):
                        ec2.metric("Best Segment", ev["best_performing_segment"])
                    if ev.get("key_learnings"):
                        st.markdown("**Key Learnings:**")
                        for lrn in ev["key_learnings"]:
                            st.write(f" • {lrn}")
                    if ev.get("email_insights"):
                        st.markdown("**Email Insights:**")
                        for ins in ev["email_insights"]:
                            st.write(f" • {ins}")

# 
# TAB 9 — USERS
# 

with tab_users:
    if not show_users:
        st.info("User management is available to admins and approvers only.")
    else:
        st.header("👤 User Management")
        bu_scope = "BU: {user_bu}" if user_bu else "All Business Units"
        st.caption(f"Scope: **{bu_scope}** • Your role: **{user_role.title()}**")

        # — Create User Form
        st.subheader("Create New User")
        with st.form("create_user_form"):
            u1, u2 = st.columns(2)
            new_name = u1.text_input("Full Name *", placeholder="Jane Doe")
            new_email = u2.text_input("Email *", placeholder="jane@company.com")

            u3, u4 = st.columns(2)
            # Role options depend on caller's role
            if user_role == "admin":
                role_options = ["campaigner", "approver", "leader", "admin"]
            else: # approver
                role_options = ["campaigner", "approver", "leader"]
            new_role = u3.selectbox("Role", role_options)

            # BU options depend on caller's role
            if user_role == "admin":
                bu_options = ["BU1", "BU2", ""]
            else:
                bu_options = [user_bu]
            new_bu = u4.selectbox("Business Unit", bu_options, help="Leave blank for admin/leader")

            st.info(f"New user will be assigned to your BU: **{user_bu}**")
            
            # Manager select
            users_list = api_get("/users") or []
            mgr_options = {u["id"]: f"{u['name']} ({u['role']})" for u in users_list}
            mgr_options = {"": "— No manager —"} | mgr_options
            new_mgr = st.selectbox(
                "Manager (optional)",
                options=list(mgr_options.keys()),
                format_func=lambda x: mgr_options.get(x, x),
            )

            create_btn = st.form_submit_button("➕ Create User", type="primary")

        if create_btn:
            if not new_name.strip() or not new_email.strip():
                st.warning("Name and email are required.")
            else:
                res = api_post("/users", {
                    "name": new_name.strip(),
                    "email": new_email.strip(),
                    "role": new_role,
                    "business_unit": new_bu or None,
                    "manager_id": new_mgr or None,
                })
                if res:
                    st.success(
                        f"✅ User **{new_name}** created!\n\n"
                        f"**Temporary password** (show once): `{res['temp_password']}`\n\n"
                        f"Share this with the user — they must change it on first login."
                    )

        # — User List
        st.divider()
        st.subheader("Existing Users")
        if st.button("🔄 Refresh", key="refresh_users"):
            st.rerun()

        users_data = api_get("/users") or []
        if not users_data:
            st.info("No users found.")
        else:
            role_icon = {"admin": "🔴", "leader": "🟣", "approver": "🟡", "campaigner": "🟢"}
            for u in users_data:
                icon = role_icon.get(u.get("role"), "⚪️")
                bu_str = u.get("business_unit") or "Cross-BU"
                st.write(
                    f"{icon} **{u['name']}** — `{u['email']}` — "
                    f"{u.get('role', '?').title()} | {bu_str}"
                )

# 
# TAB 10 — RAI EVALUATION (admin-only)
# 

with tab_rai:
    if not show_rai:
        st.info("RAI Evaluation is available to admins only.")
    else:
        st.header("🛡️ Responsible AI Evaluation")
        st.caption(
            "Platform-wide Responsible AI metrics across 4 dimensions: "
            "Content Safety, Process Transparency, Human Accountability, Audience Privacy"
        )

        # — Section 1: RAI Dimension Summary
        st.subheader("RAI Dimension Summary")

        rai_summary = api_get("/rai/summary")
        if rai_summary:
            cs = rai_summary.get("content_safety", {})
            pc = rai_summary.get("process_compliance", {})
            mo = rai_summary.get("manager_oversight", {})
            asat = rai_summary.get("audience_satisfaction", {})

            col1, col2, col3, col4 = st.columns(4)

            pass_rate = cs.get("pass_rate", 100.0)
            col1.metric(
                "🛡️ Content Safety",
                f"{pass_rate}%",
                delta=f"{pass_rate - 95:.1f}% vs 95% target",
                delta_color="normal" if pass_rate >= 95 else "inverse",
                help="Guardrail pass rate (PII + brand safety checks)",
            )

            hitl_rate = pc.get("hitl_rate", 0.0)
            col2.metric(
                "⚖️ HITL Compliance",
                f"{hitl_rate}%",
                delta=f"{hitl_rate - 80:.1f}% vs 80% target",
                delta_color="normal" if hitl_rate >= 80 else "inverse",
                help="% of campaigns that reached full human-in-the-loop review",
            )

            rejection_rate = mo.get("rejection_rate", 0.0)
            col3.metric(
                "👤 Manager Oversight",
                f"{rejection_rate}% rejected",
                delta=f"{mo.get('total_reviewed', 0)} reviewed",
                delta_color="off",
                help="Manager review activity: rejection rate + total reviews",
            )

            unsub_rate = asat.get("unsubscribe_rate", 0.0)
            col4.metric(
                "💌 Audience Satisfaction",
                f"{unsub_rate}%",
                delta=f"{unsub_rate - 1:.2f}% vs ≤1% target",
                delta_color="inverse" if unsub_rate > 1 else "normal",
                help="Unsubscribe rate (consent signal)",
            )

            st.divider()
            # -- RAI Framework table --
            with st.expander("🛡️ RAI Framework Definitions", expanded=False):
                st.markdown("""
| Dimension | Measured By | Data Source |
|---|---|---|
| **Content Safety** | Guardrail pass rate (PII + brand safety) | `GuardrailCheck` |
| **Process Transparency** | HITL completion (strategy + content reviewed) | `Campaign.status` |
| **Human Accountability** | Manager approval oversight | `Campaign.approved_by` |
| **Audience Privacy** | Unsubscribe rate (consent signal) | `RecipientEvent` |
            """)

        else:
            st.warning("Could not load RAI summary.")

        # -- Section 2: Guardrail Configuration --
        st.subheader("Guardrail Configuration")

        gconfig = api_get("/rai/guardrail-config")
        if gconfig:
            total_rules = sum(
                len(cat.get("rules", [])) for cat in gconfig.values()
            )
            st.caption(f"**{len(gconfig)}** categories — **{total_rules}** rules active")

            severity_badge = {"HIGH": "🔴 HIGH", "MEDIUM": "🟡 MEDIUM", "LOW": "⚪️ LOW"}
            for cat_name, cat in gconfig.items():
                badge = severity_badge.get(cat.get("severity", ""), cat.get("severity", ""))
                with st.expander(f"{badge} **{cat_name}**"):
                    st.caption(cat.get("description", ""))
                    rules = cat.get("rules", [])
                    if cat.get("type") == "pattern":
                        import pandas as pd
                        df = pd.DataFrame([
                            {"Rule": r.get("name"), "Pattern": r.get("pattern"), "Example": r.get("example", "")}
                            for r in rules
                        ])
                        st.dataframe(df, use_container_width=True, hide_index=True)
                    else:
                        phrases = [r.get("phrase") for r in rules]
                        st.markdown(", ".join(f"`{p}`" for p in phrases))
        else:
            st.warning("Could not load guardrail configuration.")

        st.divider()

        # -- Section 3: Recent Guardrail Checks --
        st.subheader("Recent Guardrail Checks")

        if st.button("🔄 Refresh Checks", key="rai_refresh_checks"):
            st.rerun()

        checks_data = api_get("/rai/checks?limit=50")
        if checks_data is None:
            st.warning("Could not load guardrail checks.")
        elif len(checks_data) == 0:
            st.info("No guardrail checks yet. Run a campaign to generate data.")
        else:
            n_passed = sum(1 for c in checks_data if c.get("passed"))
            n_failed = len(checks_data) - n_passed
            st.caption(
                f"**{len(checks_data)}** checks — "
                f"**{n_passed}** passed ✅ — "
                f"**{n_failed}** failed ❌"
            )

            import pandas as pd
            rows = []
            for c in checks_data:
                all_issues = (c.get("pii_issues") or []) + (c.get("brand_safety_issues") or [])
                rows.append({
                    "Campaign": (c.get("campaign_id") or "")[:12] + "...",
                    "Branch": (c.get("branch_id") or "")[:12] + "...",
                    "Variant": (c.get("variant_id") or "")[:12],
                    "Passed": "✅" if c.get("passed") else "❌",
                    "# Issues": len(all_issues) or 0,
                    "Issues": ", ".join(all_issues) if all_issues else "-",
                })
            st.dataframe(pd.DataFrame(rows), use_container_width=True, hide_index=True)

        st.divider()

        # -- Section 4: Per-Campaign RAI Drilldown --
        st.subheader("Per-Campaign RAI Drilldown")

        all_camps = api_get("/campaigns") or []
        if not all_camps:
            st.info("No campaigns found.")
        else:
            camp_options = {f"{c['id']}": f"{c['goal'][:55]}... [{c['status']}]" for c in all_camps}
            selected_cid = st.selectbox(
                "Select campaign",
                options=list(camp_options.keys()),
                format_func=lambda x: camp_options.get(x, x),
                key="rai_campaign_select",
            )

            if selected_cid:
                camp_checks = api_get(f"/rai/campaign/{selected_cid}")
                if not camp_checks:
                    st.info("No guardrail checks for this campaign yet.")
                else:
                    for chk in camp_checks:
                        passed = chk.get("passed", True)
                        badge = "✅ Passed" if passed else "❌ Failed"
                        branch_short = chk.get("branch_id") or "_"
                        variant_short = chk.get("variant_id") or "_"
                        with st.container():
                            c1, c2 = st.columns([1, 4])
                            c1.markdown(f"**{badge}**")
                            c2.markdown(
                                f"**Branch:** `{branch_short}` | **Variant:** `{variant_short}`"
                            )
                            
                            pii = chk.get("pii_issues") or []
                            brand = chk.get("brand_safety_issues") or []
                            if pii:
                                st.error(f"PII: " + ", ".join(pii))
                            if brand:
                                st.warning(f"Brand Safety: " + ", ".join(brand))
                            st.caption(f"Checked at: {(chk.get('checked_at') or '')[:19]}")
                            st.divider()