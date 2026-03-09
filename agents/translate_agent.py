"""
Translate Agent - translates email content to the reviewer's preferred language.

Enables reviewers who don't read the target language to understand and approve
emails before they go to recipients. The translated version is for review only - 
the original language version is always sent to recipients.

Logs every invocation to AgentRun so the monitor dashboard tracks translation
activity alongside the planning/creation/execution pipeline.
"""

from datetime import datetime
from loguru import logger

from core.config import get_settings

settings = get_settings()

def run_translate(
    text: str,
    source_language: str,
    target_language: str,
    run_id: str | None = None,
) -> str:
    """
    Translate *text* from source_language to target_language.

    text            - the email content to translate (subject + body combined)
    source_language - the language the email was written in (e.g. "Spanish")
    target_language - the reviewer's language (e.g. "English")
    run_id          - AgentRun.id for status updates

    Returns the translated string.
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
                        run.output_summary = str(output)[:300]
                    if error:
                        run.error_message = str(error)[:300]
                    db.commit()
            except Exception:
                pass

    try:
        prompt = (
            f"Translate the following email from {source_language} to {target_language}.\n"
            "Preserve all structure: subject line, greeting, body paragraphs, "
            "call-to-action, and sign-off.\n"
            "Return only the translated text - no commentary, no preamble.\n\n"
            f"Content:\n{text}"
        )

        llm = ChatGroq(api_key=settings.groq_api_key, model="llama3-8b-8192", temperature=0.1)
        response = llm.invoke([("human", prompt)])
        result = response.content

        _update_run("completed", output=result[:200])
        return result

    except Exception as e:
        logger.error(f"[TRANSLATE_AGENT] Error: {e}")
        _update_run("failed", error=str(e))
        raise RuntimeError(f"Translation failed: {e}") from e
    finally:
        db.close()