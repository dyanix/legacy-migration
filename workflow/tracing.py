"""
LangFuse tracing for every LLM call in the pipeline.

One module so each call site needs a single line, and so the "is tracing
configured?" question is answered in exactly one place. Two call sites use
it: agents/ai_mapper.py (schema mapping) and agents/doc_generator.py (data
dictionary table overviews).

Design rule: tracing NEVER blocks a migration. If langfuse isn't installed,
the keys aren't set, or the collector is unreachable, every function here
degrades to a no-op and the pipeline runs identically. An evaluator cloning
this repo without LangFuse credentials still gets a working pipeline --
observability is instrumentation, not a dependency.

Usage:
    from workflow.tracing import trace_config, flush

    llm.invoke(messages, config=trace_config("ai_mapper", prompt_id=pid))
    ...
    flush()   # once at the end of the run

Why flush() matters: the LangFuse SDK batches events and sends them on a
background thread. A short-lived script can exit before that thread sends
anything, so traces silently never appear in the UI even though the code
"worked". The orchestrator calls flush() once the graph completes.
"""

import os
import uuid

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    pass

# One session id per process, so every LLM call in a single pipeline run
# groups into one session in the LangFuse UI rather than scattering.
RUN_ID = os.getenv("LANGFUSE_SESSION_ID") or f"migration-{uuid.uuid4().hex[:12]}"

PUBLIC_KEY = os.getenv("LANGFUSE_PUBLIC_KEY")
SECRET_KEY = os.getenv("LANGFUSE_SECRET_KEY")
HOST = os.getenv("LANGFUSE_HOST", "https://cloud.langfuse.com")
ENABLED = os.getenv("LANGFUSE_ENABLED", "true").lower() not in ("false", "0", "no")

_handler = None
_resolved = False
_reason = "not initialised"


def _import_handler():
    """Returns the CallbackHandler class, or None.

    The import path moved between major versions -- langfuse 2.x exposed
    langfuse.callback, 3.x/4.x expose langfuse.langchain. Trying both means
    a version bump doesn't silently disable tracing across the pipeline.
    """
    for path in ("langfuse.langchain", "langfuse.callback"):
        try:
            module = __import__(path, fromlist=["CallbackHandler"])
            return getattr(module, "CallbackHandler")
        except Exception:
            continue
    return None


def _get_handler():
    global _handler, _resolved, _reason
    if _resolved:
        return _handler
    _resolved = True

    if not ENABLED:
        _reason = "disabled via LANGFUSE_ENABLED"
        return None
    if not (PUBLIC_KEY and SECRET_KEY):
        _reason = "LANGFUSE_PUBLIC_KEY / LANGFUSE_SECRET_KEY not set in .env"
        return None

    HandlerClass = _import_handler()
    if HandlerClass is None:
        _reason = "langfuse not installed (pip install langfuse)"
        return None

    try:
        # The v3+/v4 handler reads credentials from the configured client
        # rather than taking them as arguments, so construct the client
        # explicitly. This also surfaces bad keys here instead of at the
        # first LLM call, where the failure would be harder to attribute.
        from langfuse import Langfuse

        Langfuse(public_key=PUBLIC_KEY, secret_key=SECRET_KEY, host=HOST)
        _handler = HandlerClass()
        _reason = f"enabled ({HOST})"
    except Exception as e:
        _handler = None
        _reason = f"handler init failed: {type(e).__name__}: {e}"
    return _handler


def trace_config(node: str, **metadata) -> dict:
    """Builds the `config` dict for a LangChain .invoke() call.

    Returns {} when tracing is unavailable, which LangChain accepts as "no
    special config" -- so call sites need no conditional.

    `node` names the trace (ai_mapper, doc_generator). Extra keyword
    arguments ride along as trace metadata; pass prompt_id so a trace can be
    matched to the entry in audit/migration_audit_log.json, which is what
    makes an AI suggestion traceable end to end.
    """
    handler = _get_handler()
    if handler is None:
        return {}

    meta = {
        "langfuse_trace_name": node,
        "langfuse_session_id": RUN_ID,
        "langfuse_tags": ["legacy-migration", node],
    }
    meta.update({k: v for k, v in metadata.items() if v is not None})
    return {"callbacks": [handler], "metadata": meta}


def flush() -> None:
    """Sends any batched events. Safe to call when tracing is off."""
    if _get_handler() is None:
        return
    try:
        from langfuse import get_client

        get_client().flush()
    except Exception:
        try:
            from langfuse import Langfuse

            Langfuse().flush()
        except Exception:
            pass


def status() -> dict:
    """Diagnostic for the README/runbook and for verifying setup:
    python -c "from workflow.tracing import status; print(status())"
    """
    handler = _get_handler()
    return {
        "tracing_enabled": handler is not None,
        "reason": _reason,
        "session_id": RUN_ID,
        "host": HOST if handler else None,
        "public_key_set": bool(PUBLIC_KEY),
        "secret_key_set": bool(SECRET_KEY),
    }


if __name__ == "__main__":
    print(status())