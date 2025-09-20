import os
import subprocess
import logging
import sentry_sdk
from sentry_sdk.integrations.logging import LoggingIntegration
from sentry_sdk.integrations.asyncio import AsyncioIntegration
try:
    # handle AioHttp integration because it may not be available in all environments
    from sentry_sdk.integrations.aiohttp import AioHttpIntegration
except Exception:
    AioHttpIntegration = None

LOG = logging.getLogger(__name__)


def _get_release():
    try:
        sha = (
            subprocess.check_output(["git", "rev-parse", "--short", "HEAD"])
            .decode()
            .strip()
        )
        return sha
    except Exception:
        return os.getenv("RELEASE", None)


def before_send(event, hint):
    """Modify events before they are sent to Sentry.
    It trims overly large message bodies and attachments to avoid huge events.
    """
    try:
        # Trim request data if present
        req = event.get("request")
        if req:
            for k in ("data", "body"):
                if req.get(k):
                    v = req.get(k)
                    if isinstance(v, str) and len(v) > 2000:
                        req[k] = v[:2000] + "...[truncated]"
    except Exception:
        LOG.exception("before_send: trimming failed")
    return event


def init_sentry():
    dsn = os.getenv("SENTRY_DSN")
    if not dsn:
        LOG.info("SENTRY_DSN not set, skipping sentry init")
        return

    sentry_logging = LoggingIntegration(
        level=logging.INFO, event_level=logging.ERROR
    )

    integrations = [sentry_logging, AsyncioIntegration()]
    if AioHttpIntegration is not None:
        integrations.append(AioHttpIntegration())

    traces_sample_rate = float(os.getenv("SENTRY_TRACES_SAMPLE_RATE", "0.0"))
    sample_rate = float(os.getenv("SENTRY_SAMPLE_RATE", "1.0"))
    environment = os.getenv("SENTRY_ENVIRONMENT", "production")
    release = _get_release()

    sentry_sdk.init(
        dsn=dsn,
        integrations=integrations,
        traces_sample_rate=traces_sample_rate,
        sample_rate=sample_rate,
        environment=environment,
        release=release,
        before_send=before_send,
        send_default_pii=True,
    )


def set_user_context_from_update(update):
    try:
        if not update:
            return
        if hasattr(update, "effective_user") and update.effective_user:
            u = update.effective_user
            sentry_sdk.set_user({"id": str(u.id), "username": u.username or None})
    except Exception:
        LOG.exception("Failed to set user context")


def capture_update_exception(update, exc):
    try:
        set_user_context_from_update(update)
        sentry_sdk.capture_exception(exc)
    except Exception:
        LOG.exception("Failed to capture exception to Sentry")


def capture_exception(exc, extra=None, tags=None):
    try:
        with sentry_sdk.push_scope() as scope:
            if tags:
                for k, v in tags.items():
                    scope.set_tag(k, v)
            if extra:
                for k, v in extra.items():
                    scope.set_extra(k, v)
            sentry_sdk.capture_exception(exc)
    except Exception:
        LOG.exception("Failed to capture exception to Sentry")


def capture_cron_event(name, status="start", extra=None):
    """Capture a lightweight cron-run message (start/finish/error).

    This is used by cron-invoked scripts like `check_products.py` so we can
    monitor successful runs and failures.
    """
    try:
        message = f"cron:{name}:{status}"
        with sentry_sdk.push_scope() as scope:
            scope.set_tag("cron", name)
            scope.set_tag("cron_status", status)
            if extra:
                for k, v in extra.items():
                    scope.set_extra(k, v)
            sentry_sdk.capture_message(message)
    except Exception:
        LOG.exception("Failed to capture cron event to Sentry")


def create_task_catching(coro):
    """Create asyncio Task and report exceptions to Sentry when they occur."""
    import asyncio

    task = asyncio.create_task(coro)

    def _cb(t):
        try:
            exc = t.exception()
            if exc:
                capture_exception(exc)
        except asyncio.CancelledError:
            pass
        except Exception:
            LOG.exception("Error in task done callback")

    task.add_done_callback(_cb)
    return task
