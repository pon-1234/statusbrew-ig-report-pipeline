from __future__ import annotations

import logging
from datetime import date
from typing import Optional

from fastapi import FastAPI, HTTPException, Query

from .config import get_settings
from .logging_utils import configure_logging
from .secrets import fetch_secret
from .statusbrew_client import StatusbrewClient
from .bq import BigQueryService
from .slack import SlackNotifier
from .jobs import JobRunner


configure_logging()
logger = logging.getLogger(__name__)

settings = get_settings()

token = settings.statusbrew_access_token
if not token and settings.statusbrew_token_secret_name:
    token = fetch_secret(settings.statusbrew_token_secret_name, settings.secret_project_id or settings.gcp_project)
if not token:
    raise RuntimeError("Statusbrew access token is required.")

statusbrew_client = StatusbrewClient(
    base_url=settings.statusbrew_base_url,
    access_token=token,
    timeout_seconds=settings.http_timeout_seconds,
    retries=settings.http_retries,
)
bq_service = BigQueryService(
    project=settings.gcp_project,
    dataset=settings.bigquery_dataset,
    table_profile_daily=settings.table_profile_daily,
    table_post_snapshots=settings.table_post_snapshots,
    table_demographics=settings.table_demographics,
)
notifier = SlackNotifier(webhook_url=settings.slack_webhook_url, channel=settings.slack_channel)
runner = JobRunner(settings, statusbrew_client, bq_service, notifier)

app = FastAPI(title="Statusbrew Instagram Pipeline", version="1.0.0")


@app.get("/healthz")
def healthz():
    return {"status": "ok"}


@app.post("/job/profile_daily")
def profile_daily(target_date: Optional[date] = Query(None, description="YYYY-MM-DD")):
    try:
        return runner.run_profile_daily(target_date)
    except Exception as exc:
        notifier.notify(f"[ProfileDaily] Failed: {exc}")
        logger.exception("Profile daily job failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/job/post_snapshots")
def post_snapshots(snapshot_date: Optional[date] = Query(None, description="YYYY-MM-DD")):
    try:
        return runner.run_post_snapshots(snapshot_date)
    except Exception as exc:
        notifier.notify(f"[PostSnapshots] Failed: {exc}")
        logger.exception("Post snapshots job failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.post("/job/follower_demographics")
def follower_demographics(snapshot_date: Optional[date] = Query(None, description="YYYY-MM-DD")):
    try:
        return runner.run_follower_demographics(snapshot_date)
    except Exception as exc:
        notifier.notify(f"[Demographics] Failed: {exc}")
        logger.exception("Demographics job failed")
        raise HTTPException(status_code=500, detail=str(exc))


@app.on_event("shutdown")
def shutdown_event():
    statusbrew_client.close()
