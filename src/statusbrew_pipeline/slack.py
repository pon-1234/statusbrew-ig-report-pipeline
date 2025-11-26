from __future__ import annotations

import logging
from typing import Optional

import httpx


logger = logging.getLogger(__name__)


class SlackNotifier:
    def __init__(self, webhook_url: Optional[str], channel: Optional[str] = None):
        self.webhook_url = webhook_url
        self.channel = channel

    def notify(self, text: str) -> None:
        if not self.webhook_url:
            logger.debug("Slack webhook not configured; skipping notification.")
            return
        payload = {"text": text}
        if self.channel:
            payload["channel"] = self.channel
        try:
            response = httpx.post(self.webhook_url, json=payload, timeout=10)
            response.raise_for_status()
        except Exception:
            logger.exception("Failed to send Slack notification")
