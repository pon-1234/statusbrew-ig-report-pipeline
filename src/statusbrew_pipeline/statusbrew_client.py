from __future__ import annotations

import logging
from datetime import date
from typing import Any, Dict, List, Optional

import httpx
from tenacity import Retrying, stop_after_attempt, wait_exponential, retry_if_exception_type


logger = logging.getLogger(__name__)


class StatusbrewError(Exception):
    pass


class StatusbrewClient:
    def __init__(
        self,
        base_url: str,
        access_token: str,
        timeout_seconds: int = 60,
        retries: int = 3,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.client = httpx.Client(
            base_url=self.base_url,
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json",
            },
            timeout=timeout_seconds,
        )
        self.retries = retries
        self.retryer = Retrying(
            reraise=True,
            stop=stop_after_attempt(self.retries),
            wait=wait_exponential(multiplier=1, min=1, max=10),
            retry=retry_if_exception_type(StatusbrewError),
        )

    def _request(self, method: str, url: str, **kwargs) -> dict:
        for attempt in self.retryer:
            with attempt:
                try:
                    response = self.client.request(method, url, **kwargs)
                    response.raise_for_status()
                    return response.json()
                except httpx.HTTPError as exc:
                    logger.error("Statusbrew API error: %s", exc)
                    raise StatusbrewError(str(exc)) from exc

    def list_profiles(self, space_id: str) -> List[dict]:
        path = f"/v1/spaces/{space_id}/social_profiles"
        data = self._request("GET", path)
        return data.get("data") or data.get("profiles") or data

    def insights(
        self,
        space_id: str,
        metrics: List[str],
        dimensions: List[str],
        time_range: Dict[str, str],
        filters: Optional[Dict[str, Any]] = None,
        granularity: Optional[str] = None,
    ) -> List[dict]:
        body: Dict[str, Any] = {
            "metrics": metrics,
            "dimensions": dimensions,
            "time_range": time_range,
        }
        if filters:
            body["filters"] = filters
        if granularity:
            body["granularity"] = granularity
        path = f"/v1/spaces/{space_id}/insights"
        logger.debug("Insights request payload: %s", body)
        data = self._request("POST", path, json=body)
        return data.get("data") or data.get("rows") or data

    def fetch_profile_daily_metrics(
        self, space_id: str, profile_id: str, target_date: date
    ) -> List[dict]:
        return self.insights(
            space_id=space_id,
            metrics=[
                "followers",
                "followers_gained",
                "unfollowers",
                "actual_growth",
                "reach",
                "reach_from_organic",
                "reach_from_paid",
                "impressions",
                "profile_views",
                "bio_link_clicks",
            ],
            dimensions=["date", "profile"],
            time_range={"since": str(target_date), "until": str(target_date)},
            filters={"profile_ids": [profile_id], "platforms": ["instagram"]},
            granularity="day",
        )

    def fetch_post_snapshots(
        self,
        space_id: str,
        profile_ids: List[str],
        since: date,
        until: date,
    ) -> List[dict]:
        return self.insights(
            space_id=space_id,
            metrics=[
                "post_reach",
                "post_impressions",
                "post_reactions",
                "post_comments",
                "post_shares",
                "post_saved",
                "post_follows",
                "post_profile_activity_total",
                "post_profile_activity_bio_link_clicked",
            ],
            dimensions=["post", "profile"],
            time_range={"since": str(since), "until": str(until)},
            filters={"profile_ids": profile_ids, "platforms": ["instagram"]},
        )

    def fetch_follower_demographics(self, space_id: str, profile_id: str, snapshot_date: date) -> List[dict]:
        return self.insights(
            space_id=space_id,
            metrics=["followers"],
            dimensions=["profile", "gender", "age", "country", "city"],
            time_range={"since": str(snapshot_date), "until": str(snapshot_date)},
            filters={"profile_ids": [profile_id], "platforms": ["instagram"]},
        )

    def close(self) -> None:
        self.client.close()
