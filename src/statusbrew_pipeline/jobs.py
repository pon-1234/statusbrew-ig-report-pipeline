from __future__ import annotations

import logging
from datetime import date, datetime, timedelta
from typing import List, Optional

from dateutil import parser

from .models import ProfileDailyMetric, PostDailySnapshot, FollowerDemographics
from .slack import SlackNotifier
from .statusbrew_client import StatusbrewClient
from .bq import BigQueryService
from .config import Settings


logger = logging.getLogger(__name__)


def _parse_datetime(value: Optional[str | datetime]) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    try:
        return parser.parse(value)
    except Exception:
        return None


def _to_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_str(value) -> str:
    if value is None:
        return ""
    return str(value)


def _get(record: dict, key: str):
    if key in record:
        return record[key]
    if "metrics" in record and key in record["metrics"]:
        return record["metrics"][key]
    if "dimensions" in record and key in record["dimensions"]:
        return record["dimensions"][key]
    if "post" in record and isinstance(record["post"], dict) and key in record["post"]:
        return record["post"][key]
    if "profile" in record and isinstance(record["profile"], dict) and key in record["profile"]:
        return record["profile"][key]
    return None


class JobRunner:
    def __init__(
        self,
        settings: Settings,
        statusbrew: StatusbrewClient,
        bq: BigQueryService,
        notifier: SlackNotifier,
    ):
        self.settings = settings
        self.statusbrew = statusbrew
        self.bq = bq
        self.notifier = notifier

    def _yesterday(self) -> date:
        now = datetime.now(self.settings.tz)
        return (now - timedelta(days=1)).date()

    def run_profile_daily(self, target_date: Optional[date] = None) -> dict:
        target = target_date or self._yesterday()
        rows: List[dict] = []
        for space_id in self.settings.space_ids:
            profiles = self.statusbrew.list_profiles(space_id)
            for profile in profiles:
                if (profile.get("platform") or profile.get("platform_type")) != "instagram":
                    continue
                profile_id = profile.get("id") or profile.get("profile_id") or profile.get("uid")
                if not profile_id:
                    logger.warning("Profile ID missing in %s", profile)
                    continue
                username = profile.get("username") or profile.get("handle") or profile.get("name", "")
                records = self.statusbrew.fetch_profile_daily_metrics(space_id, profile_id, target)
                for record in records:
                    row = ProfileDailyMetric(
                        date=target,
                        space_id=space_id,
                        profile_id=str(profile_id),
                        profile_username=username or str(_get(record, "profile_username") or ""),
                        platform="instagram",
                        followers=_to_int(_get(record, "followers")),
                        followers_gained=_to_int(_get(record, "followers_gained")),
                        unfollowers=_to_int(_get(record, "unfollowers")),
                        actual_growth=_to_int(_get(record, "actual_growth")),
                        reach_total=_to_int(_get(record, "reach") or _get(record, "reach_total")),
                        reach_organic=_to_int(_get(record, "reach_from_organic")),
                        reach_paid=_to_int(_get(record, "reach_from_paid")),
                        impressions=_to_int(_get(record, "impressions")),
                        profile_views=_to_int(_get(record, "profile_views")),
                        bio_link_clicks=_to_int(_get(record, "bio_link_clicks")),
                    ).to_dict()
                    rows.append(row)
        self.bq.upsert_profile_daily(rows)
        self.notifier.notify(f"[ProfileDaily] Upserted {len(rows)} rows for {target}")
        return {"row_count": len(rows), "date": str(target)}

    def run_post_snapshots(self, snapshot_date: Optional[date] = None) -> dict:
        snapshot = snapshot_date or datetime.now(self.settings.tz).date()
        since = snapshot - timedelta(days=self.settings.recent_post_lookback_days)
        rows: List[dict] = []
        for space_id in self.settings.space_ids:
            profiles = self.statusbrew.list_profiles(space_id)
            profile_ids = [
                p.get("id") or p.get("profile_id") or p.get("uid")
                for p in profiles
                if (p.get("platform") or p.get("platform_type")) == "instagram"
            ]
            profile_map = {
                str(p.get("id") or p.get("profile_id") or p.get("uid")): p
                for p in profiles
                if (p.get("platform") or p.get("platform_type")) == "instagram"
            }
            if not profile_ids:
                continue
            records = self.statusbrew.fetch_post_snapshots(space_id, profile_ids, since, snapshot)
            for record in records:
                profile_id = str(_get(record, "profile_id") or _get(record, "profile"))
                profile_info = profile_map.get(profile_id, {})
                username = profile_info.get("username") or profile_info.get("name") or ""
                row = PostDailySnapshot(
                    snapshot_date=snapshot,
                    space_id=space_id,
                    profile_id=profile_id,
                    profile_username=username,
                    post_id=_safe_str(_get(record, "post_id") or _get(record, "post")),
                    post_permalink=_safe_str(_get(record, "post_permalink") or _get(record, "permalink")),
                    post_type=_safe_str(_get(record, "post_type") or _get(record, "type")),
                    post_published_at=_parse_datetime(
                        _get(record, "post_published_at") or _get(record, "post_created_at")
                    ),
                    reach_total=_to_int(_get(record, "post_reach")),
                    impressions_total=_to_int(_get(record, "post_impressions")),
                    likes=_to_int(_get(record, "post_reactions") or _get(record, "post_likes")),
                    comments=_to_int(_get(record, "post_comments")),
                    shares=_to_int(_get(record, "post_shares")),
                    saves=_to_int(_get(record, "post_saved") or _get(record, "post_saves")),
                    follows=_to_int(_get(record, "post_follows")),
                    profile_activity_total=_to_int(_get(record, "post_profile_activity_total")),
                    bio_link_clicks=_to_int(_get(record, "post_profile_activity_bio_link_clicked")),
                ).to_dict()
                rows.append(row)
        self.bq.upsert_post_snapshots(rows)
        self.notifier.notify(f"[PostSnapshots] Upserted {len(rows)} rows for {snapshot}")
        return {"row_count": len(rows), "snapshot_date": str(snapshot)}

    def run_follower_demographics(self, snapshot_date: Optional[date] = None) -> dict:
        snapshot = snapshot_date or datetime.now(self.settings.tz).date()
        rows: List[dict] = []
        for space_id in self.settings.space_ids:
            profiles = self.statusbrew.list_profiles(space_id)
            for profile in profiles:
                if (profile.get("platform") or profile.get("platform_type")) != "instagram":
                    continue
                profile_id = profile.get("id") or profile.get("profile_id") or profile.get("uid")
                username = profile.get("username") or profile.get("name") or ""
                records = self.statusbrew.fetch_follower_demographics(space_id, profile_id, snapshot)
                for record in records:
                    row = FollowerDemographics(
                        snapshot_date=snapshot,
                        space_id=space_id,
                        profile_id=str(profile_id),
                        profile_username=username,
                        age_group=_safe_str(_get(record, "age")),
                        gender=_safe_str(_get(record, "gender")),
                        country=_safe_str(_get(record, "country")),
                        city=_safe_str(_get(record, "city")),
                        followers=_to_int(_get(record, "followers")),
                    ).to_dict()
                    rows.append(row)
        self.bq.upsert_demographics(rows)
        self.notifier.notify(f"[Demographics] Upserted {len(rows)} rows for {snapshot}")
        return {"row_count": len(rows), "snapshot_date": str(snapshot)}
