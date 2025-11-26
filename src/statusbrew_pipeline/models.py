from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import date, datetime
from typing import Optional


def utcnow() -> datetime:
    return datetime.utcnow()


@dataclass
class ProfileDailyMetric:
    date: date
    space_id: str
    profile_id: str
    profile_username: str
    platform: str
    followers: Optional[int] = None
    followers_gained: Optional[int] = None
    unfollowers: Optional[int] = None
    actual_growth: Optional[int] = None
    reach_total: Optional[int] = None
    reach_organic: Optional[int] = None
    reach_paid: Optional[int] = None
    impressions: Optional[int] = None
    profile_views: Optional[int] = None
    bio_link_clicks: Optional[int] = None
    created_at: datetime = None
    updated_at: datetime = None

    def to_dict(self) -> dict:
        payload = asdict(self)
        now = utcnow()
        payload["created_at"] = self.created_at or now
        payload["updated_at"] = self.updated_at or now
        return payload


@dataclass
class PostDailySnapshot:
    snapshot_date: date
    space_id: str
    profile_id: str
    profile_username: str
    post_id: str
    post_permalink: Optional[str]
    post_type: Optional[str]
    post_published_at: Optional[datetime]
    reach_total: Optional[int] = None
    impressions_total: Optional[int] = None
    likes: Optional[int] = None
    comments: Optional[int] = None
    shares: Optional[int] = None
    saves: Optional[int] = None
    follows: Optional[int] = None
    profile_activity_total: Optional[int] = None
    bio_link_clicks: Optional[int] = None
    created_at: datetime = None

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["created_at"] = self.created_at or utcnow()
        return payload


@dataclass
class FollowerDemographics:
    snapshot_date: date
    space_id: str
    profile_id: str
    profile_username: str
    age_group: str
    gender: str
    country: str
    city: str
    followers: Optional[int]
    created_at: datetime = None

    def to_dict(self) -> dict:
        payload = asdict(self)
        payload["created_at"] = self.created_at or utcnow()
        return payload
