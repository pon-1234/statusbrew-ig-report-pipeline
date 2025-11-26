from __future__ import annotations

import os
from typing import List, Optional
from zoneinfo import ZoneInfo

from pydantic import BaseSettings, Field, validator
from dotenv import load_dotenv


load_dotenv()


class Settings(BaseSettings):
    """Runtime configuration loaded from environment variables."""

    gcp_project: str = Field(..., env="GCP_PROJECT")
    bigquery_dataset: str = Field("statusbrew_ig", env="BIGQUERY_DATASET")
    table_profile_daily: str = Field("sb_ig_profile_daily_metrics", env="TABLE_PROFILE_DAILY")
    table_post_snapshots: str = Field("sb_ig_post_daily_snapshots", env="TABLE_POST_SNAPSHOTS")
    table_demographics: str = Field("sb_ig_follower_demographics", env="TABLE_DEMOGRAPHICS")

    statusbrew_base_url: str = Field("https://api.statusbrew.com", env="STATUSBREW_BASE_URL")
    statusbrew_access_token: Optional[str] = Field(None, env="STATUSBREW_ACCESS_TOKEN")
    statusbrew_token_secret_name: Optional[str] = Field(None, env="STATUSBREW_TOKEN_SECRET_NAME")
    secret_project_id: Optional[str] = Field(None, env="SECRET_PROJECT_ID")

    space_ids: List[str] = Field(..., env="SPACE_IDS")
    timezone: str = Field("Asia/Tokyo", env="TIMEZONE")
    recent_post_lookback_days: int = Field(10, env="RECENT_POST_LOOKBACK_DAYS")
    http_timeout_seconds: int = Field(60, env="HTTP_TIMEOUT_SECONDS")
    http_retries: int = Field(3, env="HTTP_RETRIES")

    slack_webhook_url: Optional[str] = Field(None, env="SLACK_WEBHOOK_URL")
    slack_channel: Optional[str] = Field(None, env="SLACK_CHANNEL")

    app_port: int = Field(8080, env="PORT")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"

    @validator("space_ids", pre=True)
    def parse_space_ids(cls, value: str | list[str]) -> list[str]:
        if isinstance(value, list):
            return value
        return [v.strip() for v in value.split(",") if v.strip()]

    @property
    def tz(self) -> ZoneInfo:
        return ZoneInfo(self.timezone)

    def require_token(self) -> str:
        if self.statusbrew_access_token:
            return self.statusbrew_access_token
        env_name = "STATUSBREW_ACCESS_TOKEN"
        raise RuntimeError(
            f"Statusbrew access token is not set. Provide {env_name} or configure Secret Manager."
        )


def get_settings() -> Settings:
    return Settings()
