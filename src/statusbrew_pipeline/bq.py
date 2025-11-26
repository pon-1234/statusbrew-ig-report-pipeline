from __future__ import annotations

import logging
import uuid
from typing import List

from google.cloud import bigquery

from .table_schemas import (
    PROFILE_DAILY_SCHEMA,
    POST_SNAPSHOT_SCHEMA,
    FOLLOWER_DEMOGRAPHICS_SCHEMA,
)


logger = logging.getLogger(__name__)


class BigQueryService:
    def __init__(
        self,
        project: str,
        dataset: str,
        table_profile_daily: str,
        table_post_snapshots: str,
        table_demographics: str,
    ):
        self.project = project
        self.dataset = dataset
        self.table_profile_daily = table_profile_daily
        self.table_post_snapshots = table_post_snapshots
        self.table_demographics = table_demographics
        self.client = bigquery.Client(project=project)

    def table_path(self, table_name: str) -> str:
        return f"{self.project}.{self.dataset}.{table_name}"

    def _load_temp_table(self, rows: List[dict], schema: List[bigquery.SchemaField]) -> str:
        temp_table_name = f"tmp_{uuid.uuid4().hex}"
        table_id = f"{self.project}.{self.dataset}.{temp_table_name}"
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            autodetect=False,
        )
        load_job = self.client.load_table_from_json(rows, table_id, job_config=job_config)
        load_job.result()
        logger.debug("Loaded %s rows into temporary table %s", len(rows), table_id)
        return temp_table_name

    def _merge(
        self,
        target_table: str,
        temp_table: str,
        key_columns: List[str],
        all_columns: List[str],
        update_columns: List[str] | None = None,
    ) -> None:
        on_clause = " AND ".join([f"T.{col} = S.{col}" for col in key_columns])
        update_columns = update_columns or [c for c in all_columns if c not in key_columns]
        update_clause = ", ".join([f"{col}=S.{col}" for col in update_columns])
        insert_columns = ", ".join(all_columns)
        insert_values = ", ".join([f"S.{col}" for col in all_columns])
        query = f"""
        MERGE `{self.table_path(target_table)}` AS T
        USING `{self.table_path(temp_table)}` AS S
        ON {on_clause}
        WHEN MATCHED THEN
          UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN
          INSERT ({insert_columns})
          VALUES ({insert_values})
        """
        logger.debug("Running merge for %s using %s", target_table, temp_table)
        self.client.query(query).result()
        logger.info("Upserted rows into %s", target_table)

    def upsert_profile_daily(self, rows: List[dict]) -> None:
        if not rows:
            logger.info("No profile daily metrics to upsert.")
            return
        temp_table = self._load_temp_table(rows, PROFILE_DAILY_SCHEMA)
        try:
            self._merge(
                target_table=self.table_profile_daily,
                temp_table=temp_table,
                key_columns=["date", "profile_id"],
                all_columns=[field.name for field in PROFILE_DAILY_SCHEMA],
                update_columns=[
                    c
                    for c in [field.name for field in PROFILE_DAILY_SCHEMA]
                    if c not in {"date", "profile_id", "created_at"}
                ],
            )
        finally:
            self.client.delete_table(self.table_path(temp_table), not_found_ok=True)

    def upsert_post_snapshots(self, rows: List[dict]) -> None:
        if not rows:
            logger.info("No post snapshots to upsert.")
            return
        temp_table = self._load_temp_table(rows, POST_SNAPSHOT_SCHEMA)
        try:
            self._merge(
                target_table=self.table_post_snapshots,
                temp_table=temp_table,
                key_columns=["snapshot_date", "post_id"],
                all_columns=[field.name for field in POST_SNAPSHOT_SCHEMA],
                update_columns=[
                    c
                    for c in [field.name for field in POST_SNAPSHOT_SCHEMA]
                    if c not in {"snapshot_date", "post_id", "created_at"}
                ],
            )
        finally:
            self.client.delete_table(self.table_path(temp_table), not_found_ok=True)

    def upsert_demographics(self, rows: List[dict]) -> None:
        if not rows:
            logger.info("No demographics to upsert.")
            return
        temp_table = self._load_temp_table(rows, FOLLOWER_DEMOGRAPHICS_SCHEMA)
        try:
            self._merge(
                target_table=self.table_demographics,
                temp_table=temp_table,
                key_columns=["snapshot_date", "profile_id", "age_group", "gender", "country", "city"],
                all_columns=[field.name for field in FOLLOWER_DEMOGRAPHICS_SCHEMA],
                update_columns=[
                    c
                    for c in [field.name for field in FOLLOWER_DEMOGRAPHICS_SCHEMA]
                    if c
                    not in {"snapshot_date", "profile_id", "age_group", "gender", "country", "city", "created_at"}
                ],
            )
        finally:
            self.client.delete_table(self.table_path(temp_table), not_found_ok=True)

    def recent_posts(self, lookback_days: int) -> List[dict]:
        query = f"""
        SELECT post_id, profile_id, MAX(post_published_at) AS post_published_at
        FROM `{self.table_path(self.table_post_snapshots)}`
        WHERE snapshot_date >= DATE_SUB(CURRENT_DATE(), INTERVAL @lookback DAY)
        GROUP BY post_id, profile_id
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[bigquery.ScalarQueryParameter("lookback", "INT64", lookback_days)]
        )
        result = self.client.query(query, job_config=job_config).result()
        return [dict(row) for row in result]
