-- Tables for Statusbrew Instagram pipeline

CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.sb_ig_profile_daily_metrics`
PARTITION BY date
CLUSTER BY profile_id AS
SELECT
  DATE '1970-01-01' AS date,
  "" AS space_id,
  "" AS profile_id,
  "" AS profile_username,
  "instagram" AS platform,
  CAST(NULL AS INT64) AS followers,
  CAST(NULL AS INT64) AS followers_gained,
  CAST(NULL AS INT64) AS unfollowers,
  CAST(NULL AS INT64) AS actual_growth,
  CAST(NULL AS INT64) AS reach_total,
  CAST(NULL AS INT64) AS reach_organic,
  CAST(NULL AS INT64) AS reach_paid,
  CAST(NULL AS INT64) AS impressions,
  CAST(NULL AS INT64) AS profile_views,
  CAST(NULL AS INT64) AS bio_link_clicks,
  CURRENT_TIMESTAMP() AS created_at,
  CURRENT_TIMESTAMP() AS updated_at
LIMIT 0;


CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.sb_ig_post_daily_snapshots`
PARTITION BY snapshot_date
CLUSTER BY profile_id, post_id AS
SELECT
  DATE '1970-01-01' AS snapshot_date,
  "" AS space_id,
  "" AS profile_id,
  "" AS profile_username,
  "" AS post_id,
  "" AS post_permalink,
  "" AS post_type,
  TIMESTAMP('1970-01-01 00:00:00') AS post_published_at,
  CAST(NULL AS INT64) AS reach_total,
  CAST(NULL AS INT64) AS impressions_total,
  CAST(NULL AS INT64) AS likes,
  CAST(NULL AS INT64) AS comments,
  CAST(NULL AS INT64) AS shares,
  CAST(NULL AS INT64) AS saves,
  CAST(NULL AS INT64) AS follows,
  CAST(NULL AS INT64) AS profile_activity_total,
  CAST(NULL AS INT64) AS bio_link_clicks,
  CURRENT_TIMESTAMP() AS created_at
LIMIT 0;


CREATE TABLE IF NOT EXISTS `${PROJECT_ID}.${DATASET}.sb_ig_follower_demographics`
PARTITION BY snapshot_date
CLUSTER BY profile_id AS
SELECT
  DATE '1970-01-01' AS snapshot_date,
  "" AS space_id,
  "" AS profile_id,
  "" AS profile_username,
  "" AS age_group,
  "" AS gender,
  "" AS country,
  "" AS city,
  CAST(NULL AS INT64) AS followers,
  CURRENT_TIMESTAMP() AS created_at
LIMIT 0;
