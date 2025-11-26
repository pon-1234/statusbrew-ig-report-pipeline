-- Views for reporting

CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.vw_ig_profile_daily_followers` AS
SELECT
  date,
  profile_id,
  profile_username,
  followers,
  followers_gained,
  unfollowers,
  actual_growth
FROM `${PROJECT_ID}.${DATASET}.sb_ig_profile_daily_metrics`;


CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.vw_ig_post_day7_metrics` AS
WITH numbered AS (
  SELECT
    *,
    DATE(post_published_at) AS post_date,
    DATE_DIFF(snapshot_date, DATE(post_published_at), DAY) AS days_since_post
  FROM `${PROJECT_ID}.${DATASET}.sb_ig_post_daily_snapshots`
)
SELECT
  post_id,
  profile_id,
  profile_username,
  post_permalink,
  post_type,
  post_published_at,
  reach_total AS reach_total_day7,
  impressions_total AS impressions_total_day7,
  likes AS likes_day7,
  comments AS comments_day7,
  shares AS shares_day7,
  saves AS saves_day7,
  follows AS follows_day7,
  profile_activity_total AS profile_activity_total_day7,
  bio_link_clicks AS bio_link_clicks_day7
FROM numbered
WHERE days_since_post = 7;


CREATE OR REPLACE VIEW `${PROJECT_ID}.${DATASET}.vw_ig_profile_monthly_summary` AS
WITH post_avg AS (
  SELECT
    profile_id,
    FORMAT_DATE('%Y-%m', DATE(post_published_at)) AS month,
    AVG(reach_total_day7) AS post_avg_reach
  FROM `${PROJECT_ID}.${DATASET}.vw_ig_post_day7_metrics`
  GROUP BY profile_id, month
)
SELECT
  FORMAT_DATE('%Y-%m', date) AS month,
  profile_id,
  profile_username,
  ARRAY_AGG(followers ORDER BY date DESC LIMIT 1)[OFFSET(0)] AS followers_closing,
  SUM(reach_total) AS reach_total_all,
  SUM(reach_organic) AS reach_total_organic,
  SUM(reach_paid) AS reach_total_paid,
  SUM(profile_views) AS profile_views_total,
  SUM(bio_link_clicks) AS hp_clicks_total,
  ANY_VALUE(post_avg.post_avg_reach) AS post_avg_reach
FROM `${PROJECT_ID}.${DATASET}.sb_ig_profile_daily_metrics` base
LEFT JOIN post_avg
  ON post_avg.profile_id = base.profile_id
  AND post_avg.month = FORMAT_DATE('%Y-%m', base.date)
GROUP BY month, profile_id, profile_username;
