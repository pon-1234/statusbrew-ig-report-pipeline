# Statusbrew Instagram Report Pipeline

Cloud Run 向けの Python/FastAPI サービスです。Statusbrew Analytics API から Instagram 指標を取得し、BigQuery に蓄積・ビューを提供し、Connected Sheets / Slides の自動レポート化を前提にしています。

## リポジトリ構成

- `src/statusbrew_pipeline/` — アプリ本体
- `sql/tables.sql` — BigQuery テーブル DDL（パーティション / クラスタ設定付き）
- `sql/views.sql` — ビュー定義（FR-4〜6）
- `Dockerfile` — Cloud Run デプロイ用
- `.env.example` — 必須環境変数例
- `tests/` — 簡易テスト

## 機能概要

- `/job/profile_daily` — FR-1: プロフィール日次指標を取得し upsert
- `/job/post_snapshots` — FR-2: 投稿 Lifetime 指標を日次スナップショット化し upsert
- `/job/follower_demographics` — FR-3: フォロワーデモグラのスナップショット取得
- Slack Webhook でジョブ成功/失敗を通知（任意設定）
- BigQuery スキーマ・ビューは FR-4〜6 を満たす形で同梱
- Cloud Scheduler から HTTP トリガー想定（7-1〜7-4）

## 前提

- Python 3.11
- Statusbrew Access Token（Secret Manager or 環境変数）
- GCP: BigQuery / Cloud Run / Cloud Scheduler / Secret Manager

## セットアップ

1. 依存関係インストール

   ```bash
   pip install -r requirements.txt
   ```

2. 環境変数を設定（`.env` を `.env.example` から作成）

   主要項目:

   - `GCP_PROJECT` — BigQuery 書き込み先プロジェクト
   - `BIGQUERY_DATASET` — データセット名（例: `statusbrew_ig`）
   - `SPACE_IDS` — 取得対象 Space ID をカンマ区切り
   - `STATUSBREW_ACCESS_TOKEN` または `STATUSBREW_TOKEN_SECRET_NAME`
   - `TIMEZONE` — デフォルト `Asia/Tokyo`
   - `RECENT_POST_LOOKBACK_DAYS` — 投稿スナップショット対象期間（既定 10日）
   - `SLACK_WEBHOOK_URL` — 任意

3. BigQuery スキーマ作成

   ```bash
   export PROJECT_ID=your-project
   export DATASET=statusbrew_ig
   envsubst < sql/tables.sql | bq query --use_legacy_sql=false
   envsubst < sql/views.sql  | bq query --use_legacy_sql=false
   ```

## ローカル実行

```bash
export PYTHONPATH=./src
uvicorn statusbrew_pipeline.main:app --reload --port 8080
```

ヘルスチェック: `curl http://localhost:8080/healthz`

ジョブ実行例:

```bash
curl -X POST 'http://localhost:8080/job/profile_daily?target_date=2025-03-01'
curl -X POST 'http://localhost:8080/job/post_snapshots'
curl -X POST 'http://localhost:8080/job/follower_demographics'
```

## Cloud Run デプロイ

```bash
gcloud builds submit --tag gcr.io/$PROJECT_ID/statusbrew-ig
gcloud run deploy statusbrew-ig \
  --image gcr.io/$PROJECT_ID/statusbrew-ig \
  --platform managed \
  --region asia-northeast1 \
  --allow-unauthenticated \
  --set-env-vars GCP_PROJECT=$PROJECT_ID,BIGQUERY_DATASET=statusbrew_ig,SPACE_IDS=xxxxx \
  --set-env-vars STATUSBREW_TOKEN_SECRET_NAME=statusbrew-access-token \
  --set-env-vars TIMEZONE=Asia/Tokyo,RECENT_POST_LOOKBACK_DAYS=10 \
  --set-env-vars SLACK_WEBHOOK_URL=https://hooks.slack.com/services/... \
  --service-account statusbrew-ig@${PROJECT_ID}.iam.gserviceaccount.com
```

## Cloud Scheduler サンプル（7-1 に対応）

```bash
gcloud scheduler jobs create http profile-daily \
  --schedule="0 3 * * *" \
  --uri="https://<cloud-run-url>/job/profile_daily" \
  --http-method=POST

gcloud scheduler jobs create http post-snapshots \
  --schedule="30 3 * * *" \
  --uri="https://<cloud-run-url>/job/post_snapshots" \
  --http-method=POST

gcloud scheduler jobs create http follower-demographics \
  --schedule="0 4 * * *" \
  --uri="https://<cloud-run-url>/job/follower_demographics" \
  --http-method=POST
```

## テーブル仕様（抜粋）

- `sb_ig_profile_daily_metrics` — `date` パーティション / `profile_id` クラスタ
- `sb_ig_post_daily_snapshots` — `snapshot_date` パーティション / `profile_id, post_id` クラスタ
- `sb_ig_follower_demographics` — `snapshot_date` パーティション / `profile_id` クラスタ

ビュー:

- `vw_ig_profile_monthly_summary`
- `vw_ig_profile_daily_followers`
- `vw_ig_post_day7_metrics`

## コード概要

- `statusbrew_client.py` — Statusbrew Insights API クライアント（リトライ付き）
- `jobs.py` — FR-1/2/3 のジョブロジック + Slack 通知
- `bq.py` — BigQuery upsert（テンポラリテーブル経由 MERGE）
- `main.py` — FastAPI エンドポイント（Cloud Scheduler から HTTP 呼び出し）
- `table_schemas.py` — テーブルスキーマ

## 運用メモ

- ジョブ失敗時は Slack 通知、Cloud Logging で詳細確認
- 28〜30日制限対策として毎日スナップショットを取得
- 月次サマリーの更新は Connected Sheets または Apps Script で 05:00 以降にリフレッシュ
- 手動入力が必要な指標は別シート `manual_input` を用意し、人手で更新

## テスト

```bash
pytest
```
