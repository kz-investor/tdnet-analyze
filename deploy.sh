#!/bin/bash

# Cloud Functions Deployment Script
# 重要: internal-only + 認証必須でデプロイ（外部アクセス不可）
set -e

# 一時差し替え用のトラップ設定（終了時に必ず復元）
ORIG_REQ="requirements.txt"
BACKUP_REQ="requirements.local.bak"
RESTORE() {
  if [ -f "$BACKUP_REQ" ]; then
    mv -f "$BACKUP_REQ" "$ORIG_REQ"
  fi
}
trap RESTORE EXIT

# --- 設定 ---
# 環境変数設定を読み込み
source deploy.env

# Cloud Functions
FUNCTION_NAME="tdnet-scraper" # スクレイピング用
REGION="${TDNET_REGION}"
MEMORY="${TDNET_MEMORY}"
TIMEOUT="${TDNET_TIMEOUT}"
MAX_INSTANCES="${TDNET_MAX_INSTANCES}"

# Cloud Scheduler
PROJECT_ID=$(gcloud config get-value project)
SERVICE_ACCOUNT="${PROJECT_ID}@appspot.gserviceaccount.com" # デフォルトのApp Engine SA
JOB_NAME="tdnet-scrape-daily"
CRON="0 19 * * *"  # 毎日19:00 JST
TIME_ZONE="Asia/Tokyo"

# --- スクリプト本体 ---
echo "Cloud Functions (${FUNCTION_NAME}) のデプロイを開始します..."

# ログ保持期間の更新
set +e
gcloud logging buckets update _Default --location=global --retention-days=3 2>/dev/null
set -e

# 依存関係ファイルを一時的に切り替え
if [ -f "requirements.txt" ] && [ -f "requirements-functions.txt" ]; then
    mv requirements.txt requirements.txt.bak
    mv requirements-functions.txt requirements.txt
fi

# gcloud deploy コマンド
gcloud functions deploy "${FUNCTION_NAME}" \
  --gen2 \
  --region "${REGION}" \
  --runtime python311 \
  --source . \
  --entry-point trigger_scraper \
  --trigger-http \
  --ingress-settings=internal-only \
  --no-allow-unauthenticated \
  --memory "${MEMORY}" \
  --timeout "${TIMEOUT}" \
  --max-instances "${MAX_INSTANCES}" \
  --set-env-vars "PROJECT_ID=${PROJECT_ID},REGION=${REGION},LOG_EXECUTION_ID=true"

# 依存関係ファイルを元に戻す
if [ -f "requirements.txt.bak" ]; then
    mv requirements.txt requirements-functions.txt
    mv requirements.txt.bak requirements.txt
fi

FUNCTION_URL=$(gcloud functions describe "${FUNCTION_NAME}" --region "${REGION}" --gen2 --format='value(serviceConfig.uri)')

echo "Cloud Functions (${FUNCTION_NAME}) のデプロイが完了しました。"
echo "関数URL: ${FUNCTION_URL}"

# --- Cloud Scheduler ジョブ (Scrape) ---
echo "Cloud Schedulerジョブ (${JOB_NAME}) を作成/更新します..."

# Cloud Schedulerジョブの作成または更新
if gcloud scheduler jobs describe "${JOB_NAME}" --location "${REGION}" &>/dev/null; then
  echo "既存のCloud Schedulerジョブを更新します..."
  gcloud scheduler jobs update http "${JOB_NAME}" \
    --location "${REGION}" \
    --schedule="${CRON}" \
    --uri="${FUNCTION_URL}" \
    --message-body='{"task":"scrape"}' \
    --update-headers='Content-Type=application/json' \
    --description="Trigger TDnet Scraper daily at 19:00 JST"
else
  echo "新しいCloud Schedulerジョブを作成します..."
  gcloud scheduler jobs create http "${JOB_NAME}" \
    --location "${REGION}" \
    --schedule="${CRON}" \
    --time-zone="${TIME_ZONE}" \
    --uri="${FUNCTION_URL}" \
    --http-method=POST \
    --message-body='{"task":"scrape"}' \
    --headers='Content-Type=application/json' \
    --description="Trigger TDnet Scraper daily at 19:00 JST" \
    --oidc-service-account-email="${SERVICE_ACCOUNT}"
fi

echo "Cloud Schedulerジョブ (${JOB_NAME}) の設定が完了しました。" 