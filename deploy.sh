#!/bin/bash
#
# TDnet分析基盤 デプロイメントスクリプト
# - Cloud Function (日次スクレイピング)
# - Cloud Run ジョブ (期間指定バッチ処理)
#
set -e

# --- 引数のパース ---
SERVICE_ACCOUNT_KEY_FILE=""
PROJECT_ID=""

while [ $# -gt 0 ]; do
  case "$1" in
    --key-file)
      SERVICE_ACCOUNT_KEY_FILE="$2"
      shift 2
      ;;
    --project-id)
      PROJECT_ID="$2"
      shift 2
      ;;
    *)
      echo "不明な引数: $1"
      exit 1
      ;;
  esac
done

if [ -z "$SERVICE_ACCOUNT_KEY_FILE" ] || [ -z "$PROJECT_ID" ]; then
  echo "使用法: $0 --key-file path/to/key.json --project-id YOUR_PROJECT_ID"
  exit 1
fi

# --- 認証設定 ---
if [ ! -f "${SERVICE_ACCOUNT_KEY_FILE}" ]; then
    echo "❌ エラー: サービスアカウントキーファイルが見つかりません: ${SERVICE_ACCOUNT_KEY_FILE}"
    exit 1
fi
echo "🔑 サービスアカウント (${SERVICE_ACCOUNT_KEY_FILE}) を使用して認証します..."
gcloud auth activate-service-account --key-file="${SERVICE_ACCOUNT_KEY_FILE}"
echo "✅ 認証完了。プロジェクトID: ${PROJECT_ID} を使用してデプロイします。"

# --- 設定 ---
source deploy.env
REGION="${TDNET_REGION:-"asia-northeast1"}" # デフォルトは東京リージョン
SERVICE_ACCOUNT_EMAIL=$(grep "client_email" "${SERVICE_ACCOUNT_KEY_FILE}" | awk -F '"' '{print $4}')
if [ -z "${SERVICE_ACCOUNT_EMAIL}" ]; then
    echo "❌ エラー: サービスアカウントキーファイルからメールアドレスを取得できませんでした。"
    exit 1
fi
echo "ℹ️ 実行サービスアカウント: ${SERVICE_ACCOUNT_EMAIL}"

# --- リソース名 ---
# Cloud Function
CF_SCRAPER_NAME="tdnet-scraper"
# Cloud Scheduler
SCHEDULER_JOB_NAME="tdnet-scraper-daily-trigger"
# Artifact Registry
AR_REPO_NAME="tdnet-analyzer-repo"
# Cloud Run Jobs
CR_SUMMARY_JOB_NAME="tdnet-summary-generator"
CR_INSIGHT_JOB_NAME="tdnet-insight-generator"

IMAGE_NAME="${REGION}-docker.pkg.dev/${PROJECT_ID}/${AR_REPO_NAME}/tdnet-analyzer:latest"


# --- APIサービスの有効化 ---
echo "🔄 必要なAPIサービスを有効化しています..."
gcloud services enable cloudfunctions.googleapis.com cloudbuild.googleapis.com cloudscheduler.googleapis.com artifactregistry.googleapis.com run.googleapis.com --project "${PROJECT_ID}"


# --- セクション1: Cloud Function (日次スクレイピング) ---
echo -e "\n--- セクション1: Cloud Function (日次スクレイピング) のデプロイを開始 ---"

gcloud functions deploy "${CF_SCRAPER_NAME}" \
  --gen2 \
  --region "${REGION}" \
  --runtime python311 \
  --source . \
  --entry-point trigger_scraper \
  --trigger-http \
  --no-allow-unauthenticated \
  --memory "1Gi" \
  --timeout "540s" \
  --set-env-vars "PROJECT_ID=${PROJECT_ID}" \
  --service-account "${SERVICE_ACCOUNT_EMAIL}" \
  --project "${PROJECT_ID}"

CF_URL=$(gcloud functions describe "${CF_SCRAPER_NAME}" --region "${REGION}" --gen2 --format='value(serviceConfig.uri)' --project "${PROJECT_ID}")
echo "✅ Cloud Function (${CF_SCRAPER_NAME}) のデプロイ完了。"


# --- セクション2: Cloud Scheduler (日次実行トリガー) ---
echo -e "\n--- セクション2: Cloud Scheduler ジョブの作成/更新を開始 ---"

gcloud scheduler jobs create http "${SCHEDULER_JOB_NAME}" \
    --location "${REGION}" \
    --schedule="0 19 * * *" \
    --time-zone="Asia/Tokyo" \
    --uri="${CF_URL}" \
    --http-method=POST \
    --description="Trigger TDnet Scraper daily at 19:00 JST" \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --attempt-deadline="320s" \
    --project "${PROJECT_ID}" \
    || \
gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
    --location "${REGION}" \
    --schedule="0 19 * * *" \
    --uri="${CF_URL}" \
    --http-method=POST \
    --description="Trigger TDnet Scraper daily at 19:00 JST" \
    --oidc-service-account-email="${SERVICE_ACCOUNT_EMAIL}" \
    --project "${PROJECT_ID}"

echo "✅ Cloud Scheduler (${SCHEDULER_JOB_NAME}) の設定完了。"


# --- セクション3: Cloud Run ジョブ (期間指定バッチ) ---
echo -e "\n--- セクション3: Cloud Run ジョブのデプロイを開始 ---"

# 3a. Artifact Registry リポジトリの作成
echo "--- Artifact Registry リポジトリの確認/作成 ---"
if ! gcloud artifacts repositories describe "${AR_REPO_NAME}" --location="${REGION}" --project="${PROJECT_ID}" &>/dev/null; then
    echo "Artifact Registryリポジトリ ${AR_REPO_NAME} を作成します..."
    gcloud artifacts repositories create "${AR_REPO_NAME}" \
        --repository-format=docker \
        --location="${REGION}" \
        --description="TDnet Analyzer container images" \
        --project="${PROJECT_ID}"
else
    echo "Artifact Registryリポジトリ ${AR_REPO_NAME} は既に存在します。"
fi

# 3b. Dockerイメージのビルドとプッシュ
echo "--- Dockerイメージのビルドとプッシュ ---"
gcloud builds submit --tag "${IMAGE_NAME}" --project "${PROJECT_ID}"

# 3c. Cloud Run ジョブのデプロイ
COMMON_JOB_FLAGS=(
  --region "${REGION}"
  --service-account "${SERVICE_ACCOUNT_EMAIL}"
  --image "${IMAGE_NAME}"
  --tasks 1
  --task-timeout 3600
  --max-retries 0
  --cpu "${CR_CPU}"
  --memory "${CR_MEMORY}"
)
echo "--- Summary Generator ジョブのデプロイ (CPU: ${CR_CPU}, Memory: ${CR_MEMORY}) ---"
gcloud run jobs deploy "${CR_SUMMARY_JOB_NAME}" \
  "${COMMON_JOB_FLAGS[@]}" \
  --args="generate_summary.py" \
  --project "${PROJECT_ID}"

echo "--- Insight Generator ジョブのデプロイ (CPU: ${CR_CPU}, Memory: ${CR_MEMORY}) ---"
gcloud run jobs deploy "${CR_INSIGHT_JOB_NAME}" \
  "${COMMON_JOB_FLAGS[@]}" \
  --args="generate_sector_insights.py" \
  --project "${PROJECT_ID}"

echo "✅ Cloud Run ジョブのデプロイ完了。"
echo -e "\n🎉🎉🎉 すべてのデプロイ処理が正常に完了しました。 🎉🎉🎉" 