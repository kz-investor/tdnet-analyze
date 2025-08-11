#!/bin/bash
#
# TDnet分析基盤 デプロイメントスクリプト
# - Cloud Function (日次スクレイピング)
# - Cloud Run ジョブ (期間指定バッチ処理)
#
set -e

# --- 認証設定 ---
if [ -z "$1" ]; then
    echo "❌ エラー: サービスアカウントキーファイルのパスを引数として指定してください。"
    echo "   使用法: ./deploy.sh keys/your-service-account-key.json"
    exit 1
fi
SERVICE_ACCOUNT_KEY_FILE="$1"
if [ ! -f "${SERVICE_ACCOUNT_KEY_FILE}" ]; then
    echo "❌ エラー: サービスアカウントキーファイルが見つかりません: ${SERVICE_ACCOUNT_KEY_FILE}"
    exit 1
fi

echo "🔑 サービスアカウント (${SERVICE_ACCOUNT_KEY_FILE}) を使用して認証します..."
gcloud auth activate-service-account --key-file="${SERVICE_ACCOUNT_KEY_FILE}"
PROJECT_ID=$(gcloud config get-value project)
echo "✅ 認証完了。プロジェクト: ${PROJECT_ID}"

# --- 設定 ---
source deploy.env
REGION="${TDNET_REGION:-"asia-northeast1"}" # デフォルトは東京リージョン
SERVICE_ACCOUNT_EMAIL="${TDNET_SERVICE_ACCOUNT:-$(gcloud iam service-accounts list --filter="displayName=tdnet-analyzer-sa" --format="value(email)")}"

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
echo "🔄 必要なAPIサービスを有効化しています (cloudfunctions, cloudbuild, cloudscheduler, artifactregistry, run)..."
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
  --service-account "${SERVICE_ACCOUNT_EMAIL}"

CF_URL=$(gcloud functions describe "${CF_SCRAPER_NAME}" --region "${REGION}" --gen2 --format='value(serviceConfig.uri)')
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
    || \
gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
    --location "${REGION}" \
    --schedule="0 19 * * *" \
    --uri="${CF_URL}" \
    --description="Trigger TDnet Scraper daily at 19:00 JST"

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
  --cpu "${CR_CPU:-1}"
  --memory "${CR_MEMORY:-2Gi}"
)
echo "--- Summary Generator ジョブのデプロイ (CPU: ${CR_CPU:-1}, Memory: ${CR_MEMORY:-2Gi}) ---"
gcloud run jobs deploy "${CR_SUMMARY_JOB_NAME}" \
  "${COMMON_JOB_FLAGS[@]}" \
  --command "python3" \
  --args "generate_summary.py" \
  --project "${PROJECT_ID}"

echo "--- Insight Generator ジョブのデプロイ (CPU: ${CR_CPU:-1}, Memory: ${CR_MEMORY:-2Gi}) ---"
gcloud run jobs deploy "${CR_INSIGHT_JOB_NAME}" \
  "${COMMON_JOB_FLAGS[@]}" \
  --command "python3" \
  --args "generate_sector_insights.py" \
  --project "${PROJECT_ID}"

echo "✅ Cloud Run ジョブのデプロイ完了。"
echo -e "\n🎉🎉🎉 すべてのデプロイ処理が正常に完了しました。 🎉🎉🎉" 