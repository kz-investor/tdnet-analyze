#!/bin/bash
#
# 手動でデータ処理パイプラインを開始するためのヘルパースクリプト
# (Cloud Run ジョブを順番に実行)
#
set -e

# --- 引数のパース ---
START_DATE=""
END_DATE=""
SERVICE_ACCOUNT_KEY_FILE=""
PROJECT_ID=""

while [ $# -gt 0 ]; do
  case "$1" in
    --start-date)
      START_DATE="$2"
      shift 2
      ;;
    --end-date)
      END_DATE="$2"
      shift 2
      ;;
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

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ] || [ -z "$SERVICE_ACCOUNT_KEY_FILE" ] || [ -z "$PROJECT_ID" ]; then
  echo "使用法: $0 --start-date YYYYMMDD --end-date YYYYMMDD --key-file path/to/key.json --project-id YOUR_PROJECT_ID"
  exit 1
fi

if [ ! -f "${SERVICE_ACCOUNT_KEY_FILE}" ]; then
    echo "❌ エラー: サービスアカウントキーファイルが見つかりません: ${SERVICE_ACCOUNT_KEY_FILE}"
    exit 1
fi

# --- gcloudコマンドの存在確認 ---
if ! command -v gcloud &> /dev/null
then
    echo "gcloud コマンドが見つかりません。Google Cloud SDKがインストールされ、PATHが通っているか確認してください。"
    exit 1
fi

# --- 認証 ---
echo "🔑 サービスアカウント (${SERVICE_ACCOUNT_KEY_FILE}) を使用して認証します..."
gcloud auth activate-service-account --key-file="${SERVICE_ACCOUNT_KEY_FILE}" --project "${PROJECT_ID}"
echo "✅ 認証完了。プロジェクトID: ${PROJECT_ID} で実行します。"


# --- 設定 ---
SUMMARY_JOB_NAME="tdnet-summary-generator"
INSIGHT_JOB_NAME="tdnet-insight-generator"
REGION="${TDNET_REGION:-"asia-northeast1"}"

# --- ステップ1: サマリー生成ジョブの実行 ---
echo "🚀 ステップ1: サマリー生成ジョブを開始します..."
echo "   ジョブ名: ${SUMMARY_JOB_NAME}"
echo "   期間: ${START_DATE} から ${END_DATE}"
gcloud run jobs execute "${SUMMARY_JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --wait \
  --args="generate_summary.py,--start-date,${START_DATE},--end-date,${END_DATE},--project,${PROJECT_ID}"

echo "✅ ステップ1: サマリー生成ジョブが完了しました。"
echo ""


# --- ステップ2: インサイト生成ジョブの実行 ---
echo "🚀 ステップ2: インサイト生成ジョブを開始します..."
echo "   ジョブ名: ${INSIGHT_JOB_NAME}"
echo "   期間: ${START_DATE} から ${END_DATE}"
gcloud run jobs execute "${INSIGHT_JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --region "${REGION}" \
  --wait \
  --args="generate_sector_insights.py,--start-date,${START_DATE},--end-date,${END_DATE},--project,${PROJECT_ID}"

echo "✅ ステップ2: インサイト生成ジョブが完了しました。"
echo ""
echo "🎉 すべての処理が正常に完了しました。" 