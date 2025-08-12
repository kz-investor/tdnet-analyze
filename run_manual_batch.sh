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
SECTOR_MODE=false

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
    --sector-mode)
      SECTOR_MODE=true
      shift
      ;;
    *)
      echo "不明な引数: $1"
      exit 1
      ;;
  esac
done

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ] || [ -z "$SERVICE_ACCOUNT_KEY_FILE" ] || [ -z "$PROJECT_ID" ]; then
  echo "使用法: $0 --start-date YYYYMMDD --end-date YYYYMMDD --key-file path/to/key.json --project-id YOUR_PROJECT_ID [--sector-mode]"
  echo "  --sector-mode: 業種別データ処理モード（新機能1対応）"
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
if [ "$SECTOR_MODE" = true ]; then
  # 業種別モード
  gcloud run jobs execute "${SUMMARY_JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --wait \
    --args=-m \
    --args=tdnet_analyzer.batch.generate_summary \
    --args=--start-date \
    --args="${START_DATE}" \
    --args=--end-date \
    --args="${END_DATE}" \
    --args=--project \
    --args="${PROJECT_ID}" \
    --args=--sector-mode
else
  # 通常モード
  gcloud run jobs execute "${SUMMARY_JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --wait \
    --args=-m \
    --args=tdnet_analyzer.batch.generate_summary \
    --args=--start-date \
    --args="${START_DATE}" \
    --args=--end-date \
    --args="${END_DATE}" \
    --args=--project \
    --args="${PROJECT_ID}"
fi

echo "✅ ステップ1: サマリー生成ジョブが完了しました。"
echo ""


# --- ステップ2: インサイト生成ジョブの実行 ---
echo "🚀 ステップ2: インサイト生成ジョブを開始します..."
echo "   ジョブ名: ${INSIGHT_JOB_NAME}"
echo "   期間: ${START_DATE} から ${END_DATE}"
if [ "$SECTOR_MODE" = true ]; then
  # 業種別モード
  gcloud run jobs execute "${INSIGHT_JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --wait \
    --args=-m \
    --args=tdnet_analyzer.batch.generate_sector_insights \
    --args=--start-date \
    --args="${START_DATE}" \
    --args=--end-date \
    --args="${END_DATE}" \
    --args=--project \
    --args="${PROJECT_ID}" \
    --args=--sector-mode
else
  # 通常モード
  gcloud run jobs execute "${INSIGHT_JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --region "${REGION}" \
    --wait \
    --args=-m \
    --args=tdnet_analyzer.batch.generate_sector_insights \
    --args=--start-date \
    --args="${START_DATE}" \
    --args=--end-date \
    --args="${END_DATE}" \
    --args=--project \
    --args="${PROJECT_ID}"
fi

echo "✅ ステップ2: インサイト生成ジョブが完了しました。"
echo ""
echo "🎉 すべての処理が正常に完了しました。" 