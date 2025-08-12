#!/bin/bash
#
# 指定した日付でTDnetスクレイピングを実行するためのスクリプト
# (Cloud SchedulerのURLを一時的に書き換えて実行し、自動で元に戻す安全な方式)
#
set -e

# --- 引数のパース ---
EXEC_DATE=""
SERVICE_ACCOUNT_KEY_FILE=""
PROJECT_ID=""

while [ $# -gt 0 ]; do
  case "$1" in
    --date)
      EXEC_DATE="$2"
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

if [ -z "$EXEC_DATE" ] || [ -z "$SERVICE_ACCOUNT_KEY_FILE" ] || [ -z "$PROJECT_ID" ]; then
  echo "使用法: $0 --date YYYYMMDD --key-file path/to/key.json --project-id YOUR_PROJECT_ID"
  exit 1
fi

if ! [[ "$EXEC_DATE" =~ ^[0-9]{8}$ ]]; then
  echo "エラー: 日付のフォーマットが不正です。YYYYMMDD形式で指定してください。"
  exit 1
fi

if [ ! -f "${SERVICE_ACCOUNT_KEY_FILE}" ]; then
    echo "❌ エラー: サービスアカウントキーファイルが見つかりません: ${SERVICE_ACCOUNT_KEY_FILE}"
    exit 1
fi

# --- 認証 ---
echo "🔑 サービスアカウント (${SERVICE_ACCOUNT_KEY_FILE}) を使用して認証します..."
gcloud auth activate-service-account --key-file="${SERVICE_ACCOUNT_KEY_FILE}" --project "${PROJECT_ID}"
echo "✅ 認証完了。"

# --- 設定 ---
SCHEDULER_JOB_NAME="tdnet-scraper-daily-trigger"
CF_SCRAPER_NAME="tdnet-scraper"
REGION="${TDNET_REGION:-"asia-northeast1"}"

# --- 元のURLを取得 ---
echo "🔄 スケジューラの現在の設定（復元用URL）を取得しています..."
CF_URL=$(gcloud functions describe ${CF_SCRAPER_NAME} --project "${PROJECT_ID}" --region "${REGION}" --gen2 --format="value(serviceConfig.uri)")
if [ -z "$CF_URL" ]; then
  echo "❌ エラー: Cloud FunctionのURLが取得できませんでした。"
  exit 1
fi
echo "   復元用URL: ${CF_URL}"

# --- 終了時に必ず元の設定に戻すためのtrap ---
function cleanup {
  echo ""
  echo "🧹 スケジューラの設定を元の状態に復元しています..."
  gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
    --project "${PROJECT_ID}" \
    --location "${REGION}" \
    --uri="${CF_URL}" \
    --http-method=POST \
    --description="Trigger TDnet Scraper daily at 19:00 JST" \
    --clear-message-body
  echo "✅ スケジューラの復元が完了しました。"
}
trap cleanup EXIT

# --- クエリパラメータで日付を渡す（Content-Type問題を回避） ---
NEW_URI="${CF_URL}?date=${EXEC_DATE}"
echo "🔧 スケジューラのURIを一時的に変更します..."
echo "   新URI: ${NEW_URI}"
gcloud scheduler jobs update http "${SCHEDULER_JOB_NAME}" \
  --project "${PROJECT_ID}" \
  --location "${REGION}" \
  --uri="${NEW_URI}" \
  --http-method=POST \
  --description="[MANUAL RUN] Trigger TDnet Scraper for ${EXEC_DATE}"

# --- スケジューラジョブの実行 ---
echo "🚀 変更した設定でスケジューラジョブを実行します..."
gcloud scheduler jobs run "${SCHEDULER_JOB_NAME}" --project "${PROJECT_ID}" --location "${REGION}"

echo "✅ ジョブの実行リクエストを送信しました。"
echo "ℹ️ 処理が完了すると、スケジューラの設定は自動で元に戻ります。" 