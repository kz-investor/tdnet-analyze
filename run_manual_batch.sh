#!/bin/bash
#
# 手動でデータ処理パイプラインを開始するためのヘルパースクリプト
# (Cloud Run ジョブを順番に実行)
#
set -e

# --- 設定 ---
SUMMARY_JOB_NAME="tdnet-summary-generator"
INSIGHT_JOB_NAME="tdnet-insight-generator"
REGION="${TDNET_REGION:-"asia-northeast1"}"

# --- 引数のパース ---
START_DATE=""
END_DATE=""

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
    *)
      echo "不明な引数: $1"
      exit 1
      ;;
  esac
done

if [ -z "$START_DATE" ] || [ -z "$END_DATE" ]; then
  echo "使用法: $0 --start-date YYYYMMDD --end-date YYYYMMDD"
  exit 1
fi

# --- gcloudコマンドの存在確認 ---
if ! command -v gcloud &> /dev/null
then
    echo "gcloud コマンドが見つかりません。Google Cloud SDKがインストールされ、PATHが通っているか確認してください。"
    exit 1
fi

# --- 実行引数の準備 ---
# 各スクリプトに渡す引数をここで組み立てる
SUMMARY_ARGS="--start-date=${START_DATE},--end-date=${END_DATE}"
INSIGHT_ARGS="--start-date=${START_DATE},--end-date=${END_DATE}"

# --- ステップ1: サマリー生成ジョブの実行 ---
echo "🚀 ステップ1: サマリー生成ジョブを開始します..."
echo "   ジョブ名: ${SUMMARY_JOB_NAME}"
echo "   期間: ${START_DATE} から ${END_DATE}"
gcloud run jobs execute "${SUMMARY_JOB_NAME}" \
  --args "${SUMMARY_ARGS}" \
  --region "${REGION}" \
  --wait # 処理の完了を待つ

echo "✅ ステップ1: サマリー生成ジョブが完了しました。"
echo ""


# --- ステップ2: インサイト生成ジョブの実行 ---
echo "🚀 ステップ2: インサイト生成ジョブを開始します..."
echo "   ジョブ名: ${INSIGHT_JOB_NAME}"
echo "   期間: ${START_DATE} から ${END_DATE}"
gcloud run jobs execute "${INSIGHT_JOB_NAME}" \
  --args "${INSIGHT_ARGS}" \
  --region "${REGION}" \
  --wait # 処理の完了を待つ

echo "✅ ステップ2: インサイト生成ジョブが完了しました。"
echo ""
echo "🎉 すべての処理が正常に完了しました。" 