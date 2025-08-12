#!/bin/bash

# 業種別時系列分析実行スクリプト
# 使用例: ./run_sector_timeseries_analysis.sh --start-date 20240601 --end-date 20240831 --key-file keys/your-service-account-key.json --project-id your-project-id

set -e

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# デフォルト値
CONFIG_FILE="config/config.yaml"
PYTHON_MODULE="tdnet_analyzer.batch.sector_timeseries_analysis"

# ヘルプ表示関数
show_help() {
    echo "業種別時系列分析実行スクリプト"
    echo ""
    echo "使用方法:"
    echo "  $0 --start-date YYYYMMDD --end-date YYYYMMDD [オプション]"
    echo ""
    echo "必須引数:"
    echo "  --start-date YYYYMMDD    開始日付"
    echo "  --end-date YYYYMMDD      終了日付"
    echo ""
    echo "オプション:"
    echo "  --key-file PATH          GCPサービスアカウントキーファイルパス"
    echo "  --project-id ID          GCPプロジェクトID"
    echo "  --bucket NAME            GCSバケット名"
    echo "  --base PATH              GCSベースパス"
    echo "  --location LOCATION      Vertex AI ロケーション (デフォルト: us-central1)"
    echo "  --model MODEL            Vertex AI モデル名"
    echo "  --help                   このヘルプを表示"
    echo ""
    echo "例:"
    echo "  $0 --start-date 20240601 --end-date 20240831 \\"
    echo "     --key-file keys/your-service-account-key.json \\"
    echo "     --project-id your-project-id"
}

# 引数解析
START_DATE=""
END_DATE=""
KEY_FILE=""
PROJECT_ID=""
BUCKET=""
BASE=""
LOCATION=""
MODEL=""

while [[ $# -gt 0 ]]; do
    case $1 in
        --start-date)
            START_DATE="$2"
            shift 2
            ;;
        --end-date)
            END_DATE="$2"
            shift 2
            ;;
        --key-file)
            KEY_FILE="$2"
            shift 2
            ;;
        --project-id)
            PROJECT_ID="$2"
            shift 2
            ;;
        --bucket)
            BUCKET="$2"
            shift 2
            ;;
        --base)
            BASE="$2"
            shift 2
            ;;
        --location)
            LOCATION="$2"
            shift 2
            ;;
        --model)
            MODEL="$2"
            shift 2
            ;;
        --help)
            show_help
            exit 0
            ;;
        *)
            echo "エラー: 不明なオプション $1"
            show_help
            exit 1
            ;;
    esac
done

# 必須引数チェック
if [[ -z "$START_DATE" ]]; then
    echo "エラー: --start-date が指定されていません"
    show_help
    exit 1
fi

if [[ -z "$END_DATE" ]]; then
    echo "エラー: --end-date が指定されていません"
    show_help
    exit 1
fi

# 日付形式チェック
if ! [[ "$START_DATE" =~ ^[0-9]{8}$ ]]; then
    echo "エラー: --start-date は YYYYMMDD 形式で指定してください"
    exit 1
fi

if ! [[ "$END_DATE" =~ ^[0-9]{8}$ ]]; then
    echo "エラー: --end-date は YYYYMMDD 形式で指定してください"
    exit 1
fi

# キーファイル設定
if [[ -n "$KEY_FILE" ]]; then
    if [[ ! -f "$KEY_FILE" ]]; then
        echo "エラー: キーファイルが見つかりません: $KEY_FILE"
        exit 1
    fi
    export GOOGLE_APPLICATION_CREDENTIALS="$KEY_FILE"
    echo "GCP認証キーファイル設定: $KEY_FILE"
fi

# プロジェクトID設定
if [[ -n "$PROJECT_ID" ]]; then
    export PROJECT_ID="$PROJECT_ID"
    export GOOGLE_CLOUD_PROJECT="$PROJECT_ID"
    echo "GCPプロジェクトID設定: $PROJECT_ID"
fi

# 実行情報表示
echo "=========================================="
echo "業種別時系列分析実行"
echo "=========================================="
echo "開始日付: $START_DATE"
echo "終了日付: $END_DATE"
[[ -n "$BUCKET" ]] && echo "GCSバケット: $BUCKET"
[[ -n "$BASE" ]] && echo "GCSベースパス: $BASE"
[[ -n "$LOCATION" ]] && echo "Vertex AIロケーション: $LOCATION"
[[ -n "$MODEL" ]] && echo "Vertex AIモデル: $MODEL"
echo "=========================================="

# Python実行コマンド構築
PYTHON_CMD="python -m $PYTHON_MODULE --start-date $START_DATE --end-date $END_DATE"

[[ -n "$PROJECT_ID" ]] && PYTHON_CMD="$PYTHON_CMD --project $PROJECT_ID"
[[ -n "$BUCKET" ]] && PYTHON_CMD="$PYTHON_CMD --bucket $BUCKET"
[[ -n "$BASE" ]] && PYTHON_CMD="$PYTHON_CMD --base $BASE"
[[ -n "$LOCATION" ]] && PYTHON_CMD="$PYTHON_CMD --location $LOCATION"
[[ -n "$MODEL" ]] && PYTHON_CMD="$PYTHON_CMD --model $MODEL"

# 実行
echo "実行開始..."
echo "実行コマンド: $PYTHON_CMD"
eval $PYTHON_CMD

exit_code=$?

if [[ $exit_code -eq 0 ]]; then
    echo "=========================================="
    echo "業種別時系列分析完了"
    echo "=========================================="
else
    echo "=========================================="
    echo "業種別時系列分析失敗 (終了コード: $exit_code)"
    echo "=========================================="
fi

exit $exit_code