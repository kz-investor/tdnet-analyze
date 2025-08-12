#!/bin/bash

# 業種別一括スクレイピング実行スクリプト
# 使用例: ./run_batch_scraper.sh --start-date 20240804 --end-date 20240808 --key-file keys/your-service-account-key.json --project-id your-project-id

set -e

# スクリプトのディレクトリを取得
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# デフォルト値
CONFIG_FILE="config/config.yaml"
PYTHON_MODULE="tdnet_analyzer.batch.batch_scraper"

# ヘルプ表示関数
show_help() {
    echo "業種別一括スクレイピング実行スクリプト"
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
    echo "  --single-thread          シングルスレッドモードで実行"
    echo "  --config PATH            設定ファイルパス (デフォルト: $CONFIG_FILE)"
    echo "  --help                   このヘルプを表示"
    echo ""
    echo "例:"
    echo "  $0 --start-date 20240804 --end-date 20240808 \\"
    echo "     --key-file keys/your-service-account-key.json \\"
    echo "     --project-id your-project-id"
}

# 引数解析
START_DATE=""
END_DATE=""
KEY_FILE=""
PROJECT_ID=""
SINGLE_THREAD=""
CUSTOM_CONFIG=""

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
        --single-thread)
            SINGLE_THREAD="--single-thread"
            shift
            ;;
        --config)
            CUSTOM_CONFIG="$2"
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
    echo "GCPプロジェクトID設定: $PROJECT_ID"
fi

# 設定ファイル
if [[ -n "$CUSTOM_CONFIG" ]]; then
    CONFIG_FILE="$CUSTOM_CONFIG"
fi

if [[ ! -f "$CONFIG_FILE" ]]; then
    echo "エラー: 設定ファイルが見つかりません: $CONFIG_FILE"
    exit 1
fi

# 実行情報表示
echo "=========================================="
echo "業種別一括スクレイピング実行"
echo "=========================================="
echo "開始日付: $START_DATE"
echo "終了日付: $END_DATE"
echo "設定ファイル: $CONFIG_FILE"
echo "モード: $([ -n "$SINGLE_THREAD" ] && echo "シングルスレッド" || echo "マルチスレッド")"
echo "=========================================="

# Python実行
echo "実行開始..."
python -m "$PYTHON_MODULE" \
    --start-date "$START_DATE" \
    --end-date "$END_DATE" \
    --config "$CONFIG_FILE" \
    $SINGLE_THREAD

exit_code=$?

if [[ $exit_code -eq 0 ]]; then
    echo "=========================================="
    echo "業種別一括スクレイピング完了"
    echo "=========================================="
else
    echo "=========================================="
    echo "業種別一括スクレイピング失敗 (終了コード: $exit_code)"
    echo "=========================================="
fi

exit $exit_code