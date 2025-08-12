# Get Started (Zero-to-Run on Google Cloud)

この手順は「新規プロジェクト作成直後」から、本システム（Cloud Scheduler, Cloud Function, Cloud Run ジョブ）をGoogle Cloud上にデプロイし、実行するまでを網羅します。

## 0. 前提
- macOS/Linux で `gcloud` コマンドが使えること（Google Cloud SDK インストール済み）
- Google Cloud プロジェクトが作成済みであること
- 課金が有効になっていること
- ローカルに `git` がインストールされており、このリポジトリをクローン済みであること

## 1. Google Cloud プロジェクト設定

```bash
# YOUR_PROJECT_ID を実際のプロジェクトIDに置き換えてください
gcloud config set project YOUR_PROJECT_ID
```

## 2. APIの有効化
`deploy.sh` が必要なAPI（Cloud Functions, Cloud Run, Cloud Schedulerなど）を自動で有効化します。

## 3. GCSバケットの作成

```bash
BUCKET_NAME="tdnet-analyzer-$(gcloud config get-value project)"
REGION="asia-northeast1"

gsutil mb -l $REGION gs://$BUCKET_NAME
```
> `config/config.yaml` の `gcs.bucket_name` と一致させてください。

## 4. デプロイ

```bash
chmod +x deploy.sh run_manual_batch.sh run_scraper_via_scheduler.sh
./deploy.sh --key-file keys/<YOUR_SERVICE_ACCOUNT_KEY>.json --project-id <YOUR_PROJECT_ID>
```

- Cloud Function: `tdnet-scraper`
- Cloud Run Jobs: `tdnet-summary-generator`, `tdnet-insight-generator`
- Cloud Scheduler: `tdnet-scraper-daily-trigger`

## 5. 動作確認

### 日次スクレイピングの手動実行（安全）

```bash
./run_scraper_via_scheduler.sh --date 20240101 \
  --key-file keys/<YOUR_SERVICE_ACCOUNT_KEY>.json \
  --project-id <YOUR_PROJECT_ID>
```

### 分析バッチの手動実行

```bash
./run_manual_batch.sh \
  --start-date 20230101 --end-date 20230101 \
  --key-file keys/<YOUR_SERVICE_ACCOUNT_KEY>.json \
  --project-id <YOUR_PROJECT_ID>
```

### ログ確認

```bash
gcloud functions logs read tdnet-scraper --region=$REGION --gen2 --limit=50 | cat
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=tdnet-summary-generator" --limit=50 | cat
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=tdnet-insight-generator" --limit=50 | cat
```

## 6. ローカルでの検証（任意）

- 認証
```bash
export GOOGLE_APPLICATION_CREDENTIALS="keys/<YOUR_SERVICE_ACCOUNT_KEY>.json"
```

- 依存インストール
```bash
pip install -r requirements.txt
```

- 実行
```bash
# スクレイピング
python -m tdnet_analyzer.scraper.tdnet_cloud --date 20230101
# 個別サマリー生成
python -m tdnet_analyzer.batch.generate_summary --start-date 20230101 --end-date 20230101
# セクターインサイト生成
python -m tdnet_analyzer.batch.generate_sector_insights --start-date 20230101 --end-date 20230101
```

## 7. パスの扱い（重要）
- 本プロジェクトは、コードを `tdnet_analyzer/` パッケージ配下に統合し、ファイル参照は `tdnet_analyzer.common.path_utils.project_path()` を通じてプロジェクトルートから解決します。
- `config/`, `inputs/`, `prompt_templates/` の相対参照は環境差異で壊れやすいため、直接 `__file__` からの相対ではなく、このヘルパーを必ず使用してください。 