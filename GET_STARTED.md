# Get Started (Zero-to-Run on Google Cloud)

この手順は「新規プロジェクト作成直後」から、本システム（Cloud Scheduler, Cloud Function, Cloud Run ジョブ）をGoogle Cloud上にデプロイし、実行するまでを網羅します。

## 0. 前提
- macOS/Linux で `gcloud` コマンドが使えること（Google Cloud SDK インストール済み）
- Google Cloud プロジェクトが作成済みであること
- 課金が有効になっていること
- ローカルに `git` がインストールされており、このリポジトリをクローン済みであること

## 1. Google Cloud プロジェクト設定
ローカルの `gcloud` コマンドラインツールに、対象のプロジェクトIDを設定します。

```bash
# YOUR_PROJECT_ID を実際のプロジェクトIDに置き換えてください
gcloud config set project YOUR_PROJECT_ID
```

## 2. APIの有効化
`deploy.sh`スクリプトが、デプロイプロセスの一部として必要なAPI（Cloud Functions, Cloud Run, Cloud Schedulerなど）を自動で有効化します。手動での操作は不要です。

## 3. GCSバケットの作成
成果物（PDF、サマリー、インサイト）を保存するためのGoogle Cloud Storage (GCS) バケットを作成します。

```bash
# BUCKET_NAMEはグローバルで一意な名前にする必要があります
# REGIONはasia-northeast1（東京）を推奨します
BUCKET_NAME="tdnet-analyzer-$(gcloud config get-value project)"
REGION="asia-northeast1"

gsutil mb -l $REGION gs://$BUCKET_NAME
```
> **Note**: `deploy.env` ファイルで `TDNET_BUCKET_NAME` を設定することで、スクリプトが使用するバケット名を指定できます。

## 4. サービスアカウントと権限設定
`deploy.sh`スクリプトが、デプロイ時に必要なサービスアカウントの作成と、IAM権限の付与を自動で行います。手動での操作は基本的に不要です。

スクリプトは `tdnet-analyzer-sa` という表示名のサービスアカウントを探し、存在しない場合は作成します。そして、そのサービスアカウントに必要なロール（Cloud Functions起動元、Cloud Run起動元、GCS書き込み権限など）を付与します。

## 5. デプロイ
リポジトリのルートディレクトリで `deploy.sh` を実行します。このスクリプトが、すべてのGoogle Cloudリソース（Cloud Function, Cloud Run ジョブ, Cloud Scheduler）のデプロイと設定を自動で行います。

**重要**: `deploy.sh` は、引数としてサービスアカウントキーのJSONファイルパスと、プロジェクトIDを必要とします。

```bash
# スクリプトに実行権限を付与（初回のみ）
chmod +x deploy.sh run_manual_batch.sh run_scraper_via_scheduler.sh

# デプロイスクリプトを実行。引数にキーファイルとプロジェクトIDを渡す。
./deploy.sh --key-file keys/<YOUR_SERVICE_ACCOUNT_KEY>.json --project-id <YOUR_PROJECT_ID>
```
これにより、以下のリソースが構築され、日次のPDF自動収集が有効になります。
- **Cloud Function**: `tdnet-scraper`
- **Cloud Run ジョブ**: `tdnet-summary-generator`, `tdnet-insight-generator`
- **Cloud Scheduler**: `tdnet-scraper-daily-trigger` (毎日19時 JST)

## 6. 動作確認

### 日次スクレイピングのテスト実行
デプロイされたCloud Schedulerジョブを手動で実行することで、日次のスクレイピング機能が正しく動作するかを確認できます。`run_scraper_via_scheduler.sh` は、このプロセスを安全に自動化するものです。

```bash
# 特定の日付を指定して安全に手動実行
./run_scraper_via_scheduler.sh --date 20240101 --key-file keys/<YOUR_SERVICE_ACCOUNT_KEY>.json --project-id <YOUR_PROJECT_ID>
```

### 分析バッチのテスト実行
`run_manual_batch.sh` を使って、指定した期間の分析バッチ処理を手動で実行します。このスクリプトも、引数としてサービスアカウントキーとプロジェクトIDを必要とします。

```bash
# 分析バッチを実行する例
./run_manual_batch.sh --start-date 20230101 --end-date 20230101 --key-file keys/<YOUR_SERVICE_ACCOUNT_KEY>.json --project-id <YOUR_PROJECT_ID>
```

### ログ確認
各処理のログはGoogle Cloud Loggingで確認できます。

```bash
# Cloud Function (スクレイパー) のログ
gcloud functions logs read tdnet-scraper --region=$REGION --gen2 --limit=50

# Cloud Run ジョブ (サマリー生成) のログ
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=tdnet-summary-generator" --limit=50

# Cloud Run ジョブ (インサイト生成) のログ
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=tdnet-insight-generator" --limit=50
```

## 7. ローカルでの検証（任意）
個別のスクリプトをローカル環境で実行することも可能です。

- **認証設定**:
  環境変数 `GOOGLE_APPLICATION_CREDENTIALS` に、サービスアカウントキーのJSONファイルへのパスを設定します。
  ```bash
  export GOOGLE_APPLICATION_CREDENTIALS="keys/<YOUR_SERVICE_ACCOUNT_KEY>.json"
  ```

- **依存インストール**:
  ```bash
  pip install -r requirements.txt
  ```

- **ローカル実行例**:
  ```bash
  # スクレイピング実行
  python tdnet_cloud.py --date 20230101

  # 個別サマリー生成
  python generate_summary.py --start-date 20230101 --end-date 20230101

  # セクターインサイト生成
  python generate_sector_insights.py --start-date 20230101 --end-date 20230101
  ```

## トラブルシュート（IAM）
- **デプロイ時の権限エラー**: `deploy.sh`を実行する際、`gcloud auth activate-service-account`で有効化したサービスアカウントに、プロジェクトに対する十分な権限（例: `roles/editor`）があることを確認してください。
- **実行時の権限エラー**: `tdnet-analyzer-sa`サービスアカウントに、必要なロール（`roles/storage.objectAdmin`, `roles/run.invoker`など）が付与されているか、GCPコンソールのIAMページで確認してください。`deploy.sh`がこれらの権限を自動で設定します。 