# Get Started (Zero-to-Run on Google Cloud)

この手順は「新規プロジェクト作成直後」から、本システム（Cloud Scheduler → Cloud Functions → GCS）を動かすまでを網羅します。

## 0. 前提
- macOS/Linux で `gcloud`/`gsutil` が使えること（Google Cloud SDK インストール済み）
- プロジェクトID: `YOUR_PROJECT_ID`
- リージョン: `asia-northeast1`（変更可）

```bash
PROJECT_ID=YOUR_PROJECT_ID
REGION=asia-northeast1
BUCKET_NAME=tdnet-documents   # 変更可（グローバル一意）
```

## 1) API 有効化
```bash
gcloud config set project $PROJECT_ID
gcloud services enable \
  cloudfunctions.googleapis.com \
  run.googleapis.com \
  cloudscheduler.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  logging.googleapis.com \
  storage.googleapis.com
```

## 2) GCS バケット作成（成果物格納）
```bash
gsutil mb -l $REGION gs://$BUCKET_NAME
```

## 3) IAM 権限（実行サービスアカウント → GCS 書き込み）
Cloud Functions Gen2 の実体は Cloud Run で動作し、デフォルトでは「Compute Default Service Account」
`$PROJECT_NUMBER-compute@developer.gserviceaccount.com` が実行に使われます。

- プロジェクト番号取得
```bash
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
echo $PROJECT_NUMBER
```

- バケットへの書き込み権限付与（最小権限）
```bash
gsutil iam ch \
  serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectCreator \
  gs://$BUCKET_NAME
```

（必要に応じて読み取りも）
```bash
gsutil iam ch \
  serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com:roles/storage.objectViewer \
  gs://$BUCKET_NAME
```

補足:
- 関数URLを「認証不要（--allow-unauthenticated）」で公開するため、Scheduler側の追加権限は不要です。
- 認証必須にする場合は、Scheduler実行SAへ Cloud Functions Invoker を付与してください。

## 4) ロギング保持期間（72時間=3日）
```bash
gcloud logging buckets update _Default --location=global --retention-days=3 || true
```

## 5) デプロイ（本リポジトリ直下で実行）

**重要**: Cloud Functionsは`internal-only` + 認証必須でデプロイされます（外部アクセス不可）

```bash
./deploy.sh
```
- 反映される設定
  - Functions: timeout=1500s, memory=1024MB, max-instances=1, HTTP
  - **セキュリティ**: `internal-only` + 認証必須（外部アクセス不可）
  - Scheduler: JST 19:00 に毎日実行
  - Logging: 保持3日

## 6) 動作確認

**注意**: Cloud Functionsは`internal-only`設定のため、以下のHTTPアクセスは失敗します。

- 当日（JST）実行（参考）
```bash
FUNCTION_URL=$(gcloud functions describe tdnet-scraper --region=$REGION --gen2 --format='value(serviceConfig.uri)')
curl -X POST "$FUNCTION_URL/"  # 404エラー（正常）
```

**実際の動作確認**:
```bash
# Cloud Scheduler経由で実行
gcloud scheduler jobs run tdnet-scrape-daily --location=asia-northeast1
```
- ログ確認
```bash
gcloud functions logs read tdnet-scraper --region=$REGION --gen2 --limit=100
```

## 7) ローカルでの検証（任意）
- 認証（サービスアカウント鍵を用意）
```bash
export GOOGLE_APPLICATION_CREDENTIALS="keys/<YOUR_SERVICE_ACCOUNT_KEY>.json"
```
- 依存インストール
```bash
pip install -r requirements.txt
```
- ローカル実行（日付必須）
```bash
# スクレイピング実行
python tdnet_cloud.py --date 20250807

# GCS→ローカル ダウンロード
python gcs_download.py --date 20250807 --out downloads

# LLM処理（個別サマリー）
python generate_summary.py --date 20250807 --project <YOUR_PROJECT_ID>

# LLM処理（セクターインサイト）
python generate_sector_insights.py --date 20250807 --project <YOUR_PROJECT_ID>
```

## トラブルシュート（IAM）
- アップロード 403: 実行SA（`${PROJECT_NUMBER}-compute@developer.gserviceaccount.com`）に
  `roles/storage.objectCreator` が付与されているか確認
- Function起動不可（認証関連）: 今回は `--allow-unauthenticated`。制限したい場合は `Cloud Functions Invoker` を適切なSAへ付与

## 補足（セキュリティ）
- ローカル鍵はリポジトリにコミットしない（`.gitignore` で除外済み）
- 本番は鍵不要（ADCで自動認証） 