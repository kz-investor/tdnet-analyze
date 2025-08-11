# keys ディレクトリについて

- ここにはローカル実行用のサービスアカウント鍵（JSON）を配置します。
- 鍵ファイルはリポジトリにコミットしないでください（`.gitignore` に `keys/*.json` を設定済み）。

## 配置手順（ローカル）

1. サービスアカウント鍵（JSON）を用意し、`keys/` に配置します。
   - 例: `keys/<YOUR_SA_KEY>.json`
2. 環境変数を設定します。
   ```bash
   export GOOGLE_APPLICATION_CREDENTIALS="keys/<YOUR_SA_KEY>.json"
   ```
3. 実行例:
   ```bash
   pip install -r requirements.txt
   python tdnet_cloud.py --date 20250807 --config config/config.yaml
   ```

## 備考
- Cloud Functions 本番実行には鍵は不要です（デプロイ先のサービスアカウントで実行されます）。
- 今後のクリーンアップでは `keys/` 配下は削除対象にしません。 