# Python 3.11をベースイメージとして使用
FROM python:3.11-slim

# 作業ディレクトリを設定
WORKDIR /app

# Cloud Function用の依存関係ファイルをコピー
COPY requirements-functions.txt requirements.txt

# 依存関係をインストール
# --no-cache-dir オプションでキャッシュを無効化し、イメージサイズを削減
RUN pip install --no-cache-dir -r requirements.txt

# アプリケーションコードをコピー
COPY . .

# （注意）
# このDockerfileはCloud Runジョブで直接スクリプトを実行するために使用されます。
# CMDやENTRYPOINTは、gcloud run jobs deployコマンドの--commandおよび--argsで
# オーバーライドされるため、ここでは設定しません。 
ENTRYPOINT ["python3"] 