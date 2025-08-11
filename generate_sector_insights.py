#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import re
import argparse
import os
import sys
from collections import defaultdict
import yaml

from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- グローバル設定 ---
CONFIG = {}
try:
    with open("config/config.yaml", "r") as f:
        CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    logger.warning("config/config.yamlが見つかりません。")
except Exception as e:
    logger.error(f"config.yamlの読み込みエラー: {e}")

# 定数
DEFAULT_BUCKET = os.environ.get('TDNET_BUCKET', CONFIG.get('gcs', {}).get('bucket_name', 'tdnet-documents'))
DEFAULT_BASE = os.environ.get('TDNET_BASE', CONFIG.get('gcs', {}).get('base_path', 'vertex-ai-rag'))
DEFAULT_LOCATION = "us-central1"
DEFAULT_MODEL = CONFIG.get('llm', {}).get('model_name', 'gemini-2.5-flash-lite')
DEFAULT_MAX_WORKERS = CONFIG.get('llm', {}).get('parallel', {}).get('max_workers', 10)

# プロンプトの読み込み
PROMPT_DIR = os.path.join(os.path.dirname(__file__), 'prompt_templates')
try:
    with open(os.path.join(PROMPT_DIR, 'sector_system_prompt.md'), 'r', encoding='utf-8') as f:
        SECTOR_SYSTEM_PROMPT_TEMPLATE = f.read()
    with open(os.path.join(PROMPT_DIR, 'sector_user_prompt.md'), 'r', encoding='utf-8') as f:
        SECTOR_USER_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"プロンプトファイルが見つかりません: {e}")
    sys.exit(1)


def summarize_text_with_vertex(project: str, location: str, model_name: str, system_prompt: str, user_prompt: str) -> str:
    """システムプロンプトとユーザープロンプトを分けてLLMにリクエストする"""
    try:
        vertexai.init(project=project, location=location)
        model = GenerativeModel(model_name, system_instruction=system_prompt)
        response = model.generate_content(user_prompt)
        return response.text
    except Exception as e:
        logger.error(f"Vertex AI API呼び出し中にエラー: {e}")
        raise


def safe_name(name: str, max_len: int = 50) -> str:
    s = "".join(c for c in name if c.isalnum() or c in (" ", "-", "_"))
    return s.strip().replace(" ", "_")[:max_len]


def generate_sector_insights_for_date(date_str: str, bucket: str, base: str, project: str, location: str, model_name: str):
    logger.info(f"業種インサイト生成開始: 日付={date_str}, バケット={bucket}, ベースパス={base}")

    client = storage.Client()
    bucket_obj = client.bucket(bucket)
    prefix = f"{base}/insights-summaries/{date_str}/"
    blobs = list(client.list_blobs(bucket_obj, prefix=prefix))
    if not blobs:
        logger.warning(f"GCSに処理対象の個別サマリーファイルが見つかりません: gs://{bucket}/{prefix}")
        return

    logger.info(f"{len(blobs)}件の個別サマリーをGCSから読み込みます。")
    sector_docs = defaultdict(list)
    for blob in blobs:
        try:
            # ファイル名から業種+規模区分を抽出 (例: 20250807_サービス業_Small_2_60620_summary.md)
            parts = blob.name.split('/')[-1].split('_')
            if len(parts) >= 5:  # 新形式: date_sector_size1_size2_code_summary.md
                sector = parts[1]
                size = f"{parts[2]}_{parts[3]}"  # Small_2, Core30 など
                sector_size_key = f"{sector}_{size}"
                content = blob.download_as_text()
                sector_docs[sector_size_key].append(content)
            elif len(parts) >= 3:  # 旧形式: date_sector_code_summary.md (後方互換性)
                sector = parts[1]
                content = blob.download_as_text()
                sector_docs[sector].append(content)
            else:
                logger.warning(f"ファイル名の形式が不正なため業種を抽出できません: {blob.name}")
        except Exception as e:
            logger.error(f"GCSからのファイル読み込みエラー: {blob.name}, エラー: {e}")

    if not sector_docs:
        logger.error("サマリーファイルから業種を1件も抽出できませんでした。処理を中断します。")
        return

    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        future_to_sector = {
            executor.submit(
                summarize_text_with_vertex,
                project, location, model_name,
                SECTOR_SYSTEM_PROMPT_TEMPLATE,
                SECTOR_USER_PROMPT_TEMPLATE.replace("{{sector_name}}", sector_size_key).replace(
                    "{{count}}", str(len(docs))).replace("{{summaries}}", "\n\n".join(docs))
            ): sector_size_key
            for sector_size_key, docs in sector_docs.items()
        }

        for future in as_completed(future_to_sector):
            sector_size_key = future_to_sector[future]
            try:
                insight_text = future.result()
                insight_gcs_path = f"{base}/insights-sectors/{date_str}/{safe_name(sector_size_key)}_insights.md"
                blob = client.bucket(bucket).blob(insight_gcs_path)
                blob.upload_from_string(insight_text.encode('utf-8'), content_type="text/markdown")
                logger.info(f"セクター+規模インサイトをGCSにアップロードしました: gs://{bucket}/{insight_gcs_path}")
            except Exception as e:
                logger.error(f"セクターインサイトの生成またはアップロードに失敗: {sector_size_key}, エラー: {e}")

    logger.info("generate_sector_insights.py スクリプト完了")


def main():
    logger.info("generate_sector_insights.py スクリプト開始")
    parser = argparse.ArgumentParser(description="Generate sector insights from pre-computed summaries.")
    parser.add_argument('--date', required=True, help='Target date in YYYYMMDD format.')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET, help='GCS bucket name.')
    parser.add_argument('--base', default=DEFAULT_BASE, help='GCS base path.')
    parser.add_argument('--project', default=os.environ.get('GOOGLE_CLOUD_PROJECT'), help='Google Cloud Project ID.')
    parser.add_argument('--location', default=DEFAULT_LOCATION, help='Vertex AI location.')
    parser.add_argument('--model', default=DEFAULT_MODEL, help=f'Vertex AI model name (default: {DEFAULT_MODEL})')

    args = parser.parse_args()

    if not args.project:
        logger.error("Google Cloud Project IDが指定されていません。--project オプションで指定するか、環境変数 GOOGLE_CLOUD_PROJECT を設定してください。")
        sys.exit(1)

    try:
        generate_sector_insights_for_date(
            date_str=args.date,
            bucket=args.bucket,
            base=args.base,
            project=args.project,
            location=args.location,
            model_name=args.model,
        )
        logger.info("generate_sector_insights.py スクリプト正常終了")
    except Exception as e:
        logger.error(f"generate_sector_insights.py スクリプト異常終了: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
