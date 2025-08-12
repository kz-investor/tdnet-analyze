#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import os
import sys
import datetime
from collections import defaultdict
import yaml

from google.cloud import storage
import vertexai
from vertexai.generative_models import GenerativeModel

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from google.api_core import exceptions

from tdnet_analyzer.common.path_utils import project_path

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- グローバル設定 ---
CONFIG = {}
try:
    with open(project_path('config', 'config.yaml'), "r") as f:
        CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    logger.warning("config/config.yamlが見つかりません。")
except Exception as e:
    logger.error(f"config.yamlの読み込みエラー: {e}")

# 定数
DEFAULT_BUCKET = os.environ.get('TDNET_BUCKET', CONFIG.get('gcs', {}).get('bucket_name', 'tdnet-documents'))
DEFAULT_BASE = os.environ.get('TDNET_BASE', CONFIG.get('gcs', {}).get('base_path', 'tdnet-analyzer'))
DEFAULT_LOCATION = "us-central1"
DEFAULT_MODEL = CONFIG.get('llm', {}).get('model_name', 'gemini-1.0-pro')
DEFAULT_MAX_WORKERS = CONFIG.get('llm', {}).get('parallel', {}).get('max_workers', 10)

# プロンプトの読み込み
PROMPT_DIR = project_path('prompt_templates')
try:
    with open(PROMPT_DIR / 'sector_system_prompt.md', 'r', encoding='utf-8') as f:
        SECTOR_SYSTEM_PROMPT_TEMPLATE = f.read()
    with open(PROMPT_DIR / 'sector_user_prompt.md', 'r', encoding='utf-8') as f:
        SECTOR_USER_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"プロンプトファイルが見つかりません: {e}")
    sys.exit(1)


def summarize_text_with_vertex(project: str, location: str, model_name: str, system_prompt: str, user_prompt: str) -> str:
    max_retries = 5
    initial_backoff = 2
    for attempt in range(max_retries):
        try:
            vertexai.init(project=project, location=location)
            model = GenerativeModel(model_name, system_instruction=system_prompt)
            response = model.generate_content(user_prompt)
            return response.text
        except exceptions.ResourceExhausted as e:
            if attempt < max_retries - 1:
                wait_time = initial_backoff * (2 ** attempt) + random.uniform(0, 1)
                logger.warning(f"Vertex AI APIでリソース枯渇エラー(429)が発生しました。{wait_time:.2f}秒待機して再試行します。(試行 {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                logger.error(f"Vertex AI APIの呼び出しが最大再試行回数({max_retries}回)に達しました。エラー: {e}")
                raise
        except Exception as e:
            logger.error(f"Vertex AI API呼び出し中に予期せぬエラー: {e}")
            raise


def safe_name(name: str, max_len: int = 50) -> str:
    s = "".join(c for c in name if c.isalnum() or c in (" ", "-", "_"))
    return s.strip().replace(" ", "_")[:max_len]


def get_date_range(start_date_str: str, end_date_str: str) -> list[str]:
    import datetime as _dt
    start_date = _dt.datetime.strptime(start_date_str, "%Y%m%d")
    end_date = _dt.datetime.strptime(end_date_str, "%Y%m%d")
    delta = end_date - start_date
    return [(start_date + _dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(delta.days + 1)]


def generate_sector_insights(dates: list[str], bucket: str, base: str, project: str, location: str, model_name: str, sector_mode: bool = False):
    output_date_str = dates[-1] if dates else datetime.datetime.now().strftime("%Y%m%d")
    logger.info(f"業種インサイト生成開始: 期間={dates[0] if dates else 'N/A'}-{dates[-1] if dates else 'N/A'}, バケット={bucket}, ベースパス={base}")

    client = storage.Client()
    bucket_obj = client.bucket(bucket)

    all_blobs = []
    if sector_mode:
        # 業種別モード：最新日付のみを参照
        latest_date = dates[-1] if dates else datetime.datetime.now().strftime("%Y%m%d")
        path_parts = [base, "insights-summaries", latest_date]
        prefix = "/".join(part for part in path_parts if part) + "/"
        blobs = list(client.list_blobs(bucket_obj, prefix=prefix))
        if not blobs:
            logger.warning(f"GCSに処理対象の個別サマリーファイルが見つかりません: gs://{bucket}/{prefix}")
        all_blobs.extend(blobs)
    else:
        # 通常モード：全日付を参照
        for date_str in dates:
            path_parts = [base, "insights-summaries", date_str]
            prefix = "/".join(part for part in path_parts if part) + "/"
            blobs = list(client.list_blobs(bucket_obj, prefix=prefix))
            if not blobs:
                logger.warning(f"GCSに処理対象の個別サマリーファイルが見つかりません: gs://{bucket}/{prefix}")
            all_blobs.extend(blobs)

    if not all_blobs:
        logger.warning(f"指定された期間に処理対象の個別サマリーファイルが1件も見つかりませんでした。")
        return

    logger.info(f"{len(all_blobs)}件の個別サマリーをGCSから読み込みます。")
    sector_docs = defaultdict(list)
    for blob in all_blobs:
        try:
            filename = blob.name.split('/')[-1]
            base_name, ext = os.path.splitext(filename)
            if ext == '.md' and base_name.endswith('_summary'):
                core_name = base_name[:-len('_summary')]
                parts = core_name.split('__')
                if sector_mode and len(parts) == 5:
                    # 業種別モード：業種__規模__コード__会社名__日付_summary.md
                    sector = parts[0]
                    size_classification = parts[1]
                    sector_size_key = f"{sector}_{size_classification}"
                    content = blob.download_as_text()
                    sector_docs[sector_size_key].append(content)
                elif not sector_mode and len(parts) == 5:
                    # 通常モード：日付__業種__規模__コード__会社名_summary.md
                    sector = parts[1]
                    size_classification = parts[2]
                    sector_size_key = f"{sector}_{size_classification}"
                    content = blob.download_as_text()
                    sector_docs[sector_size_key].append(content)
                else:
                    logger.warning(f"ファイル名の形式が不正なため業種・規模を抽出できません: {filename} (parts: {len(parts)})")
            else:
                logger.warning(f"予期しないファイル形式です: {filename}")
        except Exception as e:
            logger.error(f"GCSからのファイル読み込みエラー: {blob.name}, エラー: {e}")

    if not sector_docs:
        logger.error("サマリーファイルから業種を1件も抽出できませんでした。処理を中断します。")
        return

    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        future_to_sector = {}
        for sector_size_key, docs in sector_docs.items():
            system_prompt = SECTOR_SYSTEM_PROMPT_TEMPLATE.replace("{{sector_name}}", sector_size_key).replace("{{count}}", str(len(docs)))
            user_prompt = SECTOR_USER_PROMPT_TEMPLATE.replace("{{summaries}}", "\n\n".join(docs))
            future = executor.submit(
                summarize_text_with_vertex,
                project, location, model_name,
                system_prompt,
                user_prompt
            )
            future_to_sector[future] = sector_size_key
        for future in as_completed(future_to_sector):
            sector_size_key = future_to_sector[future]
            try:
                insight_text = future.result()
                filename = f"{safe_name(sector_size_key)}_insights.md"
                path_parts = [base, "insights-sectors", output_date_str, filename]
                insight_gcs_path = "/".join(part for part in path_parts if part)
                blob = client.bucket(bucket).blob(insight_gcs_path)
                blob.upload_from_string(insight_text.encode('utf-8'), content_type="text/markdown")
                logger.info(f"セクター+規模インサイトをGCSにアップロードしました: gs://{bucket}/{insight_gcs_path}")
            except Exception as e:
                logger.error(f"セクターインサイトの生成またはアップロードに失敗: {sector_size_key}, エラー: {e}")

    logger.info("generate_sector_insights.py スクリプト完了")


def main():
    logger.info("generate_sector_insights.py スクリプト開始")
    parser = argparse.ArgumentParser(description="Generate sector insights from pre-computed summaries.")
    parser.add_argument('--start-date', required=True, help='Target start date in YYYYMMDD format.')
    parser.add_argument('--end-date', required=True, help='Target end date in YYYYMMDD format.')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET, help='GCS bucket name.')
    parser.add_argument('--base', default=DEFAULT_BASE, help='GCS base path.')
    parser.add_argument('--project', default=os.environ.get('GOOGLE_CLOUD_PROJECT'), help='Google Cloud Project ID.')
    parser.add_argument('--location', default=DEFAULT_LOCATION, help='Vertex AI location.')
    parser.add_argument('--model', default=DEFAULT_MODEL, help=f'Vertex AI model name (default: {DEFAULT_MODEL})')
    parser.add_argument('--sector-mode', action='store_true', help='Process sector-mode summaries (different filename format).')
    args = parser.parse_args()
    if not args.project:
        logger.error("Google Cloud Project IDが指定されていません。--project オプションで指定するか、環境変数 GOOGLE_CLOUD_PROJECT を設定してください。")
        sys.exit(1)
    dates_to_process = get_date_range(args.start_date, args.end_date)
    try:
        generate_sector_insights(
            dates=dates_to_process,
            bucket=args.bucket,
            base=args.base,
            project=args.project,
            location=args.location,
            model_name=args.model,
            sector_mode=args.sector_mode,
        )
        logger.info("generate_sector_insights.py スクリプト正常終了")
    except Exception as e:
        logger.error(f"generate_sector_insights.py スクリプト異常終了: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
