#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ローカル版セクターインサイト生成機能
既存クラウド版と同じ仕様でローカルファイルシステムから処理
"""

import os
import argparse
import datetime
from pathlib import Path
from collections import defaultdict
import yaml

import vertexai
from vertexai.generative_models import GenerativeModel
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from google.api_core import exceptions

from tdnet_analyzer.common.path_utils import project_path

import logging

# ロギング設定
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 設定読み込み
CONFIG = {}
try:
    with open(project_path('config', 'config.yaml'), "r") as f:
        CONFIG = yaml.safe_load(f)
except FileNotFoundError:
    logger.warning("config/config.yamlが見つかりません。")
except Exception as e:
    logger.error(f"config.yamlの読み込みエラー: {e}")

# 定数
DEFAULT_LOCATION = "us-central1"
DEFAULT_MODEL = CONFIG.get('llm', {}).get('model_name', 'gemini-1.0-pro')
DEFAULT_MAX_WORKERS = CONFIG.get('llm', {}).get('parallel', {}).get('max_workers', 10)

# プロンプト読み込み
PROMPT_DIR = project_path('prompt_templates')
try:
    with open(PROMPT_DIR / 'sector_system_prompt.md', 'r', encoding='utf-8') as f:
        SECTOR_SYSTEM_PROMPT_TEMPLATE = f.read()
    with open(PROMPT_DIR / 'sector_user_prompt.md', 'r', encoding='utf-8') as f:
        SECTOR_USER_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"プロンプトファイルが見つかりません: {e}")
    exit(1)


def summarize_text_with_vertex(project: str, location: str, model_name: str, system_prompt: str, user_prompt: str) -> str:
    """Vertex AI でテキスト要約"""
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
    """ファイル名安全化"""
    s = "".join(c for c in name if c.isalnum() or c in (" ", "-", "_"))
    return s.strip().replace(" ", "_")[:max_len]


def get_date_range(start_date_str: str, end_date_str: str) -> list[str]:
    """日付範囲生成"""
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d")
    delta = end_date - start_date
    return [(start_date + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(delta.days + 1)]


def generate_local_sector_insights(dates: list[str], out_root: str, project: str, location: str, model_name: str):
    """ローカル版セクターインサイト生成"""
    output_date_str = dates[-1] if dates else datetime.datetime.now().strftime("%Y%m%d")
    logger.info(f"ローカル版セクターインサイト生成開始: 期間={dates[0] if dates else 'N/A'}-{dates[-1] if dates else 'N/A'}")
    
    out_path = Path(out_root)
    sector_docs = defaultdict(list)
    
    # 各日付のサマリーファイルを読み込み
    for date_str in dates:
        summary_dir = out_path / "insights-summaries" / date_str
        if not summary_dir.exists():
            logger.warning(f"サマリーディレクトリが見つかりません: {summary_dir}")
            continue
        
        # サマリーファイルを読み込み
        for summary_file in summary_dir.glob("*_summary.md"):
            try:
                filename = summary_file.name
                base_name, ext = os.path.splitext(filename)
                if ext == '.md' and base_name.endswith('_summary'):
                    core_name = base_name[:-len('_summary')]
                    parts = core_name.split('__')
                    
                    if len(parts) == 5:
                        # 通常モード: 日付__業種__規模__コード__会社名_summary.md
                        sector = parts[1]
                        size_classification = parts[2]
                        sector_size_key = f"{sector}_{size_classification}"
                        
                        with open(summary_file, 'r', encoding='utf-8') as f:
                            content = f.read()
                        sector_docs[sector_size_key].append(content)
                    else:
                        logger.warning(f"ファイル名の形式が不正なため業種・規模を抽出できません: {filename} (parts: {len(parts)})")
            except Exception as e:
                logger.error(f"サマリーファイル読み込みエラー: {summary_file}, エラー: {e}")
    
    if not sector_docs:
        logger.error("サマリーファイルから業種を1件も抽出できませんでした。処理を中断します。")
        return
    
    logger.info(f"{len(sector_docs)} 個のセクター・規模組み合わせを処理します")
    
    # インサイト出力ディレクトリ作成
    insights_dir = out_path / "insights-sectors" / output_date_str
    insights_dir.mkdir(parents=True, exist_ok=True)
    
    # セクターインサイト生成
    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        future_to_sector = {}
        for sector_size_key, docs in sector_docs.items():
            system_prompt = SECTOR_SYSTEM_PROMPT_TEMPLATE.replace("{{sector_name}}", sector_size_key).replace("{{count}}", str(len(docs)))
            user_prompt = SECTOR_USER_PROMPT_TEMPLATE.replace("{{summaries}}", "\n\n".join(docs))
            
            future = executor.submit(
                summarize_text_with_vertex,
                project, location, model_name,
                system_prompt, user_prompt
            )
            future_to_sector[future] = sector_size_key
        
        for future in as_completed(future_to_sector):
            sector_size_key = future_to_sector[future]
            try:
                insight_text = future.result()
                
                # ローカルファイル保存（既存クラウド版と同じ形式）
                filename = f"{safe_name(sector_size_key)}_insights.md"
                insight_path = insights_dir / filename
                
                with open(insight_path, 'w', encoding='utf-8') as f:
                    f.write(insight_text)
                
                logger.info(f"セクター+規模インサイトをローカルに保存: {insight_path}")
                
            except Exception as e:
                logger.error(f"セクターインサイトの生成または保存に失敗: {sector_size_key}, エラー: {e}")
    
    logger.info("ローカル版セクターインサイト生成完了")


def main():
    parser = argparse.ArgumentParser(description="ローカル版セクターインサイト生成")
    parser.add_argument('--start-date', required=True, help='開始日付 (YYYYMMDD形式)')
    parser.add_argument('--end-date', required=True, help='終了日付 (YYYYMMDD形式)')
    parser.add_argument('--out-root', required=True, help='ローカル保存先ディレクトリ')
    parser.add_argument('--project', required=True, help='Google Cloud Project ID')
    parser.add_argument('--location', default=DEFAULT_LOCATION, help='Vertex AI location')
    parser.add_argument('--model', default=DEFAULT_MODEL, help=f'Vertex AI model name (default: {DEFAULT_MODEL})')
    
    args = parser.parse_args()
    
    dates_to_process = get_date_range(args.start_date, args.end_date)
    
    try:
        generate_local_sector_insights(
            dates=dates_to_process,
            out_root=args.out_root,
            project=args.project,
            location=args.location,
            model_name=args.model
        )
        
        print("ローカル版セクターインサイト生成完了")
        
    except Exception as e:
        logger.error(f"ローカル版セクターインサイト生成異常終了: {e}")
        exit(1)


if __name__ == "__main__":
    main()