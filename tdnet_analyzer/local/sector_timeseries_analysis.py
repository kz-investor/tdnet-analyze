#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ローカル版業種別時系列分析機能
ローカルファイルシステムから複数四半期決算の時系列分析を実行
"""

import os
import re
import argparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
from collections import defaultdict
import yaml

import vertexai
from vertexai.generative_models import GenerativeModel
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from google.api_core import exceptions

from tdnet_analyzer.common.path_utils import project_path
from tdnet_analyzer.common.constants import load_company_info_map, normalize_code

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
    with open(PROMPT_DIR / 'timeseries_analysis_system_prompt.md', 'r', encoding='utf-8') as f:
        TIMESERIES_SYSTEM_PROMPT_TEMPLATE = f.read()
    with open(PROMPT_DIR / 'timeseries_analysis_user_prompt.md', 'r', encoding='utf-8') as f:
        TIMESERIES_USER_PROMPT_TEMPLATE = f.read()
    with open(PROMPT_DIR / 'sector_system_prompt.md', 'r', encoding='utf-8') as f:
        SECTOR_SYSTEM_PROMPT_TEMPLATE = f.read()
    with open(PROMPT_DIR / 'sector_user_prompt.md', 'r', encoding='utf-8') as f:
        SECTOR_USER_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"プロンプトファイルが見つかりません: {e}")
    exit(1)


def extract_text_from_pdf_file(path: str) -> str:
    """PDFファイルからテキスト抽出"""
    try:
        import fitz  # PyMuPDF
        with fitz.open(path) as doc:
            texts = []
            for page in doc:
                try:
                    texts.append(page.get_text("text") or "")
                except Exception:
                    continue
            return "\n".join(texts)
    except ImportError:
        try:
            from pypdf import PdfReader
            with open(path, "rb") as f:
                reader = PdfReader(f)
                texts = []
                for page in reader.pages:
                    try:
                        texts.append(page.extract_text() or "")
                    except Exception:
                        continue
                return "\n".join(texts)
        except ImportError:
            logger.error("PDFライブラリ（PyMuPDF または pypdf）がインストールされていません")
            return ""
    except Exception as e:
        logger.error(f"PDFテキスト抽出エラー: {e}")
        return ""


def sort_by_quarter_and_date(file_paths: List[str]) -> List[str]:
    """
    ファイル名から四半期・日付情報を抽出してソート
    """
    def extract_quarter_key(file_path: str) -> Tuple[int, int, int]:
        filename = os.path.basename(file_path)
        
        # Q1/Q2/Q3/Q4パターン
        quarter_match = re.search(r'(\d{4})Q([1-4])', filename)
        if quarter_match:
            year = int(quarter_match.group(1))
            quarter = int(quarter_match.group(2))
            return (year, quarter, 0)
        
        # YYYYMMDD日付パターン
        date_match = re.search(r'(\d{8})', filename)
        if date_match:
            date_str = date_match.group(1)
            year = int(date_str[:4])
            month = int(date_str[4:6])
            quarter = (month - 1) // 3 + 1
            day = int(date_str[6:8])
            return (year, quarter, day)
        
        # フォールバック
        return (9999, 9, 99)
    
    return sorted(file_paths, key=extract_quarter_key)


def collect_local_sector_data(sectors_root: str) -> Dict[str, Dict[str, Dict[str, List[str]]]]:
    """
    ローカルsectors/ ディレクトリから業種・規模・銘柄別にファイルを収集
    """
    logger.info(f"ローカル業種別ディレクトリからファイル収集開始: {sectors_root}")
    
    sectors_path = Path(sectors_root)
    if not sectors_path.exists():
        logger.error(f"sectors ディレクトリが見つかりません: {sectors_root}")
        return {}
    
    sector_data = defaultdict(lambda: defaultdict(lambda: defaultdict(list)))
    
    # sectors/業種/規模/ 構造を走査
    for sector_dir in sectors_path.iterdir():
        if not sector_dir.is_dir():
            continue
        
        sector_name = sector_dir.name
        
        for size_dir in sector_dir.iterdir():
            if not size_dir.is_dir():
                continue
            
            size_name = size_dir.name
            
            # PDFファイルを収集
            for pdf_file in size_dir.glob("*.pdf"):
                try:
                    # ファイル名から証券コード抽出: 証券コード_会社名_元ファイル名.pdf
                    filename = pdf_file.name
                    code = filename.split('_')[0]
                    sector_data[sector_name][size_name][code].append(str(pdf_file))
                except Exception as e:
                    logger.warning(f"ファイル名解析失敗: {filename}, エラー: {e}")
                    continue
    
    total_files = sum(len(files) for sector in sector_data.values() for size in sector.values() for files in size.values())
    logger.info(f"収集完了: {len(sector_data)} 業種, 合計ファイル数: {total_files}")
    return dict(sector_data)


def analyze_local_company_timeseries(sector: str, size: str, code: str, files: List[str], 
                                   project: str, location: str, model_name: str, company_info_map: Dict) -> str:
    """
    ローカル版銘柄ごとの時系列分析を実行
    """
    logger.info(f"ローカル時系列分析開始: {sector}/{size}/{code} ({len(files)} ファイル)")
    
    # 1. ファイル名から時系列順にソート
    sorted_files = sort_by_quarter_and_date(files)
    
    # 2. 各PDFからテキスト抽出
    document_texts = []
    for file_path in sorted_files:
        try:
            text = extract_text_from_pdf_file(file_path)
            filename = os.path.basename(file_path)
            
            # 四半期情報抽出
            quarter_info = "不明"
            quarter_match = re.search(r'(\d{4})Q([1-4])', filename)
            if quarter_match:
                quarter_info = f"{quarter_match.group(1)}年Q{quarter_match.group(2)}"
            else:
                date_match = re.search(r'(\d{8})', filename)
                if date_match:
                    date_str = date_match.group(1)
                    quarter_info = f"{date_str[:4]}年{date_str[4:6]}月"
            
            document_texts.append({
                'quarter': quarter_info,
                'text': text,
                'filename': filename
            })
        except Exception as e:
            logger.error(f"テキスト抽出エラー ({file_path}): {e}")
            continue
    
    if not document_texts:
        logger.warning(f"テキスト抽出に失敗: {code}")
        return ""
    
    # 3. 企業情報取得
    company_info = company_info_map.get(normalize_code(code))
    company_name = company_info[0] if company_info else "不明"
    
    # 4. 時系列分析プロンプト構築
    document_list = []
    for i, doc in enumerate(document_texts):
        document_list.append(f"{i+1}. **{doc['quarter']}**: {doc['filename']}")
    
    user_prompt = TIMESERIES_USER_PROMPT_TEMPLATE.replace("{{company_code}}", code)
    user_prompt = user_prompt.replace("{{company_name}}", company_name)
    user_prompt = user_prompt.replace("{{sector_name}}", sector)
    user_prompt = user_prompt.replace("{{document_list}}", "\n".join(document_list))
    
    # 全文書のテキストを結合
    combined_text = "\n\n".join([f"=== {doc['quarter']}: {doc['filename']} ===\n{doc['text']}" for doc in document_texts])
    
    # 5. Vertex AI で時系列分析実行
    try:
        summary = summarize_text_with_vertex(
            project, location, model_name,
            TIMESERIES_SYSTEM_PROMPT_TEMPLATE,
            user_prompt,
            combined_text
        )
        logger.info(f"ローカル時系列分析完了: {code}")
        return summary
    except Exception as e:
        logger.error(f"ローカル時系列分析失敗 ({code}): {e}")
        return ""


def analyze_local_sector_timeseries(sector: str, size: str, company_summaries: List[str], 
                                  project: str, location: str, model_name: str) -> str:
    """
    ローカル版セクター全体の時系列トレンド分析
    """
    if not company_summaries:
        return ""
    
    logger.info(f"ローカルセクター時系列分析開始: {sector}/{size} ({len(company_summaries)} 企業)")
    
    # セクター分析用プロンプト
    system_prompt = SECTOR_SYSTEM_PROMPT_TEMPLATE.replace("{{sector_name}}", f"{sector}_{size}_時系列分析").replace("{{count}}", str(len(company_summaries)))
    user_prompt = SECTOR_USER_PROMPT_TEMPLATE.replace("{{summaries}}", "\n\n".join(company_summaries))
    
    try:
        sector_insights = summarize_text_with_vertex(
            project, location, model_name,
            system_prompt,
            user_prompt
        )
        logger.info(f"ローカルセクター時系列分析完了: {sector}/{size}")
        return sector_insights
    except Exception as e:
        logger.error(f"ローカルセクター時系列分析失敗 ({sector}/{size}): {e}")
        return ""


def save_local_timeseries_results(out_root: str, sector: str, size: str, 
                                company_results: Dict[str, str], sector_insights: str):
    """
    ローカル業種・規模別ディレクトリに結果を保存
    """
    logger.info(f"ローカル時系列分析結果保存開始: {sector}/{size}")
    
    out_path = Path(out_root)
    results_dir = out_path / "sectors-analysis" / sector / size
    results_dir.mkdir(parents=True, exist_ok=True)
    
    # 個別企業サマリー保存
    for code, summary in company_results.items():
        if not summary:
            continue
        
        filename = f"{code}_timeseries_summary.md"
        file_path = results_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(summary)
            logger.info(f"個別時系列サマリー保存: {file_path}")
        except Exception as e:
            logger.error(f"個別時系列サマリー保存失敗 ({code}): {e}")
    
    # セクターインサイト保存
    if sector_insights:
        filename = "sector_timeseries_insights.md"
        file_path = results_dir / filename
        
        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(sector_insights)
            logger.info(f"セクター時系列インサイト保存: {file_path}")
        except Exception as e:
            logger.error(f"セクター時系列インサイト保存失敗: {e}")


def summarize_text_with_vertex(project: str, location: str, model_name: str, system_prompt: str, user_prompt: str, content: str = "") -> str:
    """Vertex AI でテキスト要約"""
    max_retries = 5
    initial_backoff = 2
    for attempt in range(max_retries):
        try:
            vertexai.init(project=project, location=location)
            model = GenerativeModel(model_name, system_instruction=system_prompt)
            
            full_prompt = user_prompt
            if content:
                full_prompt += "\n\n" + content
            
            response = model.generate_content(full_prompt)
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


def run_local_sector_timeseries_analysis(sectors_root: str, out_root: str, project: str, location: str, model_name: str):
    """ローカル版業種別時系列分析メイン処理"""
    logger.info(f"ローカル版業種別時系列分析開始")
    
    # 企業情報マップ読み込み
    company_info_map = {}
    try:
        companies_csv_path = project_path('inputs', 'companies.csv')
        company_info_map = load_company_info_map(str(companies_csv_path))
        logger.info(f"企業情報マップ読み込み: {len(company_info_map)} 件")
    except Exception as e:
        logger.warning(f"企業情報マップ読み込み失敗: {e}")
    
    # ステップ1: 入力データ収集
    sector_data = collect_local_sector_data(sectors_root)
    
    if not sector_data:
        logger.error("ローカル業種別データが見つかりません")
        return
    
    # ステップ2: 銘柄ごと時系列分析
    total_companies = sum(len(companies) for sector in sector_data.values() for size in sector.values() for companies in size.values())
    processed_companies = 0
    
    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        for sector, size_data in sector_data.items():
            for size, company_data in size_data.items():
                company_results = {}
                future_to_info = {}
                
                # 個別企業分析を並列実行
                for code, files in company_data.items():
                    if len(files) < 2:  # 時系列分析には最低2ファイル必要
                        logger.warning(f"ファイル数不足でスキップ: {sector}/{size}/{code} ({len(files)} ファイル)")
                        continue
                    
                    future = executor.submit(
                        analyze_local_company_timeseries,
                        sector, size, code, files,
                        project, location, model_name, company_info_map
                    )
                    future_to_info[future] = (sector, size, code)
                
                # 結果収集
                for future in as_completed(future_to_info):
                    sector_name, size_name, code = future_to_info[future]
                    processed_companies += 1
                    
                    try:
                        summary = future.result()
                        if summary:
                            company_results[code] = summary
                            logger.info(f"✅ ({processed_companies}/{total_companies}) ローカル時系列分析完了: {sector_name}/{size_name}/{code}")
                        else:
                            logger.warning(f"⚠️ ({processed_companies}/{total_companies}) ローカル時系列分析結果が空: {sector_name}/{size_name}/{code}")
                    except Exception as e:
                        logger.error(f"❌ ({processed_companies}/{total_companies}) ローカル時系列分析失敗: {sector_name}/{size_name}/{code}, エラー: {e}")
                
                # ステップ3: セクター集約分析
                if company_results:
                    company_summaries = list(company_results.values())
                    sector_insights = analyze_local_sector_timeseries(
                        sector, size, company_summaries,
                        project, location, model_name
                    )
                    
                    # ステップ4: ローカル保存
                    save_local_timeseries_results(
                        out_root, sector, size,
                        company_results, sector_insights
                    )
    
    logger.info("ローカル版業種別時系列分析完了")


def main():
    parser = argparse.ArgumentParser(description='ローカル版業種別時系列分析')
    parser.add_argument('--sectors-root', type=str, required=True, help='sectors ディレクトリのパス')
    parser.add_argument('--out-root', type=str, required=True, help='結果出力先ディレクトリ')
    parser.add_argument('--project', type=str, required=True, help='Google Cloud Project ID')
    parser.add_argument('--location', default=DEFAULT_LOCATION, help='Vertex AI location')
    parser.add_argument('--model', default=DEFAULT_MODEL, help=f'Vertex AI model name (default: {DEFAULT_MODEL})')
    
    args = parser.parse_args()
    
    try:
        run_local_sector_timeseries_analysis(
            sectors_root=args.sectors_root,
            out_root=args.out_root,
            project=args.project,
            location=args.location,
            model_name=args.model
        )
        
        print("ローカル版業種別時系列分析完了")
        
    except Exception as e:
        logger.error(f"ローカル版業種別時系列分析異常終了: {e}")
        exit(1)


if __name__ == "__main__":
    main()