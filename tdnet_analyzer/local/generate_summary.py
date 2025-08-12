#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ローカル版サマリー生成機能
既存クラウド版と同じ仕様でローカルファイルシステムから処理
"""

import os
import argparse
import json
import datetime
from pathlib import Path
from typing import List, Dict, Optional
from dataclasses import dataclass, field

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
    with open(PROMPT_DIR / 'summary_system_prompt.md', 'r', encoding='utf-8') as f:
        SUMMARY_SYSTEM_PROMPT_TEMPLATE = f.read()
    with open(PROMPT_DIR / 'summary_system_prompt_small.md', 'r', encoding='utf-8') as f:
        SUMMARY_SYSTEM_PROMPT_SMALL_TEMPLATE = f.read()
    with open(PROMPT_DIR / 'summary_user_prompt.md', 'r', encoding='utf-8') as f:
        SUMMARY_USER_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"プロンプトファイルが見つかりません: {e}")
    exit(1)


@dataclass
class Document:
    code: str
    company_name: str
    title: str
    doc_type: str
    local_path: str = ""


@dataclass
class DocumentGroup:
    code: str
    name: str
    sector: str
    size: str
    documents: List[Document] = field(default_factory=list)
    combined_text: str = ""
    summary: str = ""


def should_use_compact_prompt(size: str) -> bool:
    """規模に応じてプロンプト選択"""
    if not size or size == 'Unknown':
        return True
    large_cap_keywords = ['Core30', 'Large70', 'Mid400']
    return not any(keyword in size for keyword in large_cap_keywords)


def extract_text_from_pdf_file(path: str) -> str:
    """PDFからテキスト抽出"""
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


def summarize_text_with_vertex(project: str, location: str, model_name: str, system_prompt: str, user_prompt: str, content: str) -> str:
    """Vertex AI でテキスト要約"""
    max_retries = 5
    initial_backoff = 2
    for attempt in range(max_retries):
        try:
            vertexai.init(project=project, location=location)
            model = GenerativeModel(model_name, system_instruction=system_prompt)
            response = model.generate_content(user_prompt + "\n\n" + content)
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


def load_local_metadata(metadata_path: str) -> Dict:
    """ローカルメタデータ読み込み"""
    with open(metadata_path, 'r', encoding='utf-8') as f:
        return json.load(f)


def build_docs_from_local_metadata(out_root: str, dates: List[str]) -> List[Document]:
    """ローカルメタデータからドキュメント構築"""
    docs = []
    out_path = Path(out_root)
    
    for date_str in dates:
        year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
        metadata_path = out_path / year / month / day / f"metadata_{date_str}.json"
        
        if not metadata_path.exists():
            logger.warning(f"メタデータファイルが見つかりません: {metadata_path}")
            continue
        
        try:
            metadata = load_local_metadata(str(metadata_path))
            for doc_info in metadata.get('documents', []):
                docs.append(Document(
                    code=str(doc_info.get('code')),
                    company_name=doc_info.get('company_name', ''),
                    title=doc_info.get('title', ''),
                    doc_type=doc_info.get('doc_type', 'other'),
                    local_path=doc_info.get('local_path', '')
                ))
        except Exception as e:
            logger.error(f"メタデータ読み込みエラー ({metadata_path}): {e}")
    
    return docs


def extract_texts_for_group(group: DocumentGroup) -> str:
    """グループのテキスト抽出"""
    combined_texts = []
    for i, doc in enumerate(group.documents):
        try:
            full_text = extract_text_from_pdf_file(doc.local_path)
            combined_texts.append(f"--- 文書: {doc.title} ---\n{full_text}")
        except Exception as e:
            logger.error(f"  (文書 {i+1}/{len(group.documents)}) テキスト抽出エラー: {doc.title}, エラー: {e}")
            combined_texts.append(f"--- テキスト抽出エラー: {doc.title} ---\n")
    
    return "\n\n".join(combined_texts)


def generate_local_summaries(dates: List[str], out_root: str, project: str, 
                           location: str = DEFAULT_LOCATION, model_name: str = DEFAULT_MODEL,
                           codes: Optional[List[str]] = None, max_files: Optional[int] = None) -> List[str]:
    """ローカル版サマリー生成"""
    output_date_str = dates[-1] if dates else datetime.datetime.now().strftime("%Y%m%d")
    logger.info(f"ローカル版サマリー生成開始: 期間={dates[0] if dates else 'N/A'}-{dates[-1] if dates else 'N/A'}")
    
    if not project:
        logger.error("Google Cloud Project IDが指定されていません。")
        return []
    
    # 企業情報マップ読み込み
    company_info_map = {}
    try:
        companies_csv_path = project_path('inputs', 'companies.csv')
        company_info_map = load_company_info_map(str(companies_csv_path))
    except Exception as e:
        logger.warning(f"企業情報マップ読み込み失敗: {e}")
    
    # ローカルメタデータからドキュメント読み込み
    all_docs = build_docs_from_local_metadata(out_root, dates)
    logger.info(f"ローカルメタデータから {len(all_docs)} 件の文書をロードしました")
    
    # 証券コードでグループ化
    from collections import defaultdict
    doc_groups = defaultdict(lambda: DocumentGroup(code="", name="", sector="", size=""))
    
    for doc in all_docs:
        if codes and normalize_code(doc.code) not in codes:
            continue
        
        group = doc_groups[doc.code]
        if not group.code:
            group.code = doc.code
            company_info = company_info_map.get(normalize_code(doc.code))
            if company_info:
                group.name, group.sector, group.size = company_info
            else:
                group.name, group.sector, group.size = "不明", "不明", "Unknown"
        group.documents.append(doc)
    
    logger.info(f"グループ化結果: {len(doc_groups)} 証券コード分の文書を処理します")
    
    groups_to_process = list(doc_groups.values())
    if max_files and len(groups_to_process) > max_files:
        groups_to_process = groups_to_process[:max_files]
    
    if not groups_to_process:
        logger.info("処理対象の文書がありません")
        return []
    
    outputs = []
    out_path = Path(out_root)
    
    # サマリー出力ディレクトリ作成
    summary_dir = out_path / "insights-summaries" / output_date_str
    summary_dir.mkdir(parents=True, exist_ok=True)
    
    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        # テキスト抽出
        future_to_group_extraction = {}
        for group in groups_to_process:
            future = executor.submit(extract_texts_for_group, group)
            future_to_group_extraction[future] = group
        
        for future in as_completed(future_to_group_extraction):
            group = future_to_group_extraction[future]
            try:
                group.combined_text = future.result()
            except Exception as e:
                logger.error(f"テキスト抽出エラー: コード={group.code}, エラー={e}")
                group.combined_text = ""
        
        # サマリー生成
        future_to_group_summarization = {}
        for group in groups_to_process:
            if not group.combined_text:
                logger.warning(f"テキストが空のためスキップ: コード={group.code}")
                continue
            
            titles = [d.title for d in group.documents]
            user_prompt = SUMMARY_USER_PROMPT_TEMPLATE.replace("{{company_code}}", group.code)
            user_prompt = user_prompt.replace("{{company_name}}", group.name or '不明')
            user_prompt = user_prompt.replace("{{sector_name}}", group.sector or '不明')
            user_prompt = user_prompt.replace("{{titles}}", "\n".join([f"- {t}" for t in titles]))
            
            use_compact = should_use_compact_prompt(group.size)
            system_prompt = SUMMARY_SYSTEM_PROMPT_SMALL_TEMPLATE if use_compact else SUMMARY_SYSTEM_PROMPT_TEMPLATE
            logger.info(f"プロンプト選択: コード={group.code}, 規模={group.size}, コンパクト={use_compact}")
            
            future = executor.submit(
                summarize_text_with_vertex,
                project, location, model_name,
                system_prompt, user_prompt, group.combined_text
            )
            future_to_group_summarization[future] = group
        
        processed_count = 0
        total_groups = len(future_to_group_summarization)
        
        for future in as_completed(future_to_group_summarization):
            processed_count += 1
            group = future_to_group_summarization[future]
            try:
                summary_text = future.result()
                logger.info(f"({processed_count}/{total_groups}) 要約成功: コード={group.code}")
                
                # ローカルファイル保存（既存クラウド版と同じ形式）
                summary_filename = f"{output_date_str}__{safe_name(group.sector)}__{safe_name(group.size)}__{group.code}__{safe_name(group.name)}_summary.md"
                summary_path = summary_dir / summary_filename
                
                with open(summary_path, 'w', encoding='utf-8') as f:
                    f.write(summary_text)
                
                outputs.append(str(summary_path))
                logger.info(f"  個別サマリーをローカルに保存: {summary_path}")
                
            except Exception as e:
                logger.error(f"({processed_count}/{total_groups}) 要約失敗: コード={group.code}, エラー={e}")
    
    logger.info("ローカル版サマリー生成完了")
    return outputs


def get_date_range(start_date_str: str, end_date_str: str) -> List[str]:
    """日付範囲生成"""
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d")
    delta = end_date - start_date
    return [(start_date + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(delta.days + 1)]


def main():
    parser = argparse.ArgumentParser(description="ローカル版サマリー生成")
    parser.add_argument('--start-date', required=True, help='開始日付 (YYYYMMDD形式)')
    parser.add_argument('--end-date', required=True, help='終了日付 (YYYYMMDD形式)')
    parser.add_argument('--out-root', required=True, help='ローカル保存先ディレクトリ')
    parser.add_argument('--project', required=True, help='Google Cloud Project ID')
    parser.add_argument('--location', default=DEFAULT_LOCATION, help='Vertex AI location')
    parser.add_argument('--model', default=DEFAULT_MODEL, help=f'Vertex AI model name (default: {DEFAULT_MODEL})')
    parser.add_argument('--codes', help='処理対象の証券コード（カンマ区切り）')
    parser.add_argument('--max-files', type=int, help='処理ファイル数制限（デバッグ用）')
    
    args = parser.parse_args()
    
    codes_list = args.codes.split(',') if args.codes else None
    dates_to_process = get_date_range(args.start_date, args.end_date)
    
    try:
        outputs = generate_local_summaries(
            dates=dates_to_process,
            out_root=args.out_root,
            project=args.project,
            location=args.location,
            model_name=args.model,
            codes=codes_list,
            max_files=args.max_files
        )
        
        print(f"ローカル版サマリー生成完了: {len(outputs)} 件のサマリーを生成")
        for output in outputs:
            print(f"  {output}")
        
    except Exception as e:
        logger.error(f"ローカル版サマリー生成異常終了: {e}")
        exit(1)


if __name__ == '__main__':
    main()