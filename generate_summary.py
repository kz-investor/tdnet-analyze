#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import re
import argparse
import os
import sys
import tempfile
import datetime
from dataclasses import dataclass, field
from typing import Dict, List, Optional
from collections import defaultdict

import yaml
from google.cloud import storage
import google.auth
import google.auth.transport.requests
import requests
import vertexai
from vertexai.generative_models import GenerativeModel

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import random
from google.api_core import exceptions

# ロギング設定（Cloud Functions対応）
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Cloud Functions用のログ出力関数


def log_info(message):
    print(f"INFO: {message}")  # Cloud Loggingに出力
    logger.info(message)  # ローカル実行時用


# PDF抽出バックエンド
FITZ_AVAILABLE = False
PDF_AVAILABLE = False
try:
    import fitz  # PyMuPDF
    FITZ_AVAILABLE = True
except Exception:
    pass
try:
    from pypdf import PdfReader
    PDF_AVAILABLE = True
except Exception:
    pass

# 証券コード正規化
try:
    from constants import normalize_code
except Exception:
    def normalize_code(c: str) -> str:
        return c

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
DEFAULT_BASE = os.environ.get('TDNET_BASE', CONFIG.get('gcs', {}).get('base_path', 'tdnet-analyzer'))
DEFAULT_LOCATION = "us-central1"
DEFAULT_MODEL = CONFIG.get('llm', {}).get('model_name', 'gemini-1.0-pro')
DEFAULT_MAX_WORKERS = CONFIG.get('llm', {}).get('parallel', {}).get('max_workers', 10)

# プロンプトの読み込み
PROMPT_DIR = os.path.join(os.path.dirname(__file__), 'prompt_templates')
try:
    # 通常版（大型株用）
    with open(os.path.join(PROMPT_DIR, 'summary_system_prompt.md'), 'r', encoding='utf-8') as f:
        SUMMARY_SYSTEM_PROMPT_TEMPLATE = f.read()
    # コンパクト版（小型株用）
    with open(os.path.join(PROMPT_DIR, 'summary_system_prompt_small.md'), 'r', encoding='utf-8') as f:
        SUMMARY_SYSTEM_PROMPT_SMALL_TEMPLATE = f.read()
    with open(os.path.join(PROMPT_DIR, 'summary_user_prompt.md'), 'r', encoding='utf-8') as f:
        SUMMARY_USER_PROMPT_TEMPLATE = f.read()
except FileNotFoundError as e:
    logger.error(f"プロンプトファイルが見つかりません: {e}")
    sys.exit(1)


def should_use_compact_prompt(size: str) -> bool:
    """規模区分に基づいてコンパクトプロンプトを使用するかを判定"""
    if not size or size == 'Unknown':
        return True  # 不明な場合はコンパクト版

    # 大型株は通常版、それ以外はコンパクト版
    large_cap_keywords = ['Core30', 'Large70', 'Mid400']
    return not any(keyword in size for keyword in large_cap_keywords)


@dataclass
class Document:
    code: str
    company_name: str
    title: str
    doc_type: str
    gcs_path: str = ""
    local_path: str = ""


@dataclass
class DocumentGroup:
    """証券コードでグループ化された文書群"""
    code: str
    name: str
    sector: str
    size: str
    documents: List[Document] = field(default_factory=list)
    combined_text: str = ""
    summary: str = ""


def load_metadata(client: storage.Client, bucket: str, base: str, date_str: str) -> Dict:
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    filename = f"metadata_{date_str}.json"
    path_parts = [base, year, month, day, filename]
    path = "/".join(part for part in path_parts if part)
    blob = client.bucket(bucket).blob(path)
    data = blob.download_as_bytes()
    import json
    return json.loads(data.decode("utf-8"))


def _extract_text_with_fitz(path: str) -> Optional[str]:
    """PyMuPDF(fitz)を使用してテキストを抽出する。失敗した場合はNoneを返す。"""
    try:
        with fitz.open(path) as doc:
            texts: List[str] = []
            for page in doc:
                try:
                    texts.append(page.get_text("text") or "")
                except Exception:
                    continue
            return "\n".join(texts)
    except Exception as e:
        logger.warning(f"PyMuPDF(fitz)でのテキスト抽出に失敗: {e}。pypdfにフォールバックします。")
        return None


def _extract_text_with_pypdf(path: str) -> str:
    try:
        with open(path, "rb") as f:
            reader = PdfReader(f)
            texts = []
            for page in reader.pages:
                try:
                    texts.append(page.extract_text() or "")
                except Exception:
                    continue
        return "\n".join(texts)
    except Exception:
        return ""


def extract_text_from_pdf_file(path: str) -> str:
    """PDFからテキストを抽出する。fitzを優先し、失敗した場合のみpypdfにフォールバックする。"""
    if FITZ_AVAILABLE:
        # fitzはより堅牢なため、まず試す
        text = _extract_text_with_fitz(path)
        if text is not None:
            # fitzが処理できた場合（空文字列を含む）、その結果を返す
            return text

    # fitzが利用不可、または処理に失敗した場合のみpypdfを試す
    if PDF_AVAILABLE:
        return _extract_text_with_pypdf(path)

    return ""


def extract_text_from_pdf_blob(blob: storage.Blob) -> str:
    fd, tmp_path = tempfile.mkstemp(suffix=".pdf")
    os.close(fd)
    try:
        blob.download_to_filename(tmp_path)
        return extract_text_from_pdf_file(tmp_path)
    finally:
        try:
            os.remove(tmp_path)
        except Exception:
            pass


def summarize_text_with_vertex(project: str, location: str, model_name: str, system_prompt: str, user_prompt: str, content: str) -> str:
    """システムプロンプトとユーザープロンプトを分けてLLMにリクエストする"""
    max_retries = 5
    initial_backoff = 2  # seconds

    for attempt in range(max_retries):
        try:
            vertexai.init(project=project, location=location)
            model = GenerativeModel(model_name, system_instruction=system_prompt)
            response = model.generate_content(
                user_prompt + "\n\n" + content,
            )
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


def build_docs_from_metadata(meta: Dict) -> List[Document]:
    docs: List[Document] = []
    for d in meta.get("documents", []):
        docs.append(Document(
            code=str(d.get("code")),
            company_name=d.get("company_name", ""),
            title=d.get("title", ""),
            doc_type=d.get("doc_type", "other"),
            gcs_path=d.get("gcs_path") or ""
        ))
    return docs


def build_docs_from_local(local_dir: str, include: Optional[str] = None, codes: Optional[List[str]] = None, max_files: Optional[int] = None) -> List[Document]:
    docs: List[Document] = []
    for root, _, files in os.walk(local_dir):
        for fn in files:
            if not fn.lower().endswith('.pdf'):
                continue
            if include and include not in fn:
                continue
            code = fn.split('_', 1)[0]
            if codes and code not in codes:
                continue
            title = os.path.splitext(fn.split('_', 1)[-1])[0] if '_' in fn else fn
            docs.append(Document(code=code, company_name="", title=title, doc_type="tanshin", local_path=os.path.join(root, fn)))
            if max_files and len(docs) >= max_files:
                return docs
    return docs


def generate_summaries(dates: List[str], bucket: str = DEFAULT_BUCKET, base: str = DEFAULT_BASE,
                       project: Optional[str] = None, location: str = DEFAULT_LOCATION,
                       model_name: str = DEFAULT_MODEL,
                       local_dir: Optional[str] = None, include: Optional[str] = None,
                       codes: Optional[List[str]] = None, max_files: Optional[int] = None) -> List[str]:
    """特定の日付の開示文書からインサイトを生成する。証券コードごとに資料をまとめて要約。"""
    output_date_str = dates[-1] if dates else datetime.datetime.now().strftime("%Y%m%d")
    logger.info(
        f"インサイト生成開始: 期間={dates[0] if dates else 'N/A'}-{dates[-1] if dates else 'N/A'}, バケット={bucket}, ベースパス={base}, プロジェクト={project or '未指定'}, ロケーション={location}, モデル={model_name}")

    if not project:
        logger.error("Google Cloud Project IDが指定されていません。--project オプションで指定するか、環境変数 GOOGLE_CLOUD_PROJECT を設定してください。")
        return []

    # --- GCSクライアントのカスタマイズ ---
    # 多数の並列処理に対応するため、接続プールサイズを増やす
    try:
        credentials, project_id_from_auth = google.auth.default()
        # 引数で渡されたprojectを優先する
        final_project_id = project or project_id_from_auth

        # requests.Session をカスタマイズ
        adapter = requests.adapters.HTTPAdapter(pool_connections=DEFAULT_MAX_WORKERS, pool_maxsize=DEFAULT_MAX_WORKERS)
        session = requests.Session()
        session.mount('https://', adapter)

        # カスタマイズしたセッションで認証済みセッションを作成
        authed_session = google.auth.transport.requests.AuthorizedSession(credentials, session=session)

        client = storage.Client(project=final_project_id, credentials=credentials, _http=authed_session)
        logger.info(f"GCSクライアントをカスタマイズしました。プロジェクト: {final_project_id}, 接続プールサイズ: {DEFAULT_MAX_WORKERS}")
    except Exception as e:
        logger.warning(f"GCSクライアントのカスタマイズに失敗しました。デフォルト設定で続行します。エラー: {e}")
        client = storage.Client(project=project)

    company_info_map: Dict[str, tuple[str, str, str]] = {}
    try:
        from constants import load_company_info_map
        companies_csv_path = os.path.join(os.path.dirname(__file__), "inputs", "companies.csv")
        company_info_map = load_company_info_map(companies_csv_path)
    except Exception as e:
        logger.warning(f"constants.pyからのcompany_info_mapロードに失敗: {e}。企業名・業種・規模分類はできません。")

    # 1. まず全ドキュメント情報をロード
    all_docs: List[Document]
    if local_dir:
        all_docs = build_docs_from_local(local_dir, include, codes, max_files)
        logger.info(f"ローカルディレクトリ {local_dir} から {len(all_docs)} 件の文書をロードしました。")
    else:
        all_docs = []
        for date_str in dates:
            try:
                all_docs.extend(build_docs_from_metadata(load_metadata(client, bucket, base, date_str)))
            except Exception as e:
                logger.warning(f"メタデータの読み込みに失敗しました(date={date_str}): {e}")
        log_info(f"GCSメタデータから {len(all_docs)} 件の文書をロードしました。")

    # 2. 証券コードでグループ化（33業種+規模区分）
    doc_groups: Dict[str, DocumentGroup] = defaultdict(lambda: DocumentGroup(code="", name="", sector="", size=""))
    for doc in all_docs:
        if include and include not in doc.title and include not in doc.local_path:
            continue
        if codes and normalize_code(doc.code) not in codes:
            continue

        group = doc_groups[doc.code]
        if not group.code:  # 初回のみ設定
            group.code = doc.code
            company_info = company_info_map.get(normalize_code(doc.code))
            if company_info:
                group.name, group.sector, group.size = company_info
            else:
                group.name, group.sector, group.size = "不明", "不明", "Unknown"
        group.documents.append(doc)

    log_info(f"フィルタリングとグループ化の結果、{len(doc_groups)} 証券コード分の文書を処理します。")

    groups_to_process = list(doc_groups.values())
    if max_files is not None and len(groups_to_process) > max_files:
        logger.info(f"max_files={max_files} のため、上位 {max_files} 件の証券コードのみ処理します。")
        groups_to_process = groups_to_process[:max_files]

    if not groups_to_process:
        logger.info("処理対象の文書がありません。スクリプトを終了します。")
        return []

    outputs: List[str] = []
    total_groups = len(groups_to_process)
    processed_groups_count = 0

    # 3. グループごとに並列処理
    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        # フェーズ1: テキスト抽出を並列実行
        future_to_group_extraction = {}
        for group in groups_to_process:
            future = executor.submit(extract_texts_for_group, group, client, bucket, bool(local_dir))
            future_to_group_extraction[future] = group

        for future in as_completed(future_to_group_extraction):
            group = future_to_group_extraction[future]
            try:
                group.combined_text = future.result()
            except Exception as e:
                logger.error(f"テキスト抽出エラー: コード={group.code}, エラー={e}")
                group.combined_text = ""

        # フェーズ2: LLM要約を並列実行
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

            # 規模区分に応じてプロンプトを選択
            use_compact = should_use_compact_prompt(group.size)
            system_prompt = SUMMARY_SYSTEM_PROMPT_SMALL_TEMPLATE if use_compact else SUMMARY_SYSTEM_PROMPT_TEMPLATE

            logger.info(f"プロンプト選択: コード={group.code}, 規模={group.size}, コンパクト={use_compact}")

            future = executor.submit(summarize_text_with_vertex,
                                     project, location, model_name,
                                     system_prompt,
                                     user_prompt,
                                     group.combined_text)
            future_to_group_summarization[future] = group

        for future in as_completed(future_to_group_summarization):
            processed_groups_count += 1
            group = future_to_group_summarization[future]
            try:
                summary_text = future.result()
                print(f"INFO: ({processed_groups_count}/{total_groups}) 要約成功: コード={group.code}")
                logger.info(f"({processed_groups_count}/{total_groups}) 要約成功: コード={group.code}")

                summary_filename = f"{output_date_str}__{safe_name(group.sector)}__{safe_name(group.size)}__{group.code}__{safe_name(group.name)}_summary.md"
                path_parts = [base, "insights-summaries", output_date_str, summary_filename]
                summary_gcs_path = "/".join(part for part in path_parts if part)

                try:
                    blob = client.bucket(bucket).blob(summary_gcs_path)
                    blob.upload_from_string(summary_text.encode('utf-8'), content_type="text/markdown")
                    outputs.append(summary_gcs_path)
                    logger.info(f"  個別サマリをGCSにアップロードしました: gs://{bucket}/{summary_gcs_path}")
                except Exception as e:
                    logger.error(f"  個別サマリのGCSアップロードエラー (ファイル: {summary_gcs_path}): {e}")

            except Exception as e:
                logger.error(f"({processed_groups_count}/{total_groups}) 要約失敗: コード={group.code}, エラー={e}")

    logger.info("generate_summary.py スクリプト完了")
    return outputs


def extract_texts_for_group(group: DocumentGroup, client: storage.Client, bucket: str, is_local: bool) -> str:
    """DocumentGroup内の全ドキュメントのテキストを抽出し、結合する"""
    combined_texts: List[str] = []
    for i, doc in enumerate(group.documents):
        full_text = ""
        try:
            if is_local:
                full_text = extract_text_from_pdf_file(doc.local_path)
            else:
                blob = client.bucket(bucket).blob(doc.gcs_path)
                full_text = extract_text_from_pdf_blob(blob)
        except Exception as e:
            logger.error(f"  (文書 {i+1}/{len(group.documents)}) テキスト抽出エラー: {doc.title}, エラー: {e}")
            full_text = f"--- テキスト抽出エラー: {doc.title} ---\n"

        combined_texts.append(f"--- 文書: {doc.title} ---\n{full_text}")

    return "\n\n".join(combined_texts)


def get_date_range(start_date_str: str, end_date_str: str) -> List[str]:
    """YYYYMMDD形式の開始日と終了日から日付文字列のリストを生成する"""
    start_date = datetime.datetime.strptime(start_date_str, "%Y%m%d")
    end_date = datetime.datetime.strptime(end_date_str, "%Y%m%d")
    delta = end_date - start_date
    return [(start_date + datetime.timedelta(days=i)).strftime("%Y%m%d") for i in range(delta.days + 1)]


def main():
    log_info("generate_summary.py スクリプト開始")
    parser = argparse.ArgumentParser(description="Generate company-level summaries from TDnet documents.")
    parser.add_argument('--start-date', required=True, help='Target start date in YYYYMMDD format.')
    parser.add_argument('--end-date', required=True, help='Target end date in YYYYMMDD format.')
    parser.add_argument('--bucket', default=DEFAULT_BUCKET, help='GCS bucket name.')
    parser.add_argument('--base', default=DEFAULT_BASE, help='GCS base path.')
    parser.add_argument('--project', default=os.environ.get('GOOGLE_CLOUD_PROJECT'), help='Google Cloud Project ID.')
    parser.add_argument('--location', default=DEFAULT_LOCATION, help='Vertex AI location.')
    parser.add_argument('--model', default=DEFAULT_MODEL, help=f'Vertex AI model name (default: {DEFAULT_MODEL})')
    parser.add_argument('--local-dir', help='Local directory to load PDFs from (for testing).')
    parser.add_argument('--include', help='Substring to filter documents by title or path.')
    parser.add_argument('--codes', help='Comma-separated list of company codes to process.')
    parser.add_argument('--max-files', type=int, default=None, help='Limit the number of files to process for debugging.')

    args = parser.parse_args()

    codes_list = args.codes.split(',') if args.codes else None
    dates_to_process = get_date_range(args.start_date, args.end_date)

    try:
        generate_summaries(
            dates=dates_to_process,
            bucket=args.bucket,
            base=args.base,
            project=args.project,
            location=args.location,
            model_name=args.model,
            local_dir=args.local_dir,
            include=args.include,
            codes=codes_list,
            max_files=args.max_files
        )
        log_info("generate_summary.py スクリプト正常終了")
    except Exception as e:
        print(f"ERROR: generate_summary.py スクリプト異常終了: {e}")
        logger.error(f"generate_summary.py スクリプト異常終了: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
