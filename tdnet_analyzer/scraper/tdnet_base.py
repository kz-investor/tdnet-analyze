#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
適時開示データ取得システム - ベースクラス
日本株の適時開示サービスからデータを取得して、必要なデータを収集してデータベース化する
"""

import os
import re
import yaml
import logging
import requests
from datetime import datetime
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from typing import List, Dict, Optional, Tuple
import time
import concurrent.futures
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import deque
import csv

from tdnet_analyzer.common.path_utils import project_path


class TDNetBase:
    """適時開示データスクレイパーのベースクラス"""

    def __init__(self, config_path: str | None = None, use_multithread: bool = True):
        """初期化"""
        resolved_config = config_path or str(project_path('config', 'config.yaml'))
        self.config = self._load_config(resolved_config)
        self._setup_logging()
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        # SSL証明書の検証をスキップ（適時開示サービスは自己署名証明書を使用）
        self.session.verify = False
        # SSL警告を抑制
        import urllib3
        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        self.use_multithread = use_multithread

        # 設定ファイルからマルチスレッド設定を読み込み
        multithread_config = self.config.get('scraping', {}).get('multithread', {})
        self.max_workers = multithread_config.get('max_workers', 5)

        # レート制限用のキュー（1秒あたり最大5枚）
        self.rate_limit_queue = deque()
        self.rate_limit_lock = threading.Lock()

    def _load_config(self, config_path: str) -> Dict:
        """設定ファイルを読み込み"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                return yaml.safe_load(f)
        except FileNotFoundError:
            raise FileNotFoundError(f"設定ファイルが見つかりません: {config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"設定ファイルの解析エラー: {e}")

    def _setup_logging(self):
        """ログ設定"""
        log_config = self.config.get('logging', {})
        log_level = getattr(logging, log_config.get('level', 'INFO'))

        # Cloud Functions環境かどうかを判定
        is_cloud_functions = 'FUNCTION_TARGET' in os.environ

        handlers = [logging.StreamHandler()]
        if not is_cloud_functions:
            log_file = log_config.get('file', 'logs/scraping.log')
            # ログディレクトリを作成
            os.makedirs(os.path.dirname(log_file), exist_ok=True)
            handlers.append(logging.FileHandler(log_file, encoding='utf-8'))

        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=handlers
        )
        self.logger = logging.getLogger(__name__)

    def _is_target_document(self, title: str) -> Tuple[bool, str]:
        """対象文書かどうかを判定し、文書タイプを返す"""
        title_lower = title.lower()
        tanshin_patterns = ['決算短信', '決算短', '短信', '決算', '業績']
        for pattern in tanshin_patterns:
            if pattern in title or pattern in title_lower:
                self.logger.debug(f"決算短信として判定: {title}")
                return True, 'tanshin'
        presentation_patterns = ['説明資料', '補足資料', 'プレゼンテーション', '資料', '説明']
        for pattern in presentation_patterns:
            if pattern in title or pattern in title_lower:
                self.logger.debug(f"プレゼンテーション資料として判定: {title}")
                return True, 'presentation'
        dividend_patterns = ['配当', '配当金', '配当政策']
        for pattern in dividend_patterns:
            if pattern in title or pattern in title_lower:
                self.logger.debug(f"配当関連として判定: {title}")
                return True, 'dividend'
        other_patterns = ['開示事項', '経過', '変更', '修正', '訂正', '重要']
        for pattern in other_patterns:
            if pattern in title or pattern in title_lower:
                self.logger.debug(f"その他の重要文書として判定: {title}")
                return True, 'other'
        return False, ''

    def _extract_data_from_html(self, html_content: str, page_url: str) -> List[Dict]:
        soup = BeautifulSoup(html_content, 'html.parser')
        data_list = []
        rows = soup.find_all('tr')
        if not rows:
            self.logger.warning("テーブルの行が見つかりません")
            return []
        for row in rows:
            try:
                cells = row.find_all('td')
                if len(cells) < 6:
                    continue
                time_cell = cells[0].get_text(strip=True)
                if not time_cell or time_cell == '時刻':
                    continue
                code_cell = cells[1].get_text(strip=True)
                if not code_cell or code_cell == 'コード':
                    continue
                company_name = cells[2].get_text(strip=True)
                if not company_name or company_name == '会社名':
                    continue
                title_cell = cells[3]
                title = title_cell.get_text(strip=True)
                if not title or title == 'タイトル':
                    continue
                pdf_link = title_cell.find('a')
                pdf_url = None
                if pdf_link and pdf_link.has_attr('href'):
                    pdf_url = pdf_link['href']
                    if pdf_url:
                        if pdf_url.startswith('/'):
                            pdf_url = f"https://www.release.tdnet.info{pdf_url}"
                        elif not pdf_url.startswith('http'):
                            pdf_url = urljoin("https://www.release.tdnet.info/inbs/", pdf_url)
                is_target, doc_type = self._is_target_document(title)
                if not is_target:
                    continue
                data = {
                    'time': time_cell,
                    'code': code_cell,
                    'company_name': company_name,
                    'title': title,
                    'pdf_url': pdf_url,
                    'doc_type': doc_type
                }
                data_list.append(data)
            except Exception as e:
                self.logger.error(f"行の処理中にエラー: {e}")
                continue
        return data_list

    def _fetch_page(self, url: str) -> Optional[str]:
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = self.session.get(url, headers=headers, timeout=10, verify=False)
            response.encoding = 'utf-8'
            if response.status_code == 200:
                return response.text
            else:
                self.logger.error(f"ページ取得エラー: {response.status_code} - {url}")
                return None
        except Exception as e:
            self.logger.error(f"ページ取得中にエラー: {e}")
            return None

    def _rate_limit_wait(self):
        with self.rate_limit_lock:
            current_time = time.time()
            while self.rate_limit_queue and current_time - self.rate_limit_queue[0] >= 1.0:
                self.rate_limit_queue.popleft()
            if len(self.rate_limit_queue) >= 5:
                wait_time = 1.0 - (current_time - self.rate_limit_queue[0])
                if wait_time > 0:
                    time.sleep(wait_time)
                    current_time = time.time()
            self.rate_limit_queue.append(current_time)

    def _check_date_exists(self, date_str: str) -> bool:
        try:
            first_page_url = f"https://www.release.tdnet.info/inbs/I_list_001_{date_str}.html"
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            }
            response = self.session.get(first_page_url, headers=headers, timeout=10, verify=False)
            if response.status_code == 200:
                self.logger.info(f"日付 {date_str} のデータが存在します")
                return True
            else:
                self.logger.warning(f"日付 {date_str} のデータが存在しません (ステータス: {response.status_code})")
                return False
        except Exception as e:
            self.logger.error(f"日付存在チェック中にエラー: {e}")
            return False

    def scrape_page(self, page_index: int, date_str: str) -> List[Dict]:
        try:
            page_url = f"https://www.release.tdnet.info/inbs/I_list_{page_index:03d}_{date_str}.html"
            self.logger.info(f"ページ {page_index} をスクレイピング中: {page_url}")
            html_content = self._fetch_page(page_url)
            if not html_content:
                self.logger.warning(f"ページ {page_index} の取得に失敗")
                return []
            data = self._extract_data_from_html(html_content, page_url)
            self.logger.info(f"ページ {page_index} から {len(data)} 件のデータを抽出")
            return data
        except Exception as e:
            self.logger.error(f"ページ {page_index} のスクレイピング中にエラー: {e}")
            return []

    def scrape_date(self, date_str: str) -> List[Dict]:
        if not self._check_date_exists(date_str):
            self.logger.warning(f"日付 {date_str} のデータは存在しません")
            return []
        all_data = []
        page_index = 1
        while True:
            page_data = self.scrape_page(page_index, date_str)
            if not page_data:
                self.logger.info(f"ページ {page_index} にデータがないため、スクレイピングを終了")
                break
            all_data.extend(page_data)
            page_index += 1
        self.logger.info(f"日付 {date_str} のスクレイピング完了: {len(all_data)} 件のデータを取得")
        return all_data

    def _save_to_csv(self, data_list: List[Dict], date_str: str, output_dir: str):
        try:
            csv_filename = f"tdnet_data_{date_str}.csv"
            csv_path = os.path.join(output_dir, csv_filename)
            with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
                if data_list:
                    fieldnames = data_list[0].keys()
                    writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(data_list)
            self.logger.info(f"CSVファイルを保存しました: {csv_path}")
            return csv_path
        except Exception as e:
            self.logger.error(f"CSV保存エラー: {e}")
            return None

    def run(self, date_str: str = None):
        if date_str is None:
            import pytz
            jst = pytz.timezone('Asia/Tokyo')
            date_str = datetime.now(jst).strftime('%Y%m%d')
        self.logger.info(f"日付 {date_str} のスクレイピングを開始")
        data_list = self.scrape_date(date_str)
        if data_list:
            self.logger.info(f"{len(data_list)} 件のデータを取得しました")
            return data_list
        else:
            self.logger.warning("データが取得できませんでした")
            return []
