#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
業種別一括スクレイピング機能
日付範囲指定で業種・規模別ディレクトリに保存
"""

import os
import tempfile
import argparse
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
from google.cloud import storage
import json
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed

from tdnet_analyzer.scraper.tdnet_cloud import TDNetCloud
from tdnet_analyzer.common.path_utils import project_path
from tdnet_analyzer.common.constants import (
    load_company_info_map, normalize_code, normalize_size as normalize_size_const
)


def normalize_size(size):
    """規模分類を正規化（Unknown/空/-はotherに）"""
    if not size or size.strip() in ['', '-', 'Unknown', 'unknown', 'UNKNOWN']:
        return 'other'
    return size


def normalize_filename(code, company, title):
    """業種別保存用のファイル名生成"""
    return f"{code}_{company}_{title}.pdf"


class BatchScraper(TDNetCloud):
    """業種別一括スクレイピング機能"""

    def __init__(self, config_path: str | None = None, use_multithread: bool = True):
        super().__init__(config_path, use_multithread)
        # 企業情報マップ読み込み
        self.company_info_map: Dict[str, tuple[str, str, str]] = {}
        companies_csv_path = project_path('inputs', 'companies.csv')
        if companies_csv_path.exists():
            try:
                self.company_info_map = load_company_info_map(str(companies_csv_path))
                self.logger.info(f"企業情報を読み込み: {len(self.company_info_map)} 件")
            except Exception as e:
                self.logger.warning(f"企業情報の読み込みに失敗: {e}")
        else:
            self.logger.warning("企業情報ファイルが見つかりません")

    def _get_company_info(self, code: str) -> tuple[str, str, str]:
        """証券コードから企業情報を取得"""
        normalized_code = normalize_code(code)
        if normalized_code in self.company_info_map:
            name, sector, size = self.company_info_map[normalized_code]
            # 規模分類を正規化
            normalized_size = normalize_size(size)
            return name, sector, normalized_size
        return 'Unknown', 'Unknown', 'other'

    def _generate_sector_gcs_path(self, data: Dict, sector: str, size: str) -> str:
        """業種別ディレクトリ構造でのGCSパス生成"""
        company_code = data.get('code', 'unknown')
        company_name = data.get('company_name', 'Unknown')
        title = data.get('title', 'unknown')
        
        # ファイル名の安全化
        import re as _re
        safe_title = _re.sub(r'[<>:"/\\|?*]', '', title)
        safe_title = safe_title.replace(' ', '_')[:50].rstrip('_')
        safe_company_name = _re.sub(r'[<>:"/\\|?*]', '', company_name)
        safe_company_name = safe_company_name.replace(' ', '_')[:30].rstrip('_')
        
        # ファイル名: 証券コード_会社名_元ファイル名.pdf
        filename = normalize_filename(company_code, safe_company_name, safe_title)
        
        # パス: sectors/業種/規模/filename
        path_parts = [self.base_path, 'sectors', sector, size, filename]
        return "/".join(part for part in path_parts if part)

    def _process_documents_sector_batch(self, data_list: List[Dict]) -> int:
        """業種別バッチ処理でPDFを処理"""
        if not self.use_multithread:
            return self._process_documents_sector_single(data_list)
        
        self.logger.info(f"業種別マルチスレッド処理開始 (ワーカー数: {self.max_workers})")

        def process_single_document(data):
            try:
                pdf_url = data.get('pdf_url')
                if not pdf_url:
                    return False, f"PDF URLが存在しません: {data.get('title', 'Unknown')}"
                
                # 企業情報取得
                code = data.get('code', 'unknown')
                company_name, sector, size = self._get_company_info(code)
                
                # PDFダウンロード
                temp_file_path = self._download_pdf_to_temp(pdf_url)
                if not temp_file_path:
                    return False, f"PDFダウンロード失敗: {data.get('title', 'Unknown')}"
                
                try:
                    # 業種別GCSパス生成
                    gcs_path = self._generate_sector_gcs_path(data, sector, size)
                    if self._upload_to_gcs(temp_file_path, gcs_path):
                        return True, f"処理完了: {gcs_path} (業種:{sector}, 規模:{size})"
                    else:
                        return False, f"GCSアップロード失敗: {data.get('title', 'Unknown')}"
                finally:
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
            except Exception as e:
                return False, f"処理中にエラー: {e}"

        success_count = 0
        processed_count = 0
        total_count = len(data_list)
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            future_to_data = {executor.submit(process_single_document, data): data for data in data_list}
            for future in as_completed(future_to_data):
                data = future_to_data[future]
                processed_count += 1
                try:
                    success, message = future.result()
                    if success:
                        success_count += 1
                        code = data.get('code', 'unknown')
                        company_name, sector, size = self._get_company_info(code)
                        self.logger.info(
                            f"✅ [{processed_count}/{total_count}] 成功: code={code} 業種={sector} 規模={size} title=\"{data.get('title', 'Unknown')[:30]}...\""
                        )
                    else:
                        self.logger.error(
                            f"❌ [{processed_count}/{total_count}] 失敗: code={data.get('code')} title=\"{data.get('title', 'Unknown')[:30]}...\" - {message}"
                        )
                except Exception as e:
                    self.logger.error(f"💥 [{processed_count}/{total_count}] 処理中にエラー: {e}")
                
                if processed_count % 10 == 0 or processed_count == total_count:
                    progress_rate = (processed_count / total_count) * 100
                    self.logger.info(f"📊 進捗: {processed_count}/{total_count} 件完了 ({progress_rate:.1f}%) - 成功: {success_count} 件")
        
        self.logger.info(f"業種別マルチスレッド処理完了: {success_count}/{len(data_list)} 件成功")
        return success_count

    def _process_documents_sector_single(self, data_list: List[Dict]) -> int:
        """業種別シングルスレッド処理"""
        self.logger.info("業種別シングルスレッド処理開始")
        success_count = 0
        
        for data in data_list:
            try:
                pdf_url = data.get('pdf_url')
                if not pdf_url:
                    self.logger.warning(f"PDF URLが存在しません: {data.get('title', 'Unknown')}")
                    continue
                
                # 企業情報取得
                code = data.get('code', 'unknown')
                company_name, sector, size = self._get_company_info(code)
                
                # PDFダウンロード
                temp_file_path = self._download_pdf_to_temp(pdf_url)
                if not temp_file_path:
                    self.logger.error(f"PDFダウンロード失敗: {data.get('title', 'Unknown')}")
                    continue
                
                try:
                    # 業種別GCSパス生成
                    gcs_path = self._generate_sector_gcs_path(data, sector, size)
                    if self._upload_to_gcs(temp_file_path, gcs_path):
                        success_count += 1
                        self.logger.info(
                            f"成功: code={code} 業種={sector} 規模={size} title=\"{data.get('title', 'Unknown')}\" -> {gcs_path}"
                        )
                    else:
                        self.logger.error(f"GCSアップロード失敗: {data.get('title', 'Unknown')}")
                finally:
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)
            except Exception as e:
                self.logger.error(f"処理中にエラー: {e}")
        
        self.logger.info(f"業種別シングルスレッド処理完了: {success_count}/{len(data_list)} 件成功")
        return success_count

    def scrape_date_range_sector_batch(self, start_date: str, end_date: str) -> Dict[str, int]:
        """日付範囲で業種別一括スクレイピング"""
        self.logger.info(f"業種別一括スクレイピング開始: {start_date} - {end_date}")
        
        results = {}
        current_date = datetime.strptime(start_date, '%Y%m%d')
        end_date_obj = datetime.strptime(end_date, '%Y%m%d')
        
        # 各日付の全データを収集
        all_data = []
        
        while current_date <= end_date_obj:
            date_str = current_date.strftime('%Y%m%d')
            self.logger.info(f"日付 {date_str} のデータ収集中...")
            
            if not self._check_date_exists(date_str):
                self.logger.warning(f"日付 {date_str} のデータは存在しません")
                results[date_str] = 0
                current_date += timedelta(days=1)
                continue
            
            # その日のすべてのページからデータを収集
            page_data_for_date = []
            page_index = 1
            
            while True:
                page_data = self.scrape_page(page_index, date_str)
                if not page_data:
                    break
                page_data_for_date.extend(page_data)
                page_index += 1
            
            # 市場フィルタ適用
            if self.company_market_map and self.excluded_markets:
                before = len(page_data_for_date)
                
                def _is_excluded_by_market(data: Dict) -> bool:
                    code_raw = str(data.get('code', '')).strip().upper()
                    code_norm = normalize_code(code_raw)
                    market = (
                        self.company_market_map.get(code_raw)
                        or self.company_market_map.get(code_norm)
                    )
                    return market in self.excluded_markets
                
                page_data_for_date = [d for d in page_data_for_date if not _is_excluded_by_market(d)]
                excluded = before - len(page_data_for_date)
                if excluded:
                    self.logger.info(f"日付 {date_str}: 市場フィルタで {excluded}件を除外")
            
            all_data.extend(page_data_for_date)
            results[date_str] = len(page_data_for_date)
            self.logger.info(f"日付 {date_str}: {len(page_data_for_date)}件のデータを収集")
            
            current_date += timedelta(days=1)
        
        if not all_data:
            self.logger.warning("対象期間にデータがありません")
            return results
        
        self.logger.info(f"全期間のデータ収集完了: 合計 {len(all_data)} 件")
        
        # 業種別バッチ処理でPDF保存
        self.logger.info("業種別ディレクトリ構造での一括保存開始...")
        saved_count = self._process_documents_sector_batch(all_data)
        
        self.logger.info(f"業種別一括スクレイピング完了: {saved_count}/{len(all_data)} 件保存")
        
        # 結果サマリー
        total_collected = sum(results.values())
        self.logger.info(f"期間別収集サマリー:")
        for date, count in results.items():
            self.logger.info(f"  {date}: {count} 件")
        self.logger.info(f"合計収集: {total_collected} 件, 保存成功: {saved_count} 件")
        
        return results


def main():
    parser = argparse.ArgumentParser(description='業種別一括スクレイピング')
    parser.add_argument('--start-date', type=str, required=True, help='開始日付 (YYYYMMDD形式)')
    parser.add_argument('--end-date', type=str, required=True, help='終了日付 (YYYYMMDD形式)')
    parser.add_argument('--key-file', type=str, help='GCPサービスアカウントキーファイル')
    parser.add_argument('--project-id', type=str, help='GCPプロジェクトID')
    parser.add_argument('--single-thread', action='store_true', help='シングルスレッドモード')
    parser.add_argument('--config', type=str, default=str(project_path('config', 'config.yaml')), help='設定ファイルパス')
    
    args = parser.parse_args()
    
    # GCP認証設定
    if args.key_file:
        os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = args.key_file
    if args.project_id:
        os.environ['PROJECT_ID'] = args.project_id
    
    try:
        scraper = BatchScraper(args.config, use_multithread=not args.single_thread)
        results = scraper.scrape_date_range_sector_batch(args.start_date, args.end_date)
        
        total_count = sum(results.values())
        print(f"業種別一括スクレイピング完了: 合計 {total_count} 件の文書を処理")
        for date, count in results.items():
            print(f"  {date}: {count} 件")
        
        return 0
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())