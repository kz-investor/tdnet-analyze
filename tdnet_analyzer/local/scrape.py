#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ローカル版スクレイピング機能
既存クラウド版と同じ仕様でローカルファイルシステムに保存
"""

import os
import tempfile
import argparse
import json
import shutil
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Set

from tdnet_analyzer.scraper.tdnet_base import TDNetBase
from tdnet_analyzer.common.path_utils import project_path
from tdnet_analyzer.common.constants import (
    load_company_market_map, EXCLUDED_MARKETS_DEFAULT, normalize_code
)


class TDNetLocal(TDNetBase):
    """ローカル版の適時開示データスクレイパー"""

    def __init__(self, config_path: str | None = None, use_multithread: bool = True, out_root: str = "./local_store"):
        super().__init__(config_path, use_multithread)
        
        # ローカル保存設定
        self.out_root = Path(out_root)
        self.out_root.mkdir(parents=True, exist_ok=True)
        
        # 市場フィルタの読み込み
        self.company_market_map: Dict[str, str] = {}
        self.excluded_markets: Set[str] = EXCLUDED_MARKETS_DEFAULT
        companies_csv_path = project_path('inputs', 'companies.csv')
        if companies_csv_path.exists():
            try:
                self.company_market_map = load_company_market_map(str(companies_csv_path))
                self.logger.info(f"市場定義を読み込み: {len(self.company_market_map)} 件")
            except Exception as e:
                self.logger.warning(f"市場定義の読み込みに失敗: {e}")
        else:
            self.logger.warning("市場定義ファイルが見つからないため、フィルタは無効")
        
        self.logger.info(f"除外対象市場: {self.excluded_markets}")
        self.logger.info(f"ローカル保存先: {self.out_root}")

    def _download_pdf_to_local(self, pdf_url: str, local_path: str) -> bool:
        """PDFをローカルファイルに直接ダウンロード"""
        try:
            self._rate_limit_wait()
            response = self.session.get(pdf_url, timeout=30, verify=False)
            if response.status_code == 200:
                # ディレクトリが存在しない場合は作成
                os.makedirs(os.path.dirname(local_path), exist_ok=True)
                with open(local_path, 'wb') as f:
                    f.write(response.content)
                self.logger.info(f"ローカル保存完了: {local_path}")
                return True
            else:
                self.logger.error(f"PDFダウンロードエラー: {response.status_code} - {pdf_url}")
                return False
        except Exception as e:
            self.logger.error(f"PDFダウンロード中にエラー: {e}")
            return False

    def _generate_local_path(self, data: Dict, date_str: str) -> str:
        """ローカル保存パス生成（既存クラウド版と同じ構造）"""
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]
        
        # 元のファイル名を使用（既存クラウド版と同じ）
        title = data.get('title', 'unknown')
        import re as _re
        safe_title = _re.sub(r'[<>:"/\\|?*]', '', title)
        safe_title = safe_title.replace(' ', '_')[:100].rstrip('_')
        filename = f"{safe_title}.pdf"
        
        # パス構造: out_root/YYYY/MM/DD/filename
        local_path = self.out_root / year / month / day / filename
        return str(local_path)

    def _process_documents_local(self, data_list: List[Dict], date_str: str) -> int:
        """ローカルモードでPDF処理"""
        if not self.use_multithread:
            return self._process_documents_single_local(data_list, date_str)
        
        self.logger.info(f"ローカルマルチスレッドモードでPDF処理開始 (ワーカー数: {self.max_workers})")
        
        from concurrent.futures import ThreadPoolExecutor, as_completed
        
        def process_single_document(data):
            try:
                pdf_url = data.get('pdf_url')
                if not pdf_url:
                    return False, f"PDF URLが存在しません: {data.get('title', 'Unknown')}"
                
                local_path = self._generate_local_path(data, date_str)
                if self._download_pdf_to_local(pdf_url, local_path):
                    return True, f"処理完了: {local_path}"
                else:
                    return False, f"ローカル保存失敗: {data.get('title', 'Unknown')}"
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
                        self.logger.info(
                            f"✅ [{processed_count}/{total_count}] 成功: code={data.get('code')} type={data.get('doc_type')} title=\"{data.get('title', 'Unknown')[:50]}...\""
                        )
                    else:
                        self.logger.error(
                            f"❌ [{processed_count}/{total_count}] 失敗: code={data.get('code')} title=\"{data.get('title', 'Unknown')[:50]}...\" - {message}"
                        )
                except Exception as e:
                    self.logger.error(f"💥 [{processed_count}/{total_count}] 処理中にエラー: {e}")
                
                if processed_count % 10 == 0 or processed_count == total_count:
                    progress_rate = (processed_count / total_count) * 100
                    self.logger.info(f"📊 進捗: {processed_count}/{total_count} 件完了 ({progress_rate:.1f}%) - 成功: {success_count} 件")
        
        self.logger.info(f"ローカルマルチスレッド処理完了: {success_count}/{len(data_list)} 件成功")
        return success_count

    def _process_documents_single_local(self, data_list: List[Dict], date_str: str) -> int:
        """ローカルシングルスレッド処理"""
        self.logger.info("ローカルシングルスレッド処理開始")
        success_count = 0
        
        for data in data_list:
            try:
                pdf_url = data.get('pdf_url')
                if not pdf_url:
                    self.logger.warning(f"PDF URLが存在しません: {data.get('title', 'Unknown')}")
                    continue
                
                local_path = self._generate_local_path(data, date_str)
                if self._download_pdf_to_local(pdf_url, local_path):
                    success_count += 1
                    self.logger.info(
                        f"成功: code={data.get('code')} type={data.get('doc_type')} title=\"{data.get('title', 'Unknown')}\" -> {local_path}"
                    )
                else:
                    self.logger.error(f"ローカル保存失敗: {data.get('title', 'Unknown')}")
            except Exception as e:
                self.logger.error(f"処理中にエラー: {e}")
        
        self.logger.info(f"ローカルシングルスレッド処理完了: {success_count}/{len(data_list)} 件成功")
        return success_count

    def _save_metadata_to_local(self, data_list: List[Dict], date_str: str):
        """メタデータをローカルファイルに保存（既存クラウド版と同じ形式）"""
        try:
            year = date_str[:4]
            month = date_str[4:6]
            day = date_str[6:8]
            
            metadata = {
                'date': date_str,
                'total_documents': len(data_list),
                'document_types': {},
                'companies': {},
                'documents': []
            }
            
            for data in data_list:
                doc_type = data.get('doc_type', 'other')
                company_code = data.get('code', 'unknown')
                metadata['document_types'][doc_type] = metadata['document_types'].get(doc_type, 0) + 1
                metadata['companies'][company_code] = metadata['companies'].get(company_code, 0) + 1
                
                doc_info = {
                    'time': data.get('time'),
                    'code': data.get('code'),
                    'company_name': data.get('company_name'),
                    'title': data.get('title'),
                    'doc_type': data.get('doc_type'),
                    'local_path': self._generate_local_path(data, date_str)
                }
                metadata['documents'].append(doc_info)
            
            # メタデータファイル保存
            metadata_dir = self.out_root / year / month / day
            metadata_dir.mkdir(parents=True, exist_ok=True)
            metadata_path = metadata_dir / f"metadata_{date_str}.json"
            
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"メタデータをローカルに保存しました: {metadata_path}")
            
        except Exception as e:
            self.logger.error(f"メタデータ保存中にエラー: {e}")

    def scrape_date(self, date_str: str) -> int:
        """日付指定スクレイピング（既存クラウド版と同じロジック）"""
        if not self._check_date_exists(date_str):
            self.logger.warning(f"日付 {date_str} のデータは存在しません")
            return 0
        
        total_saved = 0
        page_index = 1
        batch_size = 50
        
        while True:
            page_data = self.scrape_page(page_index, date_str)
            if not page_data:
                self.logger.info(f"ページ {page_index} にデータがないため、スクレイピングを終了")
                break
            
            self.logger.info(f"市場フィルタ適用前: {len(page_data)}件の文書")
            
            # 市場フィルタ適用
            if self.company_market_map and self.excluded_markets:
                before = len(page_data)

                def _is_excluded_by_market(data: Dict) -> bool:
                    code_raw = str(data.get('code', '')).strip().upper()
                    code_norm = normalize_code(code_raw)
                    market = (
                        self.company_market_map.get(code_raw)
                        or self.company_market_map.get(code_norm)
                    )
                    is_excluded = market in self.excluded_markets
                    if is_excluded:
                        self.logger.info(f"  除外対象: コード={code_raw} (正規化後={code_norm}), 市場={market}")
                    else:
                        self.logger.debug(f"  除外対象外: コード={code_raw} (正規化後={code_norm}), 市場={market}")
                    return is_excluded
                
                page_data = [d for d in page_data if not _is_excluded_by_market(d)]
                excluded = before - len(page_data)
                if excluded:
                    self.logger.info(f"市場フィルタ: {excluded}件を除外（対象市場: {', '.join(sorted(self.excluded_markets))}）")
                self.logger.info(f"ローカル保存対象件数: {len(page_data)}件")
            
            if len(page_data) >= batch_size:
                self.logger.info(f"ページ {page_index}: {len(page_data)}件をバッチ処理中...")
                saved_count = self._process_documents_local(page_data, date_str)
                total_saved += saved_count
                self.logger.info(f"ページ {page_index} 完了: {saved_count}件保存")
                page_data.clear()
            
            page_index += 1
        
        if page_data:
            self.logger.info(f"最終バッチ: {len(page_data)}件を処理中...")
            saved_count = self._process_documents_local(page_data, date_str)
            total_saved += saved_count
            self.logger.info(f"最終バッチ完了: {saved_count}件保存")
        
        if total_saved > 0:
            self.logger.info(f"合計 {total_saved} 件の文書をローカルに保存しました")
            # メタデータ保存
            self._save_metadata_to_local(self._get_all_metadata(date_str), date_str)
            self.logger.info(f"完了: 日付 {date_str}, 保存件数: {total_saved}")
            return total_saved
        else:
            self.logger.warning("取得したデータがありません")
            self.logger.info(f"完了: 日付 {date_str}, 保存件数: 0")
            return 0

    def _get_all_metadata(self, date_str: str) -> List[Dict]:
        """メタデータ用にデータを再取得（既存クラウド版と同じ）"""
        self.logger.info("メタデータ用にデータを再取得中...")
        all_data = []
        page_index = 1
        
        while True:
            page_data = self.scrape_page(page_index, date_str)
            if not page_data:
                break
            all_data.extend(page_data)
            page_index += 1
        
        return all_data


def main():
    parser = argparse.ArgumentParser(description='ローカル版TDnetスクレイピング')
    parser.add_argument('--date', type=str, required=True, help='対象日付 (YYYYMMDD形式)')
    parser.add_argument('--out-root', type=str, default='./local_store', help='ローカル保存先ディレクトリ')
    parser.add_argument('--single-thread', action='store_true', help='シングルスレッドモード')
    parser.add_argument('--config', type=str, default=str(project_path('config', 'config.yaml')), help='設定ファイルパス')
    
    args = parser.parse_args()
    
    try:
        scraper = TDNetLocal(args.config, use_multithread=not args.single_thread, out_root=args.out_root)
        count = scraper.scrape_date(args.date)
        print(f"ローカルスクレイピング完了: {count} 件の文書を保存")
        return 0
    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit(main())