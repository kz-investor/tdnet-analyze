#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日本株の適時開示サービスからデータを取得して、GCSにアップロードし、
TDnetデータ処理パイプライン用のフォルダ構造を作成
"""

import os
import tempfile
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Set
import argparse
from google.cloud import storage
import json
import pytz
from concurrent.futures import ThreadPoolExecutor, as_completed
import functions_framework
import re

import google.auth
import google.auth.transport.requests
import requests

from tdnet_base import TDNetBase
# 市場フィルタ用
try:
    from constants import load_company_market_map, EXCLUDED_MARKETS_DEFAULT, normalize_code
except Exception:
    load_company_market_map = None
    EXCLUDED_MARKETS_DEFAULT = set()

    def normalize_code(c: str) -> str:
        return c


def _extract_date_from_request(request) -> str | None:
    """HTTPリクエストから 'date' (YYYYMMDD) を抽出する。"""
    if request is None:
        return None

    # 1) JSON body
    try:
        data = request.get_json(silent=True)
        if isinstance(data, dict):
            body_date = data.get('date')
            if body_date and re.match(r'^\d{8}$', body_date):
                return body_date
    except Exception:
        pass

    # 2) query string
    try:
        if getattr(request, 'args', None):
            query_date = request.args.get('date')
            if query_date and re.match(r'^\d{8}$', query_date):
                return query_date
    except Exception:
        pass

    return None


@functions_framework.http
def scrape(request):
    """
    Cloud Functionのエントリーポイント。
    HTTPリクエストを受け取り、指定された日付のTDnetスクレイピングを実行する。
    日付が指定されない場合は、JSTの現在の日付を使用する。
    """
    try:
        date_str = _extract_date_from_request(request)
        if not date_str:
            jst = pytz.timezone('Asia/Tokyo')
            date_str = datetime.now(jst).strftime('%Y%d')
            print(f"日付が指定されていないため、現在の日付を使用します: {date_str}")

        print(f"スクレイピング処理を開始します: 日付={date_str}")

        scraper = TDNetCloud(config_path="config/config.yaml", use_multithread=True)
        count = scraper.scrape_date(date_str)

        message = f"スクレイピング処理が完了しました: {count} 件の文書を保存"
        print(message)
        return {"status": "success", "message": message, "count": count}, 200

    except Exception as e:
        import traceback
        print(f"エラーが発生しました: {e}")
        print(traceback.format_exc())
        return {"status": "error", "message": str(e)}, 500


class TDNetCloud(TDNetBase):
    """Google Cloud用の適時開示データスクレイパー"""

    def __init__(self, config_path: str = "config/config.yaml", use_multithread: bool = True):
        """初期化"""
        super().__init__(config_path, use_multithread)

        # --- GCSクライアントのカスタマイズ ---
        try:
            credentials, project_id = google.auth.default()

            # 並列数に応じて接続プールサイズをカスタマイズ
            adapter = requests.adapters.HTTPAdapter(pool_connections=self.max_workers, pool_maxsize=self.max_workers)
            session = requests.Session()
            session.mount('https://', adapter)

            authed_session = google.auth.transport.requests.AuthorizedSession(credentials, session=session)

            self.storage_client = storage.Client(project=project_id, credentials=credentials, _http=authed_session)
            self.logger.info(f"GCSクライアントをカスタマイズしました。プロジェクト: {project_id}, 接続プールサイズ: {self.max_workers}")
        except Exception as e:
            self.logger.warning(f"GCSクライアントのカスタマイズに失敗。デフォルト設定で続行します。エラー: {e}")
            # フォールバック時もプロジェクトIDを指定する
            try:
                credentials, project_id = google.auth.default()
                self.storage_client = storage.Client(project=project_id)
            except Exception:
                self.storage_client = storage.Client(project=os.environ.get("PROJECT_ID"))

        # GCSクライアントのみ使用

        # 設定からGCS設定を読み込み
        gcs_config = self.config.get('gcs', {})
        self.bucket_name = gcs_config.get('bucket_name')
        self.base_path = gcs_config.get('base_path', 'tdnet-analyzer')
        # doc_type強制（例: "tanshin"）。未設定なら分類結果を使用
        self.force_doc_type = gcs_config.get('force_doc_type')
        # フラット保存設定
        self.flat_per_day = bool(gcs_config.get('flat_per_day', False))

        if not self.bucket_name:
            raise ValueError("GCSバケット名が設定されていません")

        # スクレイピング設定の確認とログ出力
        self.logger.info("🚀 TDnet スクレイピング設定:")
        self.logger.info(f"  • GCSバケット: {self.bucket_name}")
        self.logger.info(f"  • マルチスレッド: {self.use_multithread}")
        self.logger.info(f"  • 最大ワーカー数: {self.max_workers}")
        self.logger.info(f"  • フラット保存: {self.config.get('gcs', {}).get('flat_per_day', False)}")
        self.logger.info(f"  • ベースURL: {self.config.get('data_fetch', {}).get('base_url', '未設定')}")

        # レート制限設定を動的に取得
        rate_limit_config = self.config.get('scraping', {}).get('rate_limit', {})
        max_requests = rate_limit_config.get('max_requests_per_second', 5)
        self.logger.info(f"  • レート制限: 1秒あたり最大{max_requests}リクエスト")

        # 市場フィルタの読み込み
        self.company_market_map: Dict[str, str] = {}
        self.excluded_markets: Set[str] = EXCLUDED_MARKETS_DEFAULT
        companies_csv_path = os.path.join(os.path.dirname(__file__), 'inputs', 'companies.csv')
        if load_company_market_map and os.path.exists(companies_csv_path):
            try:
                self.company_market_map = load_company_market_map(companies_csv_path)
                self.logger.info(f"市場定義を読み込み: {len(self.company_market_map)} 件")
            except Exception as e:
                self.logger.warning(f"市場定義の読み込みに失敗: {e}")
        else:
            self.logger.info("市場定義ファイルが見つからないため、フィルタは無効")
        self.logger.info(f"除外対象市場: {self.excluded_markets}")

    def _download_pdf_to_temp(self, pdf_url: str) -> Optional[str]:
        """PDFを一時ファイルにダウンロード"""
        try:
            # レート制限を適用
            self._rate_limit_wait()

            response = self.session.get(pdf_url, timeout=30, verify=False)
            if response.status_code == 200:
                # 一時ファイルを作成
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.pdf')
                temp_file.write(response.content)
                temp_file.close()
                return temp_file.name
            else:
                self.logger.error(f"PDFダウンロードエラー: {response.status_code} - {pdf_url}")
                return None
        except Exception as e:
            self.logger.error(f"PDFダウンロード中にエラー: {e}")
            return None

    def _upload_to_gcs(self, local_file_path: str, gcs_path: str) -> bool:
        """ファイルをGCSにアップロード"""
        try:
            bucket = self.storage_client.bucket(self.bucket_name)
            blob = bucket.blob(gcs_path)

            # ファイルをアップロード
            blob.upload_from_filename(local_file_path)

            self.logger.info(f"GCSアップロード完了: {gcs_path}")
            return True

        except Exception as e:
            self.logger.error(f"GCSアップロードエラー: {e}")
            return False

    def _generate_gcs_path(self, data: Dict, date_str: str) -> str:
        """Vertex AI RAG用のGCSパスを生成"""
        year = date_str[:4]
        month = date_str[4:6]
        day = date_str[6:8]

        company_code = data.get('code', 'unknown')
        doc_type = data.get('doc_type', 'other')

        title = data.get('title', 'unknown')
        # 日本語文字も保持し、ファイルシステムで問題となる文字のみ除去
        import re
        safe_title = re.sub(r'[<>:"/\\|?*]', '', title)  # ファイルシステムで禁止された文字のみ除去
        safe_title = safe_title.replace(' ', '_')[:50].rstrip('_')

        # ディレクトリは分けず、日付配下にフラット保存
        if self.flat_per_day:
            filename = f"{company_code}_{safe_title}.pdf"
            return f"{self.base_path}/{year}/{month}/{day}/{filename}"

        # 従来レイアウト（doc_type配下）
        filename = f"{company_code}_{safe_title}.pdf"
        return f"{self.base_path}/{year}/{month}/{day}/{doc_type}/{filename}"

    def _process_documents_cloud(self, data_list: List[Dict], date_str: str) -> int:
        """Cloud用の文書処理（PDFダウンロード + GCSアップロード）"""
        if not self.use_multithread:
            return self._process_documents_single_cloud(data_list, date_str)

        self.logger.info(f"マルチスレッドモードでPDF処理開始 (ワーカー数: {self.max_workers})")

        def process_single_document(data):
            """単一文書の処理"""
            try:
                pdf_url = data.get('pdf_url')
                if not pdf_url:
                    return False, f"PDF URLが存在しません: {data.get('title', 'Unknown')}"

                # PDFを一時ファイルにダウンロード
                temp_file_path = self._download_pdf_to_temp(pdf_url)
                if not temp_file_path:
                    return False, f"PDFダウンロード失敗: {data.get('title', 'Unknown')}"

                try:
                    # GCSパスを生成
                    gcs_path = self._generate_gcs_path(data, date_str)

                    # GCSにアップロード
                    if self._upload_to_gcs(temp_file_path, gcs_path):
                        return True, f"処理完了: {gcs_path}"
                    else:
                        return False, f"GCSアップロード失敗: {data.get('title', 'Unknown')}"

                finally:
                    # 一時ファイルを削除
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)

            except Exception as e:
                return False, f"処理中にエラー: {e}"

        # マルチスレッドで処理
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

                # 10件ごとに進捗率をログ出力
                if processed_count % 10 == 0 or processed_count == total_count:
                    progress_rate = (processed_count / total_count) * 100
                    self.logger.info(f"📊 進捗: {processed_count}/{total_count} 件完了 ({progress_rate:.1f}%) - 成功: {success_count} 件")

        self.logger.info(f"マルチスレッド処理完了: {success_count}/{len(data_list)} 件成功")
        return success_count

    def _process_documents_single_cloud(self, data_list: List[Dict], date_str: str) -> int:
        """シングルスレッドでの文書処理"""
        self.logger.info("シングルスレッドモードでPDF処理開始")

        success_count = 0
        for data in data_list:
            try:
                pdf_url = data.get('pdf_url')
                if not pdf_url:
                    self.logger.warning(f"PDF URLが存在しません: {data.get('title', 'Unknown')}")
                    continue

                # PDFを一時ファイルにダウンロード
                temp_file_path = self._download_pdf_to_temp(pdf_url)
                if not temp_file_path:
                    self.logger.error(f"PDFダウンロード失敗: {data.get('title', 'Unknown')}")
                    continue

                try:
                    # GCSパスを生成
                    gcs_path = self._generate_gcs_path(data, date_str)

                    # GCSにアップロード
                    if self._upload_to_gcs(temp_file_path, gcs_path):
                        success_count += 1
                        self.logger.info(
                            f"成功: code={data.get('code')} type={data.get('doc_type')} title=\"{data.get('title', 'Unknown')}\" -> {gcs_path}"
                        )
                    else:
                        self.logger.error(f"GCSアップロード失敗: {data.get('title', 'Unknown')}")

                finally:
                    # 一時ファイルを削除
                    if os.path.exists(temp_file_path):
                        os.unlink(temp_file_path)

            except Exception as e:
                self.logger.error(f"処理中にエラー: {e}")

        self.logger.info(f"シングルスレッド処理完了: {success_count}/{len(data_list)} 件成功")
        return success_count

    def _save_metadata_to_gcs(self, data_list: List[Dict], date_str: str):
        """メタデータをGCSに保存（Vertex AI RAG用）"""
        try:
            # メタデータを構造化
            metadata = {
                'date': date_str,
                'total_documents': len(data_list),
                'document_types': {},
                'companies': {},
                'documents': []
            }

            # 統計情報を集計
            for data in data_list:
                doc_type = data.get('doc_type', 'other')
                company_code = data.get('code', 'unknown')

                # 文書タイプ別カウント
                metadata['document_types'][doc_type] = metadata['document_types'].get(doc_type, 0) + 1

                # 会社別カウント
                metadata['companies'][company_code] = metadata['companies'].get(company_code, 0) + 1

                # 個別文書情報
                doc_info = {
                    'time': data.get('time'),
                    'code': data.get('code'),
                    'company_name': data.get('company_name'),
                    'title': data.get('title'),
                    'doc_type': data.get('doc_type'),
                    'gcs_path': self._generate_gcs_path(data, date_str)
                }
                metadata['documents'].append(doc_info)

            # 一時ファイルに保存
            temp_file = tempfile.NamedTemporaryFile(delete=False, suffix='.json', mode='w', encoding='utf-8')
            json.dump(metadata, temp_file, ensure_ascii=False, indent=2)
            temp_file.close()

            try:
                # GCSにアップロード
                year = date_str[:4]
                month = date_str[4:6]
                day = date_str[6:8]
                gcs_path = f"{self.base_path}/{year}/{month}/{day}/metadata_{date_str}.json"

                if self._upload_to_gcs(temp_file.name, gcs_path):
                    self.logger.info(f"メタデータをGCSに保存しました: {gcs_path}")
                else:
                    self.logger.error("メタデータのGCS保存に失敗")

            finally:
                # 一時ファイルを削除
                if os.path.exists(temp_file.name):
                    os.unlink(temp_file.name)

        except Exception as e:
            self.logger.error(f"メタデータ保存中にエラー: {e}")

    def scrape_date(self, date_str: str) -> int:
        """指定日付のデータをスクレイピングしてGCSに保存（メモリ効率化版）"""
        if not self._check_date_exists(date_str):
            self.logger.warning(f"日付 {date_str} のデータは存在しません")
            return 0

        total_saved = 0
        page_index = 1
        batch_size = 50  # 50件ずつバッチ処理

        while True:
            page_data = self.scrape_page(page_index, date_str)
            if not page_data:
                self.logger.info(f"ページ {page_index} にデータがないため、スクレイピングを終了")
                break

            self.logger.info(f"市場フィルタ適用前: {len(page_data)}件の文書")

            # 市場フィルタを適用
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

                # GCSへのアップロード直前にログを追加
                self.logger.info(f"GCSアップロード対象件数: {len(page_data)}件")

            # バッチサイズに達したら処理
            if len(page_data) >= batch_size:
                self.logger.info(f"ページ {page_index}: {len(page_data)}件をバッチ処理中...")
                saved_count = self._process_documents_cloud(page_data, date_str)
                total_saved += saved_count
                self.logger.info(f"ページ {page_index} 完了: {saved_count}件保存")

                # メモリクリア
                page_data.clear()

            page_index += 1

        # 残りのデータを処理
        if page_data:
            self.logger.info(f"最終バッチ: {len(page_data)}件を処理中...")
            saved_count = self._process_documents_cloud(page_data, date_str)
            total_saved += saved_count
            self.logger.info(f"最終バッチ完了: {saved_count}件保存")

        if total_saved > 0:
            self.logger.info(f"合計 {total_saved} 件の文書をGCSに保存しました")
            # メタデータは最後にまとめて保存
            self._save_metadata_to_gcs(self._get_all_metadata(date_str), date_str)
            self.logger.info(f"完了: 日付 {date_str}, 保存件数: {total_saved}")
            return total_saved
        else:
            self.logger.warning("取得したデータがありません")
            self.logger.info(f"完了: 日付 {date_str}, 保存件数: 0")
            return 0

    def _get_all_metadata(self, date_str: str) -> List[Dict]:
        """指定日付の全データを再取得してメタデータ用に使用"""
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

    def run_date_range(self, start_date: str, end_date: str) -> Dict[str, int]:
        """日付範囲で実行"""
        start_dt = datetime.strptime(start_date, '%Y%m%d')
        end_dt = datetime.strptime(end_date, '%Y%m%d')

        results = {}
        current_dt = start_dt

        while current_dt <= end_dt:
            date_str = current_dt.strftime('%Y%m%d')
            self.logger.info(f"🎯 日付 {date_str} の処理を開始")
            self.logger.info(f"  • 対象URL: https://www.release.tdnet.info/inbs/I_list_001_{date_str}.html")
            self.logger.info(f"  • 処理予定: ページ順次取得 -> 市場フィルタ -> PDF取得・アップロード")

            try:
                count = self.scrape_date(date_str)
                results[date_str] = count
            except Exception as e:
                self.logger.error(f"日付 {date_str} の処理中にエラー: {e}")
                results[date_str] = -1

            current_dt += timedelta(days=1)

        return results


def main():
    """メイン関数"""
    parser = argparse.ArgumentParser(description='TDnet Cloud Scraper')
    parser.add_argument('--date', type=str, help='対象日付 (YYYYMMDD形式)')
    parser.add_argument('--start-date', type=str, help='開始日付 (YYYYMMDD形式)')
    parser.add_argument('--end-date', type=str, help='終了日付 (YYYYMMDD形式)')
    parser.add_argument('--single-thread', action='store_true', help='シングルスレッドモード')
    parser.add_argument('--config', type=str, default='config/config.yaml', help='設定ファイルパス')

    args = parser.parse_args()

    # 日付指定の検証
    if args.date and (args.start_date or args.end_date):
        print("エラー: --date と --start-date/--end-date は同時に指定できません")
        return 1

    if args.start_date and not args.end_date:
        print("エラー: --start-date を指定する場合は --end-date も必要です")
        return 1

    if args.end_date and not args.start_date:
        print("エラー: --end-date を指定する場合は --start-date も必要です")
        return 1

    try:
        scraper = TDNetCloud(args.config, use_multithread=not args.single_thread)

        if args.date:
            # 単一日付の処理
            count = scraper.scrape_date(args.date)
            print(f"処理完了: {count} 件の文書を保存")

        elif args.start_date and args.end_date:
            # 日付範囲の処理
            results = scraper.run_date_range(args.start_date, args.end_date)
            total_count = sum(count for count in results.values() if count > 0)
            print(f"範囲処理完了: 合計 {total_count} 件の文書を保存")
            for date, count in results.items():
                print(f"  {date}: {count} 件")
        else:
            # 環境変数から日付を取得、なければ今日の日付
            import os
            target_date = os.environ.get('TARGET_DATE')

            if target_date:
                print(f"環境変数から日付({target_date})を取得して実行します")
                count = scraper.scrape_date(target_date)
                print(f"処理完了: {count} 件の文書を保存")
            else:
                # 今日の日付で自動実行
                import pytz
                jst = pytz.timezone('Asia/Tokyo')
                today = datetime.now(jst).strftime('%Y%m%d')
                print(f"今日の日付({today})で自動実行します")
                count = scraper.scrape_date(today)
                print(f"処理完了: {count} 件の文書を保存")

    except Exception as e:
        print(f"エラーが発生しました: {e}")
        import traceback
        traceback.print_exc()
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
