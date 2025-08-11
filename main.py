#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Functions - TDnet Scraper
Cloud Schedulerから呼び出されてtdnet_cloud.pyを実行する
"""
import functions_framework
import os
import sys
from datetime import datetime
import pytz
import subprocess
import shlex

# tdnet_cloud.pyのパスを設定
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TDNET_CLOUD_PATH = os.path.join(SCRIPT_DIR, 'tdnet_cloud.py')


@functions_framework.http
def trigger_scraper(request):
    """
    Cloud Scheduler/HTTPから呼び出され、日次のTDnetスクレイピングを実行する。
    リクエストで日付が指定されていない場合は、JSTの今日の日付を使用する。
    """
    try:
        # 日付の決定
        date_str = request.args.get('date') if request.args else None
        if not date_str:
            jst = pytz.timezone('Asia/Tokyo')
            date_str = datetime.now(jst).strftime('%Y%m%d')
            print(f"INFO: 日付が指定されていないため、現在の日付（JST）を使用します: {date_str}")
        else:
            print(f"INFO: 指定された日付で実行します: {date_str}")

        # 実行コマンドの構築
        cmd = [
            sys.executable, '-u',
            TDNET_CLOUD_PATH,
            '--date', date_str,
        ]

        print(f"INFO: 実行コマンド: {' '.join(shlex.quote(c) for c in cmd)}")
        print("INFO: スクレイピング処理を開始します...")

        # サブプロセスとしてスクリプトを実行
        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

        # 標準出力と標準エラーをログに出力
        if result.stdout:
            print("--- STDOUT ---")
            print(result.stdout)
        if result.stderr:
            print("--- STDERR ---", file=sys.stderr)
            print(result.stderr, file=sys.stderr)

        if result.returncode != 0:
            print(f"ERROR: スクレイピング処理がエラーで終了しました。リターンコード: {result.returncode}", file=sys.stderr)
            return "Scraping process failed. See logs for details.", 500

        print("INFO: スクレイピング処理が正常に完了しました。")
        return "Scraping process completed successfully.", 200

    except Exception as e:
        print(f"FATAL: 予期せぬエラーが発生しました: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc(file=sys.stderr)
        return "Internal Server Error", 500
