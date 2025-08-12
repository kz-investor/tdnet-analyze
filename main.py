#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Functions - TDnet Scraper
Cloud Schedulerから呼び出されてパッケージ内のクラウドスクレイパーを実行
"""
import functions_framework
import os
import sys
from datetime import datetime
import pytz
import subprocess
import shlex


@functions_framework.http
def trigger_scraper(request):
    """
    Cloud Scheduler/HTTPから呼び出され、日次のTDnetスクレイピングを実行する。
    リクエストで日付が指定されていない場合は、JSTの今日の日付を使用する。
    """
    try:
        # POST(JSON) と GET(query) の両対応
        date_str = None
        request_json = request.get_json(silent=True)
        if request_json and 'date' in request_json:
            date_str = request_json['date']
        elif request.args and 'date' in request.args:
            date_str = request.args.get('date')

        if not date_str:
            jst = pytz.timezone('Asia/Tokyo')
            date_str = datetime.now(jst).strftime('%Y%m%d')
            print(f"INFO: 日付が指定されていないため、現在の日付（JST）を使用します: {date_str}")
        else:
            print(f"INFO: 指定された日付で実行します: {date_str}")

        # パッケージモジュールとして実行（ENTRYPOINT python3 を前提）
        cmd = [
            sys.executable, '-u', '-m',
            'tdnet_analyzer.scraper.tdnet_cloud',
            '--date', date_str,
        ]

        print(f"INFO: 実行コマンド: {' '.join(shlex.quote(c) for c in cmd)}")
        print("INFO: スクレイピング処理を開始します...")

        result = subprocess.run(cmd, capture_output=True, text=True, check=False)

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
