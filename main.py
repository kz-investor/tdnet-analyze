#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Cloud Functions - TDnet Scraper
Cloud Schedulerから呼び出されてtdnet_cloud.pyを直接実行
"""

import functions_framework
import os
import sys
from datetime import datetime
import pytz
import subprocess
import shlex
import tempfile
import shutil
import re
from urllib.parse import urlparse, parse_qs

# tdnet_cloud.pyのパスを設定
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
TDNET_CLOUD_PATH = os.path.join(SCRIPT_DIR, 'tdnet_cloud.py')
GENERATE_SUMMARY_PATH = os.path.join(SCRIPT_DIR, 'generate_summary.py')  # パスを追加
GENERATE_SECTOR_INSIGHTS_PATH = os.path.join(SCRIPT_DIR, 'generate_sector_insights.py')  # パスを修正

DATE_RE = re.compile(r"^\d{8}$")


def _extract_date_from_request(request) -> str | None:
    # 1) query
    try:
        if request and getattr(request, 'args', None):
            qd = request.args
            date_q = qd.get('date')
            if date_q and DATE_RE.match(date_q):
                return date_q
    except Exception:
        pass
    # 2) json body
    try:
        data = request.get_json(silent=True)
        if isinstance(data, dict):
            body_date = data.get('date')
            if body_date and DATE_RE.match(body_date):
                return body_date
    except Exception:
        pass
    # 3) form urlencoded
    try:
        if getattr(request, 'form', None):
            form_date = request.form.get('date')
            if form_date and DATE_RE.match(form_date):
                return form_date
    except Exception:
        pass
    # 4) raw body fallback
    try:
        raw = request.get_data(as_text=True) or ''
        m = re.search(r"date=(\d{8})", raw)
        if m and DATE_RE.match(m.group(1)):
            return m.group(1)
    except Exception:
        pass
    # 5) header override
    try:
        hdr_date = request.headers.get('X-Date')
        if hdr_date and DATE_RE.match(hdr_date):
            return hdr_date
    except Exception:
        pass
    # 6) URL full path parse（保険）
    try:
        full_url = request.url or ''
        qs = parse_qs(urlparse(full_url).query)
        url_date = (qs.get('date') or [None])[0]
        if url_date and DATE_RE.match(url_date):
            return url_date
    except Exception:
        pass
    return None


def _extract_task_and_opts(request) -> tuple[str, dict]:
    """task: 'scrape'|'insights'|'summary' を解釈し、model/location/project等の任意引数を返す"""
    task = 'scrape'
    opts: dict = {}
    try:
        # query優先
        if request and getattr(request, 'args', None):
            qd = request.args
            if qd.get('task'):
                task = qd.get('task')
            for k in ('model', 'location', 'project', 'include', 'codes', 'max_files'):
                v = qd.get(k)
                if v:
                    opts[k] = v
        # body補完
        data = request.get_json(silent=True)
        if isinstance(data, dict):
            task = data.get('task', task)
            for k in ('model', 'location', 'project', 'include', 'codes', 'max_files'):
                if data.get(k) is not None:
                    opts[k] = data.get(k)
    except Exception:
        pass
    # 既定のproject/location
    opts.setdefault('project', os.environ.get('PROJECT_ID'))
    opts.setdefault('location', os.environ.get('VERTEX_LOCATION', 'us-central1'))  # LLMは us-central1 固定
    return task, opts


@functions_framework.http
def trigger_scraper(request):
    """Cloud Scheduler/HTTPから呼び出され、指定日付で処理を実行する。
    task: scrape(既定)/summary/insights を受け付ける。
    JSON/クエリ/フォーム/ヘッダから {date: YYYYMMDD}、任意で{model, location, project, include, codes, max_files} を解釈。
    """
    import time
    start_time = time.time()

    try:
        # Cloud Functions環境情報を出力
        print("🚀 Cloud Function 実行開始")
        print(f"  • FUNCTION_NAME: {os.environ.get('FUNCTION_NAME', '未設定')}")
        print(f"  • PROJECT_ID: {os.environ.get('PROJECT_ID', '未設定')}")
        print(f"  • REGION: {os.environ.get('REGION', '未設定')}")

        # リクエスト情報を詳細出力
        print(f"  • Method: {request.method}")
        print(f"  • Content-Type: {request.headers.get('Content-Type', '未設定')}")

        date_str = _extract_date_from_request(request)
        if not date_str:
            jst = pytz.timezone('Asia/Tokyo')
            date_str = datetime.now(jst).strftime('%Y%m%d')
            print(f"  • 日付自動設定: {date_str} (JST)")
        else:
            print(f"  • 指定日付: {date_str}")

        task, opts = _extract_task_and_opts(request)
        print(f"📋 実行タスク: {task}")
        if opts:
            print(f"  • オプション: {opts}")

        print(f"⏰ 開始: task={task} date={date_str}")

        if task == 'scrape':
            # スクレイピング実行
            cmd = [
                sys.executable,
                TDNET_CLOUD_PATH,
                '--date', date_str,
            ]
        elif task == 'summary':
            # 個別サマリー生成
            cmd = [
                sys.executable,
                GENERATE_SUMMARY_PATH,
                '--date', date_str,
                '--bucket', os.environ.get('TDNET_BUCKET', 'tdnet-documents'),
                '--base', os.environ.get('TDNET_BASE', 'vertex-ai-rag')
            ]
            if opts.get('project'):
                cmd += ['--project', opts['project']]
            if opts.get('location'):
                cmd += ['--location', opts['location']]
            if opts.get('model'):
                cmd += ['--model', opts['model']]
            if opts.get('include'):
                cmd += ['--include', opts['include']]
            if opts.get('codes'):
                cmd += ['--codes', opts['codes']]
            if opts.get('max_files'):
                cmd += ['--max-files', opts['max_files']]
        elif task == 'insights':
            # 業種インサイト生成
            cmd = [
                sys.executable,
                GENERATE_SECTOR_INSIGHTS_PATH,
                '--date', date_str,
                '--bucket', os.environ.get('TDNET_BUCKET', 'tdnet-documents'),
                '--base', os.environ.get('TDNET_BASE', 'vertex-ai-rag')
            ]
            if opts.get('project'):
                cmd += ['--project', opts['project']]
            if opts.get('location'):
                cmd += ['--location', opts['location']]
            if opts.get('model'):
                cmd += ['--model', opts['model']]

        else:
            print(f"❌ 不明なタスク: {task}")
            return f"Error: Unknown task '{task}'", 400

        print(f"🔧 実行コマンド: {' '.join(shlex.quote(c) for c in cmd)}")
        print(f"🏃 サブプロセス実行開始...")

        subprocess_start = time.time()

        # サブプロセス実行
        proc = subprocess.run(cmd, capture_output=True, text=True, check=False)

        subprocess_duration = time.time() - subprocess_start
        print(f"⏱️  サブプロセス実行時間: {subprocess_duration:.2f}秒")

        # 標準出力と標準エラーをログに出力
        print("📝 --- Subprocess STDOUT ---")
        if proc.stdout.strip():
            print(proc.stdout)
        else:
            print("  (標準出力なし)")

        print("🚨 --- Subprocess STDERR ---")
        if proc.stderr.strip():
            print(proc.stderr)
        else:
            print("  (標準エラーなし)")
        print("------------------------")

        if proc.returncode != 0:
            total_duration = time.time() - start_time
            print(f"❌ 失敗: task={task} date={date_str} でサブプロセスがエラー終了")
            print(f"   • 終了コード: {proc.returncode}")
            print(f"   • 実行時間: {total_duration:.2f}秒")
            return f"Error executing task '{task}'. See logs for details.", 500

        total_duration = time.time() - start_time
        print(f"✅ 完了: task={task} date={date_str}")
        print(f"   • 総実行時間: {total_duration:.2f}秒")
        print(f"   • サブプロセス実行時間: {subprocess_duration:.2f}秒")
        return f"Successfully completed task: {task} for date: {date_str}", 200

    except Exception as e:
        import traceback
        total_duration = time.time() - start_time
        print(f"💥 エラー: 予期せぬエラーが発生しました")
        print(f"   • エラー内容: {e}")
        print(f"   • 実行時間: {total_duration:.2f}秒")
        print(f"   • スタックトレース:")
        print(traceback.format_exc())
        return "Internal Server Error", 500
