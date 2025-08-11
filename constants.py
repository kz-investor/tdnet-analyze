#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import csv
from typing import Dict, Set, List
import sys

# 拡張した除外候補（必要に応じて調整）
EXCLUDED_MARKETS_DEFAULT: Set[str] = {
    "ETF・ETN",
    "PRO Market",
    "REIT・ベンチャーファンド・カントリーファンド・インフラファンド",
    "出資証券",
    # 外国株式（不要であれば除外）
    "プライム（外国株式）",
    "スタンダード（外国株式）",
    "グロース（外国株式）",
}

HEADER = [
    "date", "code", "name", "market", "code_33", "sector_33",
    "code_17", "sector_17", "size_code", "size"
]

# '銘柄名', '市場・商品区分', '33業種コード', '33業種区分', '17業種コード', '17業種区分', '規模コード', '規模区分'
CSV_COLUMN_CODE = "コード"
CSV_COLUMN_NAME = "銘柄名"
CSV_COLUMN_MARKET = "市場・商品区分"
CSV_COLUMN_SECTOR = "33業種区分"
CSV_COLUMN_SIZE = "規模区分"


def normalize_code(code: str) -> str:
    """TDnetやファイル名で5桁(末尾0)になるケースを4桁に寄せるなど、照合用に正規化。
    - 5桁かつ末尾が'0'なら末尾0を削除（例: 64030 -> 6403）
    - 英字含むコードは大文字に統一（例: 130a -> 130A）
    - 前後空白を除去
    """
    if not code:
        return code
    c = code.strip().upper()
    # 5桁かつ末尾が'0'なら末尾0を削除
    if len(c) == 5 and c.endswith('0') and c[:-1].isdigit():
        return c[:-1]
    # 5桁で末尾が数字なら末尾1桁を削除（例: 13264 -> 1326）
    if len(c) == 5 and c.isdigit():  # ここを修正: c.isdigit()で数字のみか確認
        return c[:-1]
    return c


def load_company_market_map(path: str) -> Dict[str, str]:
    """企業コードと市場区分のマップをCSVから読み込む"""
    mapping: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get(CSV_COLUMN_CODE)
                market = row.get(CSV_COLUMN_MARKET)
                if code and market:
                    mapping[normalize_code(code)] = market
    except FileNotFoundError:
        print(f"警告: ファイルが見つかりません: {path}", file=sys.stderr)
    return mapping


def load_company_sector_map(path: str) -> Dict[str, str]:
    """企業コードと33業種区分のマップをCSVから読み込む"""
    mapping: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get(CSV_COLUMN_CODE)
                sector = row.get(CSV_COLUMN_SECTOR)
                if code and sector:
                    mapping[normalize_code(code)] = sector
    except FileNotFoundError:
        print(f"警告: ファイルが見つかりません: {path}", file=sys.stderr)
    return mapping


def normalize_size(size: str) -> str:
    """規模区分を短縮形に正規化"""
    if not size or size == '-':
        return 'Unknown'

    # TOPIX系の短縮
    if size.startswith('TOPIX '):
        return size.replace('TOPIX ', '')

    return size


def load_company_size_map(path: str) -> Dict[str, str]:
    """企業コードと規模区分のマップをCSVから読み込む"""
    mapping: Dict[str, str] = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get(CSV_COLUMN_CODE)
                size = row.get(CSV_COLUMN_SIZE)
                if code:
                    # 規模区分が空や'-'の場合はUnknownにする
                    normalized_size = normalize_size(size) if size else 'Unknown'
                    mapping[normalize_code(code)] = normalized_size
    except FileNotFoundError:
        print(f"警告: ファイルが見つかりません: {path}", file=sys.stderr)
    return mapping


def load_company_sector_size_map(path: str) -> Dict[str, tuple[str, str]]:
    """企業コードと(33業種区分, 規模区分)のタプルマップをCSVから読み込む"""
    mapping: Dict[str, tuple[str, str]] = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get(CSV_COLUMN_CODE)
                sector = row.get(CSV_COLUMN_SECTOR)
                size = row.get(CSV_COLUMN_SIZE)
                if code:
                    # セクターが空の場合はUnknown、規模区分も正規化
                    normalized_sector = sector if sector else 'Unknown'
                    normalized_size = normalize_size(size) if size else 'Unknown'
                    mapping[normalize_code(code)] = (normalized_sector, normalized_size)
    except FileNotFoundError:
        print(f"警告: ファイルが見つかりません: {path}", file=sys.stderr)
    return mapping


def load_company_info_map(path: str) -> Dict[str, tuple[str, str, str]]:
    """企業コードと(企業名, 33業種区分, 規模区分)のタプルマップをCSVから読み込む"""
    mapping: Dict[str, tuple[str, str, str]] = {}
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                code = row.get(CSV_COLUMN_CODE)
                name = row.get(CSV_COLUMN_NAME)
                sector = row.get(CSV_COLUMN_SECTOR)
                size = row.get(CSV_COLUMN_SIZE)
                if code:
                    # 各値の正規化
                    normalized_name = name if name else 'Unknown'
                    normalized_sector = sector if sector else 'Unknown'
                    normalized_size = normalize_size(size) if size else 'Unknown'
                    mapping[normalize_code(code)] = (normalized_name, normalized_sector, normalized_size)
    except FileNotFoundError:
        print(f"警告: ファイルが見つかりません: {path}", file=sys.stderr)
    return mapping


def list_unique_markets(csv_path: str) -> List[str]:
    mapping = load_company_market_map(csv_path)
    uniques = sorted(set(mapping.values()))
    return uniques
