#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
import argparse
import os
from google.cloud import storage

DEFAULT_BUCKET = os.environ.get("TDNET_BUCKET", "tdnet-documents")
DEFAULT_BASE = os.environ.get("TDNET_BASE", "vertex-ai-rag")


def list_blobs_for_date(client: storage.Client, bucket_name: str, base_path: str, date_str: str):
    year, month, day = date_str[:4], date_str[4:6], date_str[6:8]
    prefix = f"{base_path}/{year}/{month}/{day}/"
    bucket = client.bucket(bucket_name)
    return bucket.list_blobs(prefix=prefix)


def download_for_date(date_str: str, dest_dir: str, bucket_name: str = DEFAULT_BUCKET, base_path: str = DEFAULT_BASE) -> list[str]:
    os.makedirs(dest_dir, exist_ok=True)
    client = storage.Client()
    saved = []
    for blob in list_blobs_for_date(client, bucket_name, base_path, date_str):
        rel_path = blob.name.split(base_path + "/", 1)[-1]
        local_path = os.path.join(dest_dir, rel_path)
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        blob.download_to_filename(local_path)
        saved.append(local_path)
    return saved


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="YYYYMMDD")
    ap.add_argument("--out", default="downloads", help="local output dir")
    ap.add_argument("--bucket", default=DEFAULT_BUCKET)
    ap.add_argument("--base", default=DEFAULT_BASE)
    args = ap.parse_args()

    files = download_for_date(args.date, args.out, args.bucket, args.base)
    print(f"downloaded {len(files)} files:")
    for p in files:
        print(p)


if __name__ == "__main__":
    main()
