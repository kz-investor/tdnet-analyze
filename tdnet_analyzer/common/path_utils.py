#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
from pathlib import Path


def get_project_root(start: Path | None = None) -> Path:
    """Return the repository root by searching upwards for known markers.
    Prefers the first parent that contains 'config/config.yaml'.
    Fallback to two levels up from this file.
    """
    start_path = start or Path(__file__).resolve()
    for p in [start_path] + list(start_path.parents):
        if (p / 'config' / 'config.yaml').exists():
            return p
        # repository marker
        if (p / '.git').exists() and (p / 'README.md').exists():
            return p
    # fallback: two levels up from this file (package root -> repo root)
    return Path(__file__).resolve().parents[2]


def project_path(*parts: str) -> Path:
    return get_project_root() / Path(*parts)
