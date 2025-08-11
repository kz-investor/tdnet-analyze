#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
from constants import list_unique_markets, EXCLUDED_MARKETS_DEFAULT


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default="inputs/companies.csv")
    args = p.parse_args()

    uniques = list_unique_markets(args.csv)
    print("Unique markets (sorted):")
    for m in uniques:
        print(f"- {m}")

    print("\nCurrent exclusion candidates:")
    for m in sorted(EXCLUDED_MARKETS_DEFAULT):
        print(f"- {m}")


if __name__ == "__main__":
    main()
