"""
Inspect one day of us_options_opra/day_aggs_v1 to confirm schema and find
how options ticker → underlying mapping works.

Usage:  python scripts/inspect_day_aggs.py
"""
from __future__ import annotations

import gzip
import io
import os
import sys
from pathlib import Path

import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")

s3 = boto3.client(
    "s3",
    endpoint_url="https://files.massive.com",
    aws_access_key_id=os.getenv("MASSIVE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MASSIVE_SECRET_KEY"),
    region_name="us-east-1",
)
BUCKET = "flatfiles"

# 1. List recent days available
print("Listing recent day_aggs files…")
resp = s3.list_objects_v2(
    Bucket=BUCKET,
    Prefix="us_options_opra/day_aggs_v1/2026/04/",
    MaxKeys=10,
)
for obj in resp.get("Contents", [])[-5:]:
    print(f"  {obj['Key']}  ({obj['Size']/1e6:.1f} MB)")

if not resp.get("Contents"):
    # try 2025
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="us_options_opra/day_aggs_v1/2025/12/", MaxKeys=10)
    print("(no 2026/04, showing 2025/12)")
    for obj in resp.get("Contents", [])[-5:]:
        print(f"  {obj['Key']}  ({obj['Size']/1e6:.1f} MB)")

# 2. Download the most recent file and dump schema
latest = resp["Contents"][-1]
print(f"\nDownloading {latest['Key']} ({latest['Size']/1e6:.1f} MB)…")
body = s3.get_object(Bucket=BUCKET, Key=latest["Key"])["Body"].read()
print(f"Downloaded {len(body)/1e6:.1f} MB")

# 3. Decompress + inspect
print("\nDecompressing and reading first rows…")
with gzip.open(io.BytesIO(body), "rt") as f:
    header = f.readline().strip()
    print(f"\nHeader:\n  {header}")
    print(f"\nFirst 5 data rows:")
    for _ in range(5):
        print(f"  {f.readline().strip()}")

    # Find rows for SPY options to confirm the ticker format
    print(f"\nLooking for SPY rows (sample of 5)…")
    f.seek(0)
    f.readline()  # skip header
    spy_count = 0
    for line in f:
        if "SPY" in line.split(",", 1)[0]:
            print(f"  {line.strip()}")
            spy_count += 1
            if spy_count >= 5:
                break

    # Count total rows
    f.seek(0)
    f.readline()
    total = sum(1 for _ in f)
    print(f"\nTotal rows in file: {total:,}")
