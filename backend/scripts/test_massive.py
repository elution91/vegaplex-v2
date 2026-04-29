"""
Quick connectivity test for Massive Flat Files (S3-compatible).

Usage:
  1. Put credentials in backend/.env:
       MASSIVE_ACCESS_KEY=...
       MASSIVE_SECRET_KEY=...
  2. cd backend && python scripts/test_massive.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

try:
    import boto3
    from botocore.exceptions import ClientError, EndpointConnectionError
except ImportError:
    print("ERROR: boto3 not installed. Run: pip install boto3 python-dotenv")
    sys.exit(1)

try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
except ImportError:
    pass  # ok, env vars may be set another way

ENDPOINT = "https://files.massive.com"
BUCKET   = "flatfiles"

ak = os.getenv("MASSIVE_ACCESS_KEY")
sk = os.getenv("MASSIVE_SECRET_KEY")

if not ak or not sk:
    print("ERROR: MASSIVE_ACCESS_KEY / MASSIVE_SECRET_KEY not set in .env")
    sys.exit(1)

print(f"Endpoint:   {ENDPOINT}")
print(f"Bucket:     {BUCKET}")
print(f"Access key: {ak[:6]}…{ak[-4:]}")
print()

s3 = boto3.client(
    "s3",
    endpoint_url=ENDPOINT,
    aws_access_key_id=ak,
    aws_secret_access_key=sk,
    region_name="us-east-1",
)

# Test 1 — list top-level prefixes
print("─" * 60)
print("Test 1: List top-level prefixes")
print("─" * 60)
try:
    resp = s3.list_objects_v2(Bucket=BUCKET, Delimiter="/", MaxKeys=20)
    prefixes = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
    if not prefixes:
        objs = [o["Key"] for o in resp.get("Contents", [])][:10]
        print(f"  Top-level objects (first 10): {objs}")
    else:
        for p in prefixes:
            print(f"  {p}")
    print(f"  ✓ Connected and authenticated")
except (ClientError, EndpointConnectionError) as e:
    print(f"  ✗ FAILED: {e}")
    sys.exit(1)

# Test 2 — list options-related prefixes (try a few common patterns)
print()
print("─" * 60)
print("Test 2: Find options data path")
print("─" * 60)
for candidate in ["us_options_opra/", "options/", "us_options/", "global_options/"]:
    try:
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=candidate, Delimiter="/", MaxKeys=5)
        sub = [p["Prefix"] for p in resp.get("CommonPrefixes", [])]
        objs = [o["Key"] for o in resp.get("Contents", [])]
        if sub or objs:
            print(f"  ✓ {candidate} exists")
            for s in sub[:5]:
                print(f"      {s}")
            for o in objs[:3]:
                print(f"      {o}")
    except ClientError:
        pass

# Test 3 — count days available in first matched options path
print()
print("─" * 60)
print("Test 3: Sample file size (if accessible)")
print("─" * 60)
try:
    resp = s3.list_objects_v2(Bucket=BUCKET, MaxKeys=1)
    if resp.get("Contents"):
        first = resp["Contents"][0]
        print(f"  First object: {first['Key']}")
        print(f"  Size: {first['Size']/1024:.1f} KB  modified: {first['LastModified']}")
except ClientError as e:
    print(f"  ✗ {e}")

print()
print("Done.")
