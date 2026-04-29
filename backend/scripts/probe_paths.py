"""
Probe Massive Flat Files for any options paths beyond day_aggs_v1.
Looking for: snapshots, greeks, IV, open interest, fundamentals.
"""
import os, sys
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

print("Full prefix tree under us_options_opra/:")
print("=" * 70)

def walk(prefix, depth=0, max_depth=3):
    if depth > max_depth:
        return
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, Delimiter="/", MaxKeys=50)
    for sub in resp.get("CommonPrefixes", []):
        p = sub["Prefix"]
        print(f"{'  ' * depth}{p}")
        walk(p, depth + 1, max_depth)
    # Also list a few example files at this level
    files = resp.get("Contents", [])[:3]
    for f in files:
        print(f"{'  ' * depth}  · {f['Key'].split('/')[-1]}  ({f['Size']/1024:.1f} KB)")

walk("us_options_opra/", depth=0, max_depth=3)

print()
print("Other potentially relevant top-level prefixes:")
print("=" * 70)
for candidate in [
    "us_options_iv/", "us_options_greeks/", "us_options_oi/",
    "us_options_snapshots/", "us_options_universe/",
    "options_iv/", "options_greeks/", "snapshots/",
]:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=candidate, MaxKeys=3)
    if resp.get("Contents") or resp.get("CommonPrefixes"):
        print(f"  ✓ {candidate}")
        for c in resp.get("CommonPrefixes", []):
            print(f"      {c['Prefix']}")
        for f in resp.get("Contents", [])[:3]:
            print(f"      {f['Key']}")
