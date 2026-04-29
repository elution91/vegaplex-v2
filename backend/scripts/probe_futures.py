"""Probe Massive Flat Files for VIX futures (VX) data."""
import os, sys, gzip, io
from pathlib import Path
import boto3
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent.parent / ".env")
s3 = boto3.client(
    "s3", endpoint_url="https://files.massive.com",
    aws_access_key_id=os.getenv("MASSIVE_ACCESS_KEY"),
    aws_secret_access_key=os.getenv("MASSIVE_SECRET_KEY"),
    region_name="us-east-1",
)
BUCKET = "flatfiles"

print("Looking for VIX futures (VX) data across all futures exchanges…\n")

# Try every us_futures path; VX trades on CFE/CBOE
for ex in ["us_futures_cbot", "us_futures_cme", "us_futures_comex", "us_futures_nymex",
           "us_futures_cfe", "us_futures_ice"]:
    print(f"── {ex}/")
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix=f"{ex}/", Delimiter="/", MaxKeys=10)
    if not resp.get("CommonPrefixes") and not resp.get("Contents"):
        print("    (no access or empty)")
        continue
    for sub in resp.get("CommonPrefixes", []):
        print(f"    {sub['Prefix']}")

# Inspect a recent CBOT or CME daily file to see what tickers are in it
print("\nSampling one recent us_futures_cme/day_aggs file to see ticker format…")
try:
    resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="us_futures_cme/day_aggs_v1/2026/04/", MaxKeys=5)
    sample = resp.get("Contents", [{}])[-1] if resp.get("Contents") else None
    if sample:
        print(f"  {sample['Key']} ({sample['Size']/1e6:.1f} MB)")
        body = s3.get_object(Bucket=BUCKET, Key=sample["Key"])["Body"].read()
        text = gzip.decompress(body).decode("utf-8", errors="replace")
        lines = text.splitlines()
        print(f"  Header: {lines[0]}")
        print(f"  First 5 rows:")
        for line in lines[1:6]:
            print(f"    {line}")
        # Search for VX-like tickers
        print("  Scanning for VX tickers in this file…")
        vx_count = 0
        for line in lines[1:]:
            tk = line.split(",", 1)[0]
            if "VX" in tk.upper():
                if vx_count < 5:
                    print(f"    FOUND: {line}")
                vx_count += 1
        print(f"  Total VX-matching rows: {vx_count}")
except Exception as e:
    print(f"  ERROR: {e}")
