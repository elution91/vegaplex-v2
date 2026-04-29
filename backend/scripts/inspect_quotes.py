"""Inspect one quotes_v1 file to see if it has IV/greeks columns."""
import gzip, io, os, sys
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

# Find a small recent quotes file
resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="us_options_opra/quotes_v1/2026/04/", MaxKeys=10)
sample = resp["Contents"][0] if resp.get("Contents") else None
if not sample:
    sys.exit("No quotes file found")

print(f"Sampling {sample['Key']} ({sample['Size']/1e6:.1f} MB) — downloading first 200KB only…")
# Range request — just get the first chunk so we can read the header
body = s3.get_object(Bucket=BUCKET, Key=sample["Key"], Range="bytes=0-204800")["Body"].read()
# gzip needs a complete stream; the first 200KB usually decompresses partially
try:
    text = gzip.decompress(body).decode("utf-8", errors="replace")
except Exception:
    # partial gzip; try streaming-decompress what we can
    import zlib
    d = zlib.decompressobj(31)  # 31 = gzip
    text = d.decompress(body).decode("utf-8", errors="replace")

lines = text.splitlines()
print(f"\nHeader:\n  {lines[0]}")
print(f"\nFirst 5 rows:")
for line in lines[1:6]:
    print(f"  {line}")
