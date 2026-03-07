"""
upload_to_r2.py — sync scraper outputs to Cloudflare R2
Reads creds from env vars: R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY,
                           R2_ACCOUNT_ID, R2_BUCKET
"""
import os, hashlib, mimetypes, sys
from pathlib import Path
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

_REQUIRED_ENV = ["R2_ACCOUNT_ID", "R2_BUCKET", "R2_ACCESS_KEY_ID", "R2_SECRET_ACCESS_KEY"]
_missing = [v for v in _REQUIRED_ENV if not os.environ.get(v)]
if _missing:
    print(f"ERROR: missing required environment variable(s): {', '.join(_missing)}", file=sys.stderr)
    sys.exit(1)

R2_ENDPOINT = f"https://{os.environ['R2_ACCOUNT_ID']}.r2.cloudflarestorage.com"
R2_BUCKET   = os.environ["R2_BUCKET"]

s3 = boto3.client(
    "s3",
    endpoint_url=R2_ENDPOINT,
    aws_access_key_id=os.environ["R2_ACCESS_KEY_ID"],
    aws_secret_access_key=os.environ["R2_SECRET_ACCESS_KEY"],
    config=Config(signature_version="s3v4"),
    region_name="auto",
)

def md5(path: Path) -> str:
    h = hashlib.md5()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()

def upload(local: Path, key: str):
    ct = mimetypes.guess_type(str(local))[0] or "application/octet-stream"
    try:
        head = s3.head_object(Bucket=R2_BUCKET, Key=key)
        etag = head["ETag"].strip('"')
        # ETags for multipart uploads contain a hyphen (e.g. "abc123-2")
        # and cannot be compared to a plain MD5 hash — always re-upload those.
        if "-" not in etag and etag == md5(local):
            print(f"  skip (unchanged)  {key}")
            return
    except ClientError:
        pass  # doesn't exist yet — upload
    print(f"  upload  {key}  ({local.stat().st_size:,} bytes)")
    s3.upload_file(str(local), R2_BUCKET, key, ExtraArgs={"ContentType": ct})

uploaded = 0
for name in ["games.json", "games_cache.json"]:
    p = Path(name)
    if p.exists():
        upload(p, name)
        uploaded += 1
    else:
        print(f"  missing: {name}")

shots = Path("screenshots")
if shots.exists():
    for f in sorted(shots.rglob("*")):
        if f.is_file():
            upload(f, str(f).replace("\\", "/"))
            uploaded += 1
else:
    print("  screenshots/ not found — skipping")

print(f"\nDone. {uploaded} file(s) processed.")
