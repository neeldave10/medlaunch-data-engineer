import os, json, sys, logging
from datetime import date, timedelta
from typing import List, Dict, Any, Optional, Generator
from urllib.parse import unquote_plus

import boto3
from botocore.exceptions import BotoCoreError, ClientError

# ---------- Logging ----------
logger = logging.getLogger("filter_expiring_accreditations")
if not logger.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    logger.addHandler(h)
logger.setLevel(logging.INFO)

s3 = boto3.client("s3")

# ---------- Parsing helpers ----------
def _iter_ndjson(text: str) -> Optional[List[Dict[str, Any]]]:
    recs = []
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return []
    try:
        for ln in lines:
            recs.append(json.loads(ln))
        return recs
    except json.JSONDecodeError:
        return None

def _parse_json_array(text: str) -> Optional[List[Dict[str, Any]]]:
    try:
        obj = json.loads(text)
        return obj if isinstance(obj, list) else None
    except json.JSONDecodeError:
        return None

def _iter_concatenated(text: str) -> Optional[List[Dict[str, Any]]]:
    dec = json.JSONDecoder(); idx = 0; n = len(text); recs = []
    try:
        while idx < n:
            while idx < n and text[idx].isspace():
                idx += 1
            if idx >= n: break
            obj, next_idx = dec.raw_decode(text, idx)
            recs.append(obj); idx = next_idx
        return recs
    except json.JSONDecodeError:
        return None

def detect_and_extract_records(text: str) -> List[Dict[str, Any]]:
    for parser in (_iter_ndjson, _parse_json_array, _iter_concatenated):
        res = parser(text)
        if res is not None:
            return res
    raise ValueError("Unable to parse as NDJSON, JSON array, or concatenated JSON objects.")

# ---------- Date / filter ----------
from datetime import date
def _parse_iso_date(s: str):
    if not s: return None
    s = s.strip()[:10]
    try: return date.fromisoformat(s)
    except Exception: return None

def accreditation_expires_within(record: Dict[str, Any], threshold: date, today: date) -> bool:
    for acc in (record.get("accreditations") or []):
        d = _parse_iso_date(acc.get("valid_until"))
        if d and today <= d <= threshold:
            return True
    return False

# ---------- IO ----------
def list_keys(bucket: str, prefix: str) -> Generator[str, None, None]:
    paginator = s3.get_paginator("list_objects_v2")
    try:
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                k = obj["Key"]
                if not k.endswith("/"):
                    yield k
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Failed to list s3://{bucket}/{prefix}: {e}")

def process_object(bucket: str, key: str, out_prefix: str, threshold: date, today: date) -> int:
    logger.info(f"Reading s3://{bucket}/{key}")
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        text = body.decode("utf-8")
        records = detect_and_extract_records(text)
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Read error {key}: {e}"); return 0
    except UnicodeDecodeError:
        logger.error(f"Non-UTF8 file: {key}"); return 0
    except Exception as e:
        logger.error(f"Parse error {key}: {e}"); return 0

    filtered = [r for r in records if accreditation_expires_within(r, threshold, today)]
    if not filtered:
        logger.info(f"No expiring records in {key}")
        return 0

    ndjson_payload = "\n".join(json.dumps(r, separators=(",", ":")) for r in filtered)
    base = key.rsplit("/", 1)[-1].rsplit(".", 1)[0]
    out_key = out_prefix.rstrip("/") + f"/{base}_filtered.ndjson"
    try:
        s3.put_object(
            Bucket=bucket,
            Key=out_key,
            Body=ndjson_payload.encode("utf-8"),
            ContentType="application/x-ndjson",
        )
        logger.info(f"Wrote {len(filtered)} -> s3://{bucket}/{out_key}")
        return len(filtered)
    except (BotoCoreError, ClientError) as e:
        logger.error(f"Write error {key}: {e}"); return 0

def run_job(default_bucket: str, input_prefix: str, output_prefix: str, months: int,
            only_bucket: str = None, only_key: str = None) -> Dict[str, int]:
    today = date.today()
    threshold = today + timedelta(days=int(round(months * 30.5)))
    bkt = only_bucket or default_bucket

    total_files = 0
    total_written = 0

    if only_key:
        total_files = 1
        total_written += process_object(bkt, only_key, output_prefix, threshold, today)
    else:
        for key in list_keys(default_bucket, input_prefix):
            total_files += 1
            total_written += process_object(default_bucket, key, output_prefix, threshold, today)

    logger.info(f"Done. Files scanned: {total_files}, records written: {total_written}")
    return {"files_scanned": total_files, "records_written": total_written}

# ---------- Lambda entry ----------
def lambda_handler(event, context):
    bucket = os.environ["BUCKET"]            # default bucket
    input_prefix = os.environ["INPUT_PREFIX"]
    output_prefix = os.environ["OUTPUT_PREFIX"]
    months = int(os.environ.get("MONTHS", "6"))

    # S3 event? process that one object; otherwise process the whole prefix
    try:
        if isinstance(event, dict) and event.get("Records"):
            for r in event["Records"]:
                if r.get("eventSource") == "aws:s3":
                    only_bucket = r["s3"]["bucket"]["name"]
                    only_key = unquote_plus(r["s3"]["object"]["key"])
                    run_job(bucket, input_prefix, output_prefix, months,
                            only_bucket=only_bucket, only_key=only_key)
        else:
            run_job(bucket, input_prefix, output_prefix, months)
        return {"ok": True}
    except Exception as e:
        logger.exception(f"Unhandled error: {e}")
        raise