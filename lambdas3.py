import os, json, time, hashlib, urllib.parse, sys, logging
from datetime import date
import boto3
from botocore.exceptions import BotoCoreError, ClientError

ATHENA = boto3.client("athena")
S3 = boto3.client("s3")

#  Logging
log = logging.getLogger("on_upload_count_accredited")
if not log.handlers:
    h = logging.StreamHandler(sys.stdout)
    h.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(message)s"))
    log.addHandler(h)
log.setLevel(logging.INFO)

#  Env vars (set these in Lambda > Configuration > Environment variables) 
ATHENA_DATABASE   = os.environ["ATHENA_DATABASE"]     # e.g., "medlaunch_db"
ATHENA_TABLE      = os.environ["ATHENA_TABLE"]        # e.g., "facilities_raw"
ATHENA_WORKGROUP  = os.environ.get("ATHENA_WORKGROUP", "primary")
RESULTS_S3_PREFIX = os.environ["RESULTS_S3_PREFIX"]   # e.g., "s3://medlaunch/exports/state_counts/"
# Optional: UNLOAD format controls
UNLOAD_FORMAT     = os.environ.get("UNLOAD_FORMAT", "TEXTFILE")   # TEXTFILE for CSV-ish text
UNLOAD_DELIM      = os.environ.get("UNLOAD_DELIM", ",")
UNLOAD_COMPRESSION= os.environ.get("UNLOAD_COMPRESSION", "NONE")  # NONE for previewable

#  SQL builders 
def build_sql(today_str: str) -> str:
    return f"""
UNLOAD (
  SELECT *
  FROM (
    SELECT 0 AS _order, 'state' AS state, 'accredited_facilities' AS accredited_facilities
    UNION ALL
    SELECT 1 AS _order, state, CAST(accredited_facilities AS VARCHAR)
    FROM (
      SELECT r.location.state AS state,
             COUNT(DISTINCT r.facility_id) AS accredited_facilities
      FROM {ATHENA_DATABASE}.{ATHENA_TABLE} r
      CROSS JOIN UNNEST(r.accreditations) AS t(a)
      WHERE CAST(a.valid_until AS DATE) >= DATE '{today_str}'
      GROUP BY r.location.state
    ) s
  ) out
  ORDER BY _order, state
)
TO '{{OUTPUT}}'
WITH (format='TEXTFILE', field_delimiter=',', compression='NONE')
""".strip()

def results_prefix_for_object(src_bucket: str, src_key: str) -> str:
    # Example:
    # s3://medlaunch/exports/state_counts/<bucket>/<urlencoded-key>/<YYYY-MM-DD>/
    today = date.today().isoformat()
    enc_key = urllib.parse.quote(src_key, safe="")
    base = RESULTS_S3_PREFIX.rstrip("/")
    return f"{base}/{src_bucket}/{enc_key}/{today}/"

#  Athena helpers 
def start_unload(query: str, workgroup: str, output_location: str, token: str) -> str:
    # Substitute target output into query template
    q = query.replace("{OUTPUT}", output_location.rstrip("/") + "/")
    resp = ATHENA.start_query_execution(
        QueryString=q,
        QueryExecutionContext={"Database": ATHENA_DATABASE},
        ResultConfiguration={"OutputLocation": output_location},
        WorkGroup=workgroup,
        ClientRequestToken=token  # idempotency
    )
    return resp["QueryExecutionId"]

def wait_for_query(qid: str, context) -> str:
    delay = 1.0
    while True:
        resp = ATHENA.get_query_execution(QueryExecutionId=qid)
        state = resp["QueryExecution"]["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            return state
        # Avoid Lambda timeout; leave ~10s headroom
        remaining_ms = getattr(context, "get_remaining_time_in_millis", lambda: 30000)()
        if remaining_ms < 10000:
            log.warning("Exiting early to avoid timeout; letting retry handle continuation",
                        extra={"query_id": qid, "remaining_ms": remaining_ms})
            raise TimeoutError(f"Query still running: {qid}")
        time.sleep(delay)
        delay = min(delay * 1.7, 10.0)

def put_marker(bucket: str, key: str, data: dict):
    S3.put_object(Bucket=bucket, Key=key,
                  Body=json.dumps(data, separators=(",", ":")).encode("utf-8"),
                  ContentType="application/json")

#  Lambda entry 
def lambda_handler(event, context):
    # Expect S3 Put event(s)
    records = event.get("Records", []) if isinstance(event, dict) else []
    if not records:
        log.info("No Records in event; nothing to do.")
        return {"ok": True}

    for rec in records:
        if rec.get("eventSource") != "aws:s3":
            continue
        src_bucket = rec["s3"]["bucket"]["name"]
        src_key = urllib.parse.unquote_plus(rec["s3"]["object"]["key"])
        log.info(f"Triggered by s3://{src_bucket}/{src_key}")

        today_str = date.today().isoformat()
        sql = build_sql(today_str)
        output_prefix = results_prefix_for_object(src_bucket, src_key)  # where CSV will land

        # Idempotency: hash of bucket+key+sql
        token = hashlib.sha256((src_bucket + "|" + src_key + "|" + sql).encode("utf-8")).hexdigest()

        # Athena will also put its own small artifacts in the workgroup output location;
        # the actual CSV data files are written under 'output_prefix' by UNLOAD.
        try:
            qid = start_unload(sql, ATHENA_WORKGROUP, output_prefix, token)
            state = wait_for_query(qid, context)
            log.info(f"Athena UNLOAD finished: {state} (QueryExecutionId={qid})")

            if state != "SUCCEEDED":
                raise RuntimeError(f"Athena query did not succeed: {qid} state={state}")
            
            # Optional: write a marker for easy traceability
            out_bucket = output_prefix.split("/", 3)[2]
            out_key_prefix = output_prefix.split("/", 3)[3]
            put_marker(out_bucket, f"{out_key_prefix}marker.json", {
                "source_bucket": src_bucket,
                "source_key": src_key,
                "date": today_str,
                "query_execution_id": qid,
                "output_prefix": output_prefix,
                "status": "SUCCEEDED"
            })

        except (BotoCoreError, ClientError) as e:
            log.error(f"AWS client error: {e}")
            raise
        except TimeoutError as e:
            log.warning(f"Timeout; allowing retry: {e}")
            raise
        except Exception as e:
            log.error(f"Unhandled error: {e}")
            raise

    return {"ok": True}