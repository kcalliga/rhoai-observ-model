#!/usr/bin/env python3
# Loki -> Parquet (S3 or local) with **hard-coded** config

import io
import os
import re
import sys
import json
import time
import pathlib
from datetime import datetime, timezone

import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import yaml
import boto3
from botocore.config import Config

# -------------------- HARD-CODED CONFIG --------------------

# Loki (OpenShift LokiStack multi-tenant route)
LOKI_URL    = "https://logging-loki-openshift-logging.apps.rhoai.ocp-poc-demo.com"
LOKI_TENANT = "infrastructure"  # appears in the request path
LOKI_BEARER = ""

WINDOW_MIN  = 5
LOKI_STEP   = "30s"

# S3/MinIO output
S3_ENDPOINT = "http://192.168.4.152:9000"
S3_BUCKET   = "rhoai-observ-model"
S3_PREFIX   = "loki/parquet"

# REQUIRED AWS_* creds (hard-coded for now)
AWS_ACCESS_KEY_ID     = ""
AWS_SECRET_ACCESS_KEY = ""
AWS_SESSION_TOKEN     = None  # optional

# Local/PVC mode: set to a folder path to write locally instead of S3
FS_LOCAL_DIR = ""  # e.g., "/data/parquet" (empty string means "use S3")

# Queries file (if missing, we use DEFAULT_QUERIES below)
QUERIES_PATH = "queries.yaml"
DEFAULT_QUERIES = [
    {"name": "all_logs_sample", "logql": '{kubernetes_container_name!=""} |~ ".*"', "limit": 5000, "direction": "forward"},
    {"name": "errors",          "logql": '{level=~"(?i)error|err|fatal"}',          "limit": 10000, "direction": "forward"},
    {"name": "http_4xx_5xx",    "logql": '{status=~"4..|5.."}',                     "limit": 10000, "direction": "forward"},
]

# add near your config
VERIFY_TLS = False

# -------------------- Helpers --------------------

def ns(ts_sec: float) -> int:
    return int(ts_sec * 1_000_000_000)

def now_ns() -> int:
    return ns(time.time())

def _headers():
    h = {"Accept": "application/json"}
    if LOKI_BEARER:
        h["Authorization"] = f"Bearer {LOKI_BEARER}"
    if LOKI_TENANT:
        # Harmless even when tenant is in path; some setups require it.
        h["X-Scope-OrgID"] = LOKI_TENANT
    return h

def _safe_part(s: str) -> str:
    # safe for S3 path segments / local folders
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s or "unnamed")

def read_queries(path: str = QUERIES_PATH):
    try:
        with open(path, "r") as f:
            cfg = yaml.safe_load(f) or {}
        q = cfg.get("queries", [])
        if not isinstance(q, list):
            raise ValueError("queries must be a list")
        if not q:
            print("[WARN] queries.yaml has no 'queries'; using defaults", file=sys.stderr)
            return DEFAULT_QUERIES
        return q
    except FileNotFoundError:
        print(f"[WARN] queries file not found at {path}; using defaults", file=sys.stderr)
        return DEFAULT_QUERIES
    except Exception as e:
        print(f"[ERROR] failed to read queries ({e}); using defaults", file=sys.stderr)
        return DEFAULT_QUERIES

def _try_json(resp):
    ctype = resp.headers.get("content-type", "")
    if "application/json" in ctype:
        return resp.json()
    snippet = (resp.text or "")[:300].replace("\n", " ")
    raise RuntimeError(
        f"Non-JSON response (status {resp.status_code}, content-type '{ctype}'). "
        f"First 300 chars: {snippet!r}"
    )

def query_range(logql: str, start_ns: int, end_ns: int, step: str,
                direction="forward", limit=5000, timeout=120):
    if not LOKI_URL or not LOKI_TENANT:
        raise RuntimeError("LOKI_URL and LOKI_TENANT must be set")
    url = f"{LOKI_URL}/api/logs/v1/{LOKI_TENANT}/loki/api/v1/query_range"
    params = {
        "query": logql,
        "start": start_ns,
        "end": end_ns,
        "step": step,
        "direction": direction,
        "limit": str(limit),
    }
    resp = requests.get(
        url,
        headers=_headers(),
        params=params,
        timeout=timeout,
        allow_redirects=False,   # surface OAuth redirects instead of parsing HTML
        verify=VERIFY_TLS,       # curl -k equivalent
    )
    if 300 <= resp.status_code < 400:
        raise RuntimeError(f"Redirect {resp.status_code} to {resp.headers.get('Location','')}. "
                           "Likely OAuth; check token/route.")
    resp.raise_for_status()
    return _try_json(resp)

    
def to_records(result: dict):
    """
    Flatten Loki results into row dicts.
    Supports:
      - resultType='streams' -> entry['stream'] labels + 'values' [(ts, line)]
      - resultType='matrix'  -> entry['metric'] labels + 'values' [(ts, value)]
    """
    recs = []
    data = result.get("data", {})
    for entry in data.get("result", []):
        labels = entry.get("stream") or entry.get("metric") or {}
        lbls = {k: ("" if v is None else str(v)) for k, v in labels.items()}
        values = entry.get("values", [])
        for ts, val in values:
            try:
                ts_ns = int(ts)
            except Exception:
                ts_ns = int(float(ts))
            ts_iso = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc).isoformat()
            rec = {"ts_ns": ts_ns, "ts": ts_iso, "message": "" if val is None else str(val)}
            rec.update(lbls)
            recs.append(rec)
    return recs

# -------------------- Writers --------------------

def _write_parquet_local(table: pa.Table, run_dt_utc: datetime, query_name: str):
    date_part = run_dt_utc.strftime("%Y-%m-%d")
    hour_part = run_dt_utc.strftime("%H")
    minute_part = run_dt_utc.strftime("%M")
    safe_query = _safe_part(query_name)

    out_dir = pathlib.Path(FS_LOCAL_DIR) / S3_PREFIX / f"date={date_part}" / f"hour={hour_part}" / f"query={safe_query}"
    out_dir.mkdir(parents=True, exist_ok=True)
    file_path = out_dir / f"part-{minute_part}.parquet"
    pq.write_table(table, file_path.as_posix(), compression="ZSTD")
    return file_path.as_posix()

def _write_parquet_s3(table: pa.Table, run_dt_utc: datetime, query_name: str):
    if not S3_BUCKET:
        raise RuntimeError("S3_BUCKET must be set for S3 mode (unset FS_LOCAL_DIR to use S3).")

    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("Missing S3 credentials: AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY must be set (hard-coded).")

    date_part = run_dt_utc.strftime("%Y-%m-%d")
    hour_part = run_dt_utc.strftime("%H")
    minute_part = run_dt_utc.strftime("%M")
    safe_query = _safe_part(query_name)

    key_prefix = f"{S3_PREFIX}/date={date_part}/hour={hour_part}/query={safe_query}"
    key = f"{key_prefix}/part-{minute_part}.parquet"

    # Parquet -> memory buffer
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="ZSTD")
    buf.seek(0)

    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        endpoint_url=(S3_ENDPOINT or None),
        config=cfg,
    )
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=buf.getvalue())
    return f"s3://{S3_BUCKET}/{key}"

def write_parquet(df: pd.DataFrame, run_dt_utc: datetime, query_name: str):
    if df.empty:
        return None

    # enforce types
    if "ts_ns" in df.columns:
        s = pd.to_numeric(df["ts_ns"], errors="coerce")
        df["ts_ns"] = s.astype("Int64") if s.isna().any() else s.astype("int64")
    if "ts" in df.columns:
        df["ts"] = df["ts"].astype(str)
    if "message" in df.columns:
        df["message"] = df["message"].astype(str)
    for col in df.columns:
        if col not in ("ts_ns",) and df[col].dtype == "object":
            df[col] = df[col].astype(str)

    table = pa.Table.from_pandas(df, preserve_index=False)
    if FS_LOCAL_DIR:
        return _write_parquet_local(table, run_dt_utc, query_name)
    return _write_parquet_s3(table, run_dt_utc, query_name)

# -------------------- Main --------------------

def main():
    run_dt_utc = datetime.now(timezone.utc)
    end = now_ns()
    start = end - ns(60 * WINDOW_MIN)
    queries = read_queries()

    print(f"[INFO] Extracting {len(queries)} queries from {start} to {end} (last {WINDOW_MIN} min)")
    if not FS_LOCAL_DIR:
        print(f"[DEBUG] S3 endpoint: {S3_ENDPOINT}  bucket: {S3_BUCKET}  prefix: {S3_PREFIX}")
        print(f"[DEBUG] Using hard-coded AWS creds: {'yes' if AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY else 'no'}")

    written = []
    for q in queries:
        name = q.get("name") or "unnamed"
        logql = q.get("logql")
        if not logql:
            print(f"[WARN] skipping query without logql: {q}", file=sys.stderr)
            continue
        limit = int(q.get("limit", 5000))
        direction = q.get("direction", "forward")

        print(f"[INFO] Query: {name} -> {logql}")
        try:
            res = query_range(logql, start, end, LOKI_STEP, direction=direction, limit=limit)
        except Exception as e:
            print(f"[ERROR] Loki query failed for {name}: {e}", file=sys.stderr)
            continue

        rows = to_records(res)
        if not rows:
            print(f"[INFO] No rows for {name}")
            continue

        df = pd.DataFrame(rows)
        try:
            path = write_parquet(df, run_dt_utc, name)
        except Exception as e:
            print(f"[ERROR] write failed for {name}: {e}", file=sys.stderr)
            continue

        if path:
            print(f"[OK] Wrote {len(df):,} rows to {path}")
            written.append({"name": name, "rows": len(df), "path": path})

    print(json.dumps({"written": written}, indent=2))

if __name__ == "__main__":
    main()
