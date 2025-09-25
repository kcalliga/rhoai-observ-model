#!/usr/bin/env python3
import os, time, json, math, sys, pathlib
from datetime import datetime, timezone, timedelta
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import yaml

# ---- Config from env ----
LOKI_URL = os.getenv("LOKI_URL", "http://loki:3100")  # e.g., http://loki-distributor.loki:3100
LOKI_BEARER = os.getenv("LOKI_BEARER", "")            # optional: bearer token
LOKI_TENANT = os.getenv("LOKI_TENANT", "")            # optional: for multi-tenant
WINDOW_MIN = int(os.getenv("WINDOW_MIN", "5"))        # how far back to pull each run
STEP = os.getenv("LOKI_STEP", "30s")                  # query_range step
S3_BUCKET = os.getenv("S3_BUCKET", "logs-raw")
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "")            # e.g., http://s3.noobaa.svc:80 or https://s3.amazonaws.com
S3_REGION = os.getenv("S3_REGION", "us-east-1")
S3_PREFIX = os.getenv("S3_PREFIX", "loki/parquet")
# AWS creds use standard envs: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN

# Optional: write to local path instead of S3 (set FS_LOCAL_DIR to enable)
FS_LOCAL_DIR = os.getenv("FS_LOCAL_DIR", "")  # e.g., "/data/parquet"

# ---- Helpers ----
def ns(ts: float) -> int:
    return int(ts * 1_000_000_000)

def now_ns() -> int:
    return ns(time.time())

def headers():
    h = {"Accept": "application/json"}
    if LOKI_BEARER:
        h["Authorization"] = f"Bearer {LOKI_BEARER}"
    if LOKI_TENANT:
        h["X-Scope-OrgID"] = LOKI_TENANT
    return h

def read_queries(path="/etc/loki-extractor/queries.yaml"):
    with open(path, "r") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg.get("queries", [])

def query_range(logql: str, start_ns: int, end_ns: int, step: str, direction="forward", limit=5000):
    url = f"{LOKI_URL}/loki/api/v1/query_range"
    params = {
        "query": logql,
        "start": start_ns,
        "end": end_ns,
        "step": step,
        "direction": direction,
        "limit": str(limit),
    }
    r = requests.get(url, headers=headers(), params=params, timeout=120)
    r.raise_for_status()
    return r.json()

def to_records(result):
    """Flatten Loki matrix/streams to row dicts."""
    recs = []
    for stream in result.get("data", {}).get("result", []):
        labels = stream.get("metric", {})
        # Streams use 'values' for time series (timestamp, value) or 'values' for logs (ts, line)
        values = stream.get("values", [])
        for ts, val in values:
            # ts from Loki is string epoch ns; val is string (log line or numeric)
            try:
                ts_ns = int(ts)
            except:
                # sometimes ts is float string
                ts_ns = int(float(ts))
            ts_dt = datetime.fromtimestamp(ts_ns / 1e9, tz=timezone.utc)
            # For logs, `val` is the log line; for aggregates it's a number string
            rec = {
                "ts_ns": ts_ns,
                "ts": ts_dt.isoformat(),
                "message": val,
            }
            # include labels as columns
            for k, v in labels.items():
                rec[k] = v
            recs.append(rec)
    return recs

def write_parquet(df: pd.DataFrame, run_dt_utc: datetime, query_name: str):
    if df.empty:
        return None

    # Partition by date/hour for simple pruning later
    date_part = run_dt_utc.strftime("%Y-%m-%d")
    hour_part = run_dt_utc.strftime("%H")
    minute_part = run_dt_utc.strftime("%M")

    table = pa.Table.from_pandas(df, preserve_index=False)

    if FS_LOCAL_DIR:
        out_dir = pathlib.Path(FS_LOCAL_DIR) / S3_PREFIX / f"date={date_part}" / f"hour={hour_part}" / f"query={query_name}"
        out_dir.mkdir(parents=True, exist_ok=True)
        file_path = out_dir / f"part-{minute_part}.parquet"
        pq.write_table(table, file_path.as_posix(), compression="ZSTD")
        return file_path.as_posix()

    # S3 target
    s3_opts = {}
    if S3_ENDPOINT:
        s3_opts["endpoint_override"] = S3_ENDPOINT
    fs = pafs.S3FileSystem(region=S3_REGION, **s3_opts)
    out_dir = f"{S3_BUCKET}/{S3_PREFIX}/date={date_part}/hour={hour_part}/query={query_name}"
    file_path = f"{out_dir}/part-{minute_part}.parquet"
    with fs.open_output_stream(file_path) as sink:
        pq.write_table(table, sink, compression="ZSTD")
    return f"s3://{file_path}"

def main():
    run_dt_utc = datetime.now(timezone.utc)
    end = now_ns()
    start = end - ns(60 * WINDOW_MIN)
    queries = read_queries()

    print(f"[INFO] Extracting {len(queries)} queries from {start} to {end} (last {WINDOW_MIN} min)")

    written = []
    for q in queries:
        name = q.get("name") or "unnamed"
        logql = q["logql"]
        limit = int(q.get("limit", 5000))
        direction = q.get("direction", "forward")

        print(f"[INFO] Query: {name} -> {logql}")
        try:
            res = query_range(logql, start, end, STEP, direction=direction, limit=limit)
        except Exception as e:
            print(f"[ERROR] Loki query failed for {name}: {e}", file=sys.stderr)
            continue

        rows = to_records(res)
        if not rows:
            print(f"[INFO] No rows for {name}")
            continue

        df = pd.DataFrame(rows)
        # ensure column order and types
        # ts, ts_ns, message + labels
        for col in ("ts_ns",):
            if col in df.columns:
                df[col] = pd.to_int64(df[col])
        path = write_parquet(df, run_dt_utc, name)
        if path:
            print(f"[OK] Wrote {len(df):,} rows to {path}")
            written.append({"name": name, "rows": len(df), "path": path})

    print(json.dumps({"written": written}, indent=2))

if __name__ == "__main__":
    main()
