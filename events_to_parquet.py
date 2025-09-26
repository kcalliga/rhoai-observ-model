#!/usr/bin/env python3
import os, io, json, pathlib, time
from datetime import datetime, timezone, timedelta

import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq
import boto3
from botocore.config import Config

from kubernetes import client, config
from kubernetes.client import ApiClient

# -------- config (env overridable) --------
WINDOW_MIN  = int(os.getenv("WINDOW_MIN", "5"))
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET   = os.getenv("S3_BUCKET", "rhoai-observ-model")
S3_PREFIX   = os.getenv("S3_PREFIX", "k8s/events/parquet")
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN") or None
VERIFY_TLS = os.getenv("VERIFY_TLS", "false").lower() not in ("0","false","no")

# -------- helpers --------
def _now_utc():
    return datetime.now(timezone.utc)

def _to_iso(dt):
    if not dt: return None
    if isinstance(dt, str): return dt
    # k8s client returns datetime with tzinfo
    return dt.astimezone(timezone.utc).isoformat()

def _row_from_core_v1(ev):
    # core/v1 Event
    return {
        "apiVersion": "v1",
        "namespace": ev.metadata.namespace,
        "name": ev.metadata.name,
        "type": ev.type,
        "reason": ev.reason,
        "message": ev.message,
        "count": getattr(ev, "count", None),
        "first_timestamp": _to_iso(getattr(ev, "first_timestamp", None)),
        "last_timestamp": _to_iso(getattr(ev, "last_timestamp", None)),
        "event_time": None,
        "reporting_component": getattr(ev, "reporting_component", None),
        "reporting_instance": getattr(ev, "reporting_instance", None),
        "involved_kind": ev.involved_object.kind if ev.involved_object else None,
        "involved_name": ev.involved_object.name if ev.involved_object else None,
        "involved_namespace": ev.involved_object.namespace if ev.involved_object else None,
        "involved_uid": ev.involved_object.uid if ev.involved_object else None,
        "uid": ev.metadata.uid,
        "resource_version": ev.metadata.resource_version,
        "creation_timestamp": _to_iso(ev.metadata.creation_timestamp),
    }

def _row_from_events_v1(ev):
    # events.k8s.io/v1 Event
    series = getattr(ev, "series", None)
    regarding = getattr(ev, "regarding", None)
    return {
        "apiVersion": "events.k8s.io/v1",
        "namespace": ev.metadata.namespace,
        "name": ev.metadata.name,
        "type": ev.type,
        "reason": ev.reason,
        "message": getattr(ev, "note", None),
        "count": getattr(series, "count", None),
        "first_timestamp": None,
        "last_timestamp": _to_iso(getattr(series, "last_observed_time", None)),
        "event_time": _to_iso(getattr(ev, "event_time", None)),
        "reporting_component": getattr(ev, "reporting_controller", None),
        "reporting_instance": getattr(ev, "reporting_instance", None),
        "involved_kind": regarding.kind if regarding else None,
        "involved_name": regarding.name if regarding else None,
        "involved_namespace": regarding.namespace if regarding else None,
        "involved_uid": regarding.uid if regarding else None,
        "uid": ev.metadata.uid,
        "resource_version": ev.metadata.resource_version,
        "creation_timestamp": _to_iso(ev.metadata.creation_timestamp),
    }

def _within_window(row, start_utc):
    # prefer event_time/last_timestamp/creation_timestamp in that order
    for key in ("event_time", "last_timestamp", "creation_timestamp"):
        v = row.get(key)
        if v:
            try:
                dt = datetime.fromisoformat(v.replace("Z","+00:00"))
                return dt >= start_utc
            except Exception:
                pass
    return False

def _dedupe_key(row):
    # stable-ish identity per emission
    return (
        row.get("uid"),
        row.get("reason"),
        row.get("type"),
        row.get("last_timestamp") or row.get("event_time") or row.get("creation_timestamp"),
    )

def _to_table(df: pd.DataFrame) -> pa.Table:
    # cast object cols to string; leave counts as Int64
    for c in df.columns:
        if pd.api.types.is_object_dtype(df[c]):
            df[c] = df[c].astype("string")
    return pa.Table.from_pandas(df, preserve_index=False)

def _s3_put_bytes(key: str, data: bytes):
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")
    cfg = Config(signature_version="s3v4", s3={"addressing_style": "path"})
    s3 = boto3.client(
        "s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        endpoint_url=(S3_ENDPOINT or None),
        config=cfg,
        verify=VERIFY_TLS,
    )
    s3.put_object(Bucket=S3_BUCKET, Key=key, Body=data)

def main():
    start_utc = _now_utc() - timedelta(minutes=WINDOW_MIN)
    print(f"[INFO] collecting events since {start_utc.isoformat()} (last {WINDOW_MIN} min)")

    # load k8s config (in cluster or local)
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

    v1 = client.CoreV1Api()
    evapi = client.EventsV1Api()
    rows = []

    # events.k8s.io/v1 (newer API)
    try:
        cont = None
        while True:
            resp = evapi.list_event_for_all_namespaces(limit=1000, _continue=cont)
            for ev in resp.items:
                row = _row_from_events_v1(ev)
                if _within_window(row, start_utc):
                    rows.append(row)
            cont = resp.metadata._continue
            if not cont: break
        print(f"[INFO] events.k8s.io/v1 count in window: {len(rows)}")
    except Exception as e:
        print(f"[WARN] could not read events.k8s.io/v1: {e}")

    # core/v1 (legacy API) — also useful as a fallback
    try:
        cont = None
        while True:
            resp = v1.list_event_for_all_namespaces(limit=2000, _continue=cont)
            for ev in resp.items:
                row = _row_from_core_v1(ev)
                if _within_window(row, start_utc):
                    rows.append(row)
            cont = resp.metadata._continue
            if not cont: break
        print(f"[INFO] core/v1 count in window (added): {len(rows)}")
    except Exception as e:
        print(f"[WARN] could not read core/v1 events: {e}")

    if not rows:
        print("[INFO] no events in window")
        return

    # de-duplicate
    dedup = {}
    for r in rows:
        dedup[_dedupe_key(r)] = r
    rows = list(dedup.values())

    df = pd.DataFrame(rows)
    table = _to_table(df)

    # partitioned key
    run_dt = _now_utc()
    date_part = run_dt.strftime("%Y-%m-%d")
    hour_part = run_dt.strftime("%H")
    minute_part = run_dt.strftime("%M")
    key = f"{S3_PREFIX}/date={date_part}/hour={hour_part}/part-{minute_part}.parquet"

    buf = io.BytesIO()
    pq.write_table(table, buf, compression="ZSTD")
    buf.seek(0)
    _s3_put_bytes(key, buf.getvalue())
    print(f"[OK] wrote {len(df):,} events -> s3://{S3_BUCKET}/{key}")

if __name__ == "__main__":
    main()
