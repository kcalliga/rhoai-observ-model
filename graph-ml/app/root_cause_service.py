#!/usr/bin/env python3
import os, json, io
from typing import List, Dict, Optional
from fastapi import FastAPI, Body, HTTPException
import pandas as pd
import numpy as np
import boto3
from botocore.config import Config

S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET   = os.getenv("S3_BUCKET", "rhoai-observ-model")
S3_PREFIX_GRAPH = os.getenv("S3_PREFIX_GRAPH", "graph")
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN") or None
VERIFY_TLS = os.getenv("VERIFY_TLS","false").lower() not in ("0","false","no")

app = FastAPI(title="k8s-root-cause", version="0.1")

def s3():
    cfg = Config(signature_version="s3v4", s3={"addressing_style":"path"})
    return boto3.client("s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        endpoint_url=(S3_ENDPOINT or None),
        config=cfg,
        verify=VERIFY_TLS
    )

def s3_get_text(key:str)->str:
    r = s3().get_object(Bucket=S3_BUCKET, Key=key)
    return r["Body"].read().decode()

def s3_get_parquet_df(url_or_key:str)->pd.DataFrame:
    # if s3://bucket/key form, strip it
    if url_or_key.startswith("s3://"):
        _, rest = url_or_key.split("s3://",1)
        b, k = rest.split("/",1)
        assert b == S3_BUCKET, "bucket mismatch"
        key = k
    else:
        key = url_or_key
    body = s3().get_object(Bucket=S3_BUCKET, Key=key)["Body"].read()
    import pyarrow.parquet as pq
    import pyarrow as pa
    table = pq.read_table(io.BytesIO(body))
    return table.to_pandas()

def load_graph():
    cur = json.loads(s3_get_text(f"{S3_PREFIX_GRAPH}/current.json"))
    nodes_df = s3_get_parquet_df(cur["nodes"])
    ed
