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
    edges_df = s3_get_parquet_df(cur["edges"])
    return nodes_df, edges_df

# cache in memory
NODES_DF, EDGES_DF = None, None

@app.on_event("startup")
def startup():
    global NODES_DF, EDGES_DF
    NODES_DF, EDGES_DF = load_graph()

@app.post("/reload")
def reload_graph():
    global NODES_DF, EDGES_DF
    NODES_DF, EDGES_DF = load_graph()
    return {"ok": True, "nodes": len(NODES_DF), "edges": len(EDGES_DF)}

def rank_roots(anomalies: Dict[str,float], topk:int=5):
    # build adjacency (src->dst) weights
    nodes = list(NODES_DF["node_id"])
    idx = {n:i for i,n in enumerate(nodes)}
    A = np.zeros((len(nodes), len(nodes)))
    for _,e in EDGES_DF.iterrows():
        si = idx.get(e["src_id"]); di = idx.get(e["dst_id"])
        if si is None or di is None: continue
        w = float(e.get("w",1.0))
        A[si,di] = max(A[si,di], w)

    # initial anomaly vector
    x0 = np.zeros(len(nodes))
    for n,sc in anomalies.items():
        i = idx.get(n)
        if i is not None:
            x0[i] = float(sc)

    # reverse-propagate along incoming edges (transpose), damped
    alpha = 0.15
    Mt = A.T
    colsum = Mt.sum(axis=0, keepdims=True)
    Mt = np.divide(Mt, colsum, out=np.zeros_like(Mt), where=colsum!=0)

    x = x0.copy()
    for _ in range(5):
        x = alpha*x0 + (1-alpha) * Mt.dot(x)

    # prepare ranking
    order = np.argsort(-x)[:topk]
    scores = x[order]
    ids = [nodes[i] for i in order]
    # basic explanations: top outgoing edges & observed anomaly on dst
    expl = []
    for i,n in zip(order, ids):
        outs = np.where(A[i,:]>0)[0]
        outs = sorted(outs, key=lambda j: -A[i,j])[:5]
        outs_nodes = [(nodes[j], float(A[i,j])) for j in outs]
        expl.append({"node_id": n, "score": float(x[i]), "out_edges": outs_nodes})
    return expl

@app.post("/rank")
def api_rank(payload: Dict = Body(...)):
    """
    payload: {"anomalies": {"node_id": score, ...}, "topk": 5}
    """
    anomalies = payload.get("anomalies") or {}
    topk = int(payload.get("topk", 5))
    if not anomalies:
        raise HTTPException(400, "Provide anomalies dict: {node_id: score}")
    return {"topk": rank_roots(anomalies, topk)}
