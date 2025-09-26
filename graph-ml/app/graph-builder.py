#!/usr/bin/env python3
import os, io, json, hashlib, itertools
from datetime import datetime, timezone
from collections import defaultdict

import pandas as pd
import pyarrow as pa, pyarrow.parquet as pq
import boto3
from botocore.config import Config

from kubernetes import client, config

# ---------- Config (env) ----------
S3_ENDPOINT = os.getenv("S3_ENDPOINT", "http://minio:9000")
S3_BUCKET   = os.getenv("S3_BUCKET", "rhoai-observ-model")
S3_PREFIX_GRAPH   = os.getenv("S3_PREFIX_GRAPH", "graph")
S3_SIGNALS_PREFIX = os.getenv("S3_SIGNALS_PREFIX", "")   # optional: signals/… to compute influence later
AWS_ACCESS_KEY_ID     = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_SESSION_TOKEN     = os.getenv("AWS_SESSION_TOKEN") or None
VERIFY_TLS = os.getenv("VERIFY_TLS","false").lower() not in ("0","false","no")

# ---------- S3 helpers ----------
def s3_client():
    if not AWS_ACCESS_KEY_ID or not AWS_SECRET_ACCESS_KEY:
        raise RuntimeError("Missing AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY")
    cfg = Config(signature_version="s3v4", s3={"addressing_style":"path"})
    return boto3.client("s3",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
        aws_session_token=AWS_SESSION_TOKEN,
        endpoint_url=(S3_ENDPOINT or None),
        config=cfg,
        verify=VERIFY_TLS
    )

def s3_put_bytes(key: str, data: bytes):
    s3_client().put_object(Bucket=S3_BUCKET, Key=key, Body=data)

# ---------- ID helpers ----------
def nid(kind: str, namespace: str, name: str):
    ns = namespace or ""
    return f"{kind.lower()}:{ns}/{name}"

def now_utc(): return datetime.now(timezone.utc)

# ---------- Collect graph ----------
def build_graph():
    # kube config
    try:
        config.load_incluster_config()
    except Exception:
        config.load_kube_config()

    v1  = client.CoreV1Api()
    disc= client.DiscoveryV1Api()
    apps= client.AppsV1Api()
    net = client.NetworkingV1Api()
    autos= client.AutoscalingV2Api()
    batch= client.BatchV1Api()

    nodes = {}           # node_id -> dict
    edges = []           # list of dicts

    # --- Namespaces ---
    for ns in v1.list_namespace().items:
        node_id = nid("namespace", "", ns.metadata.name)
        nodes[node_id] = {"node_id":node_id, "type":"Namespace", "namespace":"", "labels":json.dumps(ns.metadata.labels or {})}

    # --- Services & Endpoints/EndpointSlices -> Pod edges ---
    svcs = net.list_service_for_all_namespaces().items
    for s in svcs:
        sid = nid("service", s.metadata.namespace, s.metadata.name)
        nodes[sid] = {"node_id":sid, "type":"Service", "namespace":s.metadata.namespace, "labels":json.dumps(s.metadata.labels or {})}

    # Prefer EndpointSlices
    slices = []
    try:
        slices = disc.list_endpoint_slice_for_all_namespaces().items
    except Exception:
        pass
    if slices:
        for es in slices:
            ns = es.metadata.namespace
            # derive owning service from labels per k8s convention
            svc_name = (es.metadata.labels or {}).get("kubernetes.io/service-name")
            if not svc_name: continue
            sid = nid("service", ns, svc_name)
            for ep in es.endpoints or []:
                ref = getattr(ep, "target_ref", None)
                if ref and ref.kind == "Pod":
                    pid = nid("pod", ns, ref.name)
                    # ensure Pod node exists (minimal—enriched below)
                    nodes.setdefault(pid, {"node_id":pid, "type":"Pod", "namespace":ns, "labels":json.dumps({})})
                    edges.append({"src_id":sid, "dst_id":pid, "etype":"topology", "w":1.0, "lead_seconds":0, "features":json.dumps({})})
    else:
        # Fallback to Endpoints
        for ep in v1.list_endpoints_for_all_namespaces().items:
            ns = ep.metadata.namespace
            svc = ep.metadata.name
            sid = nid("service", ns, svc)
            for subset in ep.subsets or []:
                for addr in subset.addresses or []:
                    ref = addr.target_ref
                    if ref and ref.kind=="Pod":
                        pid = nid("pod", ns, ref.name)
                        nodes.setdefault(pid, {"node_id":pid, "type":"Pod", "namespace":ns, "labels":json.dumps({})})
                        edges.append({"src_id":sid, "dst_id":pid, "etype":"topology", "w":1.0, "lead_seconds":0, "features":json.dumps({})})

    # --- Workloads & ownership chains ---
    # Deployments -> ReplicaSets -> Pods
    rs_index = defaultdict(list)  # key: (ns, rs_name) -> pods
    for p in v1.list_pod_for_all_namespaces().items:
        pid = nid("pod", p.metadata.namespace, p.metadata.name)
        nodes[pid] = {"node_id":pid, "type":"Pod", "namespace":p.metadata.namespace, "labels":json.dumps(p.metadata.labels or {})}
        for o in p.metadata.owner_references or []:
            if o.kind=="ReplicaSet":
                rs_index[(p.metadata.namespace, o.name)].append(pid)

        # Pod -> ConfigMap/Secret/PVC
        for vol in (p.spec.volumes or []):
            if vol.config_map:
                cmid = nid("configmap", p.metadata.namespace, vol.config_map.name)
                nodes.setdefault(cmid, {"node_id":cmid,"type":"ConfigMap","namespace":p.metadata.namespace,"labels":json.dumps({})})
                edges.append({"src_id":pid,"dst_id":cmid,"etype":"config","w":1.0,"lead_seconds":0,"features":json.dumps({"mount":"volume"})})
            if vol.secret:
                sid = nid("secret", p.metadata.namespace, vol.secret.secret_name)
                nodes.setdefault(sid, {"node_id":sid,"type":"Secret","namespace":p.metadata.namespace,"labels":json.dumps({})})
                edges.append({"src_id":pid,"dst_id":sid,"etype":"config","w":1.0,"lead_seconds":0,"features":json.dumps({"mount":"volume"})})
            if vol.persistent_volume_claim:
                pc = vol.persistent_volume_claim.claim_name
                pcid = nid("pvc", p.metadata.namespace, pc)
                nodes.setdefault(pcid, {"node_id":pcid,"type":"PVC","namespace":p.metadata.namespace,"labels":json.dumps({})})
                edges.append({"src_id":pid,"dst_id":pcid,"etype":"storage","w":1.0,"lead_seconds":0,"features":json.dumps({})})

        # envFrom / valueFrom refs
        for c in p.spec.containers or []:
            for env in (c.env or []):
                if env.value_from:
                    if env.value_from.config_map_key_ref:
                        cmid = nid("configmap", p.metadata.namespace, env.value_from.config_map_key_ref.name)
                        nodes.setdefault(cmid, {"node_id":cmid,"type":"ConfigMap","namespace":p.metadata.namespace,"labels":json.dumps({})})
                        edges.append({"src_id":pid,"dst_id":cmid,"etype":"config","w":1.0,"lead_seconds":0,"features":json.dumps({"env":"keyRef"})})
                    if env.value_from.secret_key_ref:
                        sid = nid("secret", p.metadata.namespace, env.value_from.secret_key_ref.name)
                        nodes.setdefault(sid, {"node_id":sid,"type":"Secret","namespace":p.metadata.namespace,"labels":json.dumps({})})
                        edges.append({"src_id":pid,"dst_id":sid,"etype":"config","w":1.0,"lead_seconds":0,"features":json.dumps({"env":"keyRef"})})

    # Deployments
    for d in apps.list_deployment_for_all_namespaces().items:
        did = nid("deployment", d.metadata.namespace, d.metadata.name)
        nodes[did] = {"node_id":did, "type":"Deployment", "namespace":d.metadata.namespace, "labels":json.dumps(d.metadata.labels or {})}
    # ReplicaSets -> Pods + Deployment -> ReplicaSet
    for rs in apps.list_replica_set_for_all_namespaces().items:
        rid = nid("replicaset", rs.metadata.namespace, rs.metadata.name)
        nodes[rid] = {"node_id":rid,"type":"ReplicaSet","namespace":rs.metadata.namespace,"labels":json.dumps(rs.metadata.labels or {})}
        # owner: Deployment
        for o in rs.metadata.owner_references or []:
            if o.kind=="Deployment":
                did = nid("deployment", rs.metadata.namespace, o.name)
                edges.append({"src_id":did,"dst_id":rid,"etype":"change","w":1.0,"lead_seconds":0,"features":json.dumps({"owner":True})})
        # rs -> pods
        for pid in rs_index.get((rs.metadata.namespace, rs.metadata.name), []):
            edges.append({"src_id":rid,"dst_id":pid,"etype":"topology","w":1.0,"lead_seconds":0,"features":json.dumps({})})

    # StatefulSets -> Pods
    for ss in apps.list_stateful_set_for_all_namespaces().items:
        sid = nid("statefulset", ss.metadata.namespace, ss.metadata.name)
        nodes[sid] = {"node_id":sid,"type":"StatefulSet","namespace":ss.metadata.namespace,"labels":json.dumps(ss.metadata.labels or {})}
        # pods owned directly
        # (owner refs already on pods; we don’t need explicit mapping here)

    # DaemonSets
    for ds in apps.list_daemon_set_for_all_namespaces().items:
        did = nid("daemonset", ds.metadata.namespace, ds.metadata.name)
        nodes[did] = {"node_id":did,"type":"DaemonSet","namespace":ds.metadata.namespace,"labels":json.dumps(ds.metadata.labels or {})}

    # Ingress -> Service
    for ing in net.list_ingress_for_all_namespaces().items:
        iid = nid("ingress", ing.metadata.namespace, ing.metadata.name)
        nodes[iid] = {"node_id":iid,"type":"Ingress","namespace":ing.metadata.namespace,"labels":json.dumps(ing.metadata.labels or {})}
        spec = ing.spec
        if spec and spec.rules:
            for r in spec.rules:
                if r.http and r.http.paths:
                    for p in r.http.paths:
                        if p.backend and p.backend.service and p.backend.service.name:
                            sid = nid("service", ing.metadata.namespace, p.backend.service.name)
                            edges.append({"src_id":iid,"dst_id":sid,"etype":"topology","w":1.0,"lead_seconds":0,"features":json.dumps({"path": p.path or "/"})})

    # HPA -> target (Deployment/StatefulSet)
    try:
        for h in autos.list_horizontal_pod_autoscaler_for_all_namespaces().items:
            tid = nid(h.spec.scale_target_ref.kind, h.metadata.namespace, h.spec.scale_target_ref.name)
            hid = nid("hpa", h.metadata.namespace, h.metadata.name)
            nodes[hid] = {"node_id":hid,"type":"HPA","namespace":h.metadata.namespace,"labels":json.dumps(h.metadata.labels or {})}
            nodes.setdefault(tid, {"node_id":tid,"type":h.spec.scale_target_ref.kind,"namespace":h.metadata.namespace,"labels":json.dumps({})})
            edges.append({"src_id":hid,"dst_id":tid,"etype":"control","w":0.8,"lead_seconds":0,"features":json.dumps({})})
    except Exception:
        pass

    # Jobs/CronJobs
    for cj in batch.list_cron_job_for_all_namespaces().items:
        cjid = nid("cronjob", cj.metadata.namespace, cj.metadata.name)
        nodes[cjid] = {"node_id":cjid,"type":"CronJob","namespace":cj.metadata.namespace,"labels":json.dumps(cj.metadata.labels or {})}
    for j in batch.list_job_for_all_namespaces().items:
        jid = nid("job", j.metadata.namespace, j.metadata.name)
        nodes[jid] = {"node_id":jid,"type":"Job","namespace":j.metadata.namespace,"labels":json.dumps(j.metadata.labels or {})}
        for o in j.metadata.owner_references or []:
            if o.kind=="CronJob":
                cjid = nid("cronjob", j.metadata.namespace, o.name)
                edges.append({"src_id":cjid,"dst_id":jid,"etype":"topology","w":1.0,"lead_seconds":0,"features":json.dumps({})})

    # (Optional) influence weights can be computed later using S3_SIGNALS_PREFIX

    # DataFrames
    nodes_df = pd.DataFrame.from_records(list(nodes.values()))
    edges_df = pd.DataFrame.from_records(edges)

    return nodes_df, edges_df

def to_parquet_bytes(df: pd.DataFrame) -> bytes:
    # cast objects to string
    df = df.copy()
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype("string")
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(table, buf, compression="ZSTD")
    return buf.getvalue()

def main():
    nodes_df, edges_df = build_graph()
    ts = now_utc()
    date_part = ts.strftime("%Y-%m-%d")
    hour_part = ts.strftime("%H")
    prefix = f"{S3_PREFIX_GRAPH}/date={date_part}/hour={hour_part}"
    nodes_key = f"{prefix}/nodes.parquet"
    edges_key = f"{prefix}/edges.parquet"

    s3_put_bytes(nodes_key, to_parquet_bytes(nodes_df))
    s3_put_bytes(edges_key, to_parquet_bytes(edges_df))

    # write/update pointer
    current = {"nodes": f"s3://{S3_BUCKET}/{nodes_key}",
               "edges": f"s3://{S3_BUCKET}/{edges_key}",
               "generated_at": ts.isoformat()}
    s3_put_bytes(f"{S3_PREFIX_GRAPH}/current.json", json.dumps(current, indent=2).encode())
    print(f"[OK] nodes={len(nodes_df):,}, edges={len(edges_df):,} -> s3://{S3_BUCKET}/{prefix}/ (pointer updated)")

if __name__ == "__main__":
    main()
