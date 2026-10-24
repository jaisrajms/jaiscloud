#!/usr/bin/env python3
"""crypto-medallion demo orchestrator (design §5).

One CLI that walks the demo's stage order against a k3d jaiscloud-gcp
deployment: provision -> ingest -> process -> serve -> observe -> automate,
plus reset and the recording helpers.

    python3 demo/crypto-medallion/scripts/democtl.py <stage> [opts]

Stages
    provision   create the project, apply Terraform, create the data-platform
                resources (Managed Kafka, Metastore, Dataproc, BigQuery,
                Firestore) and write the runtime state (design §5.1).
    bridge      build/push the WS->Kafka bridge image and roll the Deployment
                (design §5.2).
    process     stage the Iceberg/Kafka jars + PySpark driver on GCS and submit
                the always-on medallion streaming job (design §5.3).
    rollup      submit the periodic Spark SQL rollup once (design §5.3/§5.4).
    serve       Cloud Run publisher + leaderboard, Eventarc trigger, Scheduler
                job and the in-cluster submitter (design §5.4).
    observe     custom metric + alert policy + notification channel (design §5.5).
    automate    tick the emulator's Scheduler clock once (design §5.6).
    up          provision + bridge + process + serve (+observe).
    status      print the live state of every demo resource.
    reset       tear the demo down and reset the emulator (design §12).

Everything is idempotent: re-running a stage reconciles, never duplicates.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shutil
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

# ── locations ────────────────────────────────────────────────────────────────
DEMO_DIR = Path(__file__).resolve().parent.parent
REPO_ROOT = DEMO_DIR.parent.parent
TF_DIR = DEMO_DIR / "terraform"
SPARK_DIR = DEMO_DIR / "spark"
IMAGES_DIR = DEMO_DIR / "images"
K8S_DIR = DEMO_DIR / "k8s"
STATE_FILE = DEMO_DIR / ".state.json"
JARS_DIR = SPARK_DIR / "jars"

# ── fixed demo config (design §3 locked decisions) ───────────────────────────
PROJECT = os.environ.get("DEMO_PROJECT", "crypto-medallion")
REGION = os.environ.get("DEMO_REGION", "us-central1")
ZONE = os.environ.get("DEMO_ZONE", "us-central1-a")
NS = os.environ.get("K8S_NAMESPACE", "jaiscloud")
KAFKA_CLUSTER = "cm-trades"
KAFKA_TOPIC = "trades"
METASTORE_SERVICE = "cm-hms"
DATAPROC_CLUSTER = "cm-spark"
GKE_TARGET = f"projects/{PROJECT}/locations/{REGION}/clusters/cm-gke"
HMS = f"jaiscloud-gcp.{NS}.svc.cluster.local:9083"
IN_CLUSTER_EMULATOR = f"http://jaiscloud-gcp.{NS}.svc.cluster.local:8080"
ICE_DB = "medallion"
ICE_TABLE = "gold_ohlc"
BQ_DATASET = "crypto_medallion"
BQ_TABLE = "trades_rollup"
FIRESTORE_COLLECTION = "leaderboard"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT"]

RUN_PUBLISHER = "cm-publisher"
RUN_LEADERBOARD = "cm-leaderboard"
SUBMITTER = "cm-submitter"
SCHEDULER_JOB = "cm-rollup"
EVENTARC_TRIGGER = "cm-leaderboard-live"
METRIC_TYPE = "custom.googleapis.com/crypto_medallion/trades_per_min"
ALERT_POLICY = "cm-trades-alert"
NOTIFICATION_CHANNEL = "cm-incidents"

DOCKER_REGISTRY = os.environ.get("GCP_REGISTRY", "10.0.100.21:5050")
IMAGE_TAG = os.environ.get("DEMO_IMAGE_TAG", "demo")

# jars staged on GCS for the Spark job (design §3 / SPK1 / SPK2).
ICEBERG_JAR = "iceberg-spark-runtime-3.5_2.12-1.5.2.jar"
KAFKA_JARS = [
    "spark-sql-kafka-0-10_2.12-3.5.0.jar",
    "spark-token-provider-kafka-0-10_2.12-3.5.0.jar",
    "kafka-clients-3.4.1.jar",
    "commons-pool2-2.11.1.jar",
]


# ── tiny process helpers ─────────────────────────────────────────────────────
def log(msg: str) -> None:
    print(f"\033[1m==> {msg}\033[0m", flush=True)


def info(msg: str) -> None:
    print(f"    {msg}", flush=True)


def run(cmd, *, check=True, capture=True, env=None, input_text=None):
    """Run a command, returning its stdout (trimmed) when capture is set."""
    e = dict(os.environ)
    if env:
        e.update(env)
    if isinstance(cmd, str):
        cmd = cmd.split()
    proc = subprocess.run(
        cmd, capture_output=capture, text=True, env=e, input=input_text
    )
    if check and proc.returncode != 0:
        if capture:
            sys.stderr.write(proc.stdout or "")
            sys.stderr.write(proc.stderr or "")
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return (proc.stdout or "").strip() if capture else ""


def have(binary: str) -> bool:
    return shutil.which(binary) is not None


# ── emulator REST client ─────────────────────────────────────────────────────
class Emulator:
    """Thin JSON client for the jaiscloud-gcp REST API (host side)."""

    def __init__(self, base: str):
        self.base = base.rstrip("/")

    def request(self, method: str, path: str, body=None, *, raw=None,
                content_type="application/json", expect=(200, 201, 204, 409, 404)):
        url = self.base + path
        if raw is not None:
            data = raw
        elif body is not None:
            data = json.dumps(body).encode()
        else:
            data = None
        req = urllib.request.Request(url, data=data, method=method)
        req.add_header("Authorization", "Bearer demo-token")
        if data is not None:
            req.add_header("Content-Type", content_type)
        try:
            with urllib.request.urlopen(req, timeout=120) as resp:
                payload = resp.read()
                status = resp.status
        except urllib.error.HTTPError as exc:
            payload = exc.read()
            status = exc.code
        if status not in expect:
            raise SystemExit(
                f"{method} {path} -> HTTP {status}: {payload[:400].decode(errors='replace')}"
            )
        if not payload:
            return None, status
        try:
            return json.loads(payload), status
        except json.JSONDecodeError:
            return payload.decode(errors="replace"), status

    def get(self, path, **kw):
        return self.request("GET", path, **kw)

    def post(self, path, body=None, **kw):
        return self.request("POST", path, body, **kw)

    def patch(self, path, body=None, **kw):
        return self.request("PATCH", path, body, **kw)

    def put(self, path, body=None, **kw):
        return self.request("PUT", path, body, **kw)

    def delete(self, path, **kw):
        return self.request("DELETE", path, **kw)


def wait_healthy(base: str, timeout=60) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(base + "/_jaiscloud/health", timeout=3) as r:
                if r.status == 200:
                    return
        except Exception:
            pass
        time.sleep(1)
    raise SystemExit(f"emulator not healthy at {base}")


def ensure_port_forward(local_port: int, remote_port: int = 8080) -> str:
    """Return a host REST base, starting a port-forward if one is not already up."""
    base = f"http://localhost:{local_port}"
    try:
        with urllib.request.urlopen(base + "/_jaiscloud/health", timeout=3) as r:
            if r.status == 200:
                return base
    except Exception:
        pass
    log(f"port-forward svc/jaiscloud-gcp {local_port}:{remote_port}")
    subprocess.Popen(
        ["kubectl", "-n", NS, "port-forward", f"svc/jaiscloud-gcp",
         f"{local_port}:{remote_port}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    wait_healthy(base, timeout=40)
    return base


# ── gcloud wrapper (endpoint overrides for the emulator) ─────────────────────
def gcloud_env(endpoint: str) -> dict:
    cfg = "/tmp/opencode/gcloud-demo"
    os.makedirs(cfg, exist_ok=True)
    overrides = {
        "CLOUDRESOURCEMANAGER": f"{endpoint}/",
        "SERVICEUSAGE": f"{endpoint}/",
        "DATAPROC": f"{endpoint}/",
        "MANAGEDKAFKA": f"{endpoint}/",
        "METASTORE": f"{endpoint}/",
        "EVENTARC": f"{endpoint}/",
        "CLOUDSCHEDULER": f"{endpoint}/",
        "STORAGE": f"{endpoint}/storage/v1/",
        "PUBSUB": f"{endpoint}/v1/",
        "SECRETMANAGER": f"{endpoint}/v1/",
        "CLOUDKMS": f"{endpoint}/v1/",
        "IAM": f"{endpoint}/",
        "FIRESTORE": f"{endpoint}/",
    }
    env = {
        "CLOUDSDK_CONFIG": cfg,
        "CLOUDSDK_CORE_DISABLE_PROMPTS": "1",
        "CLOUDSDK_CORE_DISABLE_USAGE_REPORTING": "true",
        "CLOUDSDK_AUTH_ACCESS_TOKEN": os.environ.get("GOOGLE_OAUTH_ACCESS_TOKEN", "demo-token"),
        "CLOUDSDK_CORE_PROJECT": PROJECT,
    }
    for k, v in overrides.items():
        env[f"CLOUDSDK_API_ENDPOINT_OVERRIDES_{k}"] = v
    return env


def gcloud(endpoint: str, args, *, check=True, input_text=None):
    return run(["gcloud", *args], env=gcloud_env(endpoint), check=check,
               input_text=input_text)


# ── state ────────────────────────────────────────────────────────────────────
def load_state() -> dict:
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return {}


def save_state(state: dict) -> None:
    STATE_FILE.write_text(json.dumps(state, indent=2, sort_keys=True) + "\n")


def state_emulator(args) -> tuple[Emulator, dict]:
    """Load state and return an emulator client with the REST forward ensured."""
    state = load_state()
    base = ensure_port_forward(getattr(args, "port", 18080))
    state["endpoint"] = base
    return Emulator(base), state


# ── stage 1: provision ───────────────────────────────────────────────────────
def ensure_project(emu: Emulator) -> None:
    log(f"project {PROJECT}")
    out = gcloud(emu.base, ["projects", "describe", PROJECT], check=False)
    if PROJECT in out or "projectId" in out:
        info(f"{PROJECT} exists")
        return
    gcloud(emu.base, ["projects", "create", PROJECT, "--name", "Crypto Medallion Demo"])


def terraform_apply(emu: Emulator) -> dict:
    log("terraform infra subset (design §5.1)")
    env = {"GOOGLE_OAUTH_ACCESS_TOKEN": "demo-token", "GOOGLE_CLOUD_PROJECT": PROJECT}
    tf = "terraform"
    run([tf, f"-chdir={TF_DIR}", "init", "-input=false", "-no-color"], env=env)
    run([tf, f"-chdir={TF_DIR}", "apply", "-input=false", "-auto-approve",
         "-no-color", f"-var=endpoint={emu.base}", f"-var=project={PROJECT}",
         f"-var=region={REGION}"], env=env)
    out = json.loads(run([tf, f"-chdir={TF_DIR}", "output", "-json"], env=env))
    return {k: v["value"] for k, v in out.items()}


def ensure_metastore(emu: Emulator) -> str:
    name = f"projects/{PROJECT}/locations/{REGION}/services/{METASTORE_SERVICE}"
    log(f"Dataproc Metastore {METASTORE_SERVICE}")
    body = {"hiveMetastoreConfig": {"endpointProtocol": "THRIFT"}}
    emu.post(f"/v1/projects/{PROJECT}/locations/{REGION}/services"
             f"?serviceId={METASTORE_SERVICE}", body)
    info(name)
    return name


def ensure_kafka(emu: Emulator) -> dict:
    log(f"Managed Kafka cluster {KAFKA_CLUSTER} + topic {KAFKA_TOPIC}")
    emu.post(f"/v1/projects/{PROJECT}/locations/{REGION}/clusters"
             f"?clusterId={KAFKA_CLUSTER}", {})
    cluster, _ = emu.get(f"/v1/projects/{PROJECT}/locations/{REGION}/clusters/{KAFKA_CLUSTER}")
    bootstrap = cluster.get("bootstrapAddress", "")
    emu.post(f"/v1/projects/{PROJECT}/locations/{REGION}/clusters/{KAFKA_CLUSTER}"
             f"/topics?topicId={KAFKA_TOPIC}",
             {"partitionCount": 3, "replicationFactor": 1})
    info(f"bootstrapAddress={bootstrap}")
    return {"cluster": KAFKA_CLUSTER, "topic": KAFKA_TOPIC, "bootstrap": bootstrap}


def ensure_bigquery(emu: Emulator) -> None:
    log(f"BigQuery dataset {BQ_DATASET}.{BQ_TABLE}")
    emu.post(f"/bigquery/v2/projects/{PROJECT}/datasets",
             {"datasetReference": {"projectId": PROJECT, "datasetId": BQ_DATASET}})
    schema = {"fields": [
        {"name": "symbol", "type": "STRING", "mode": "REQUIRED"},
        {"name": "vwap", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "volume", "type": "FLOAT", "mode": "NULLABLE"},
        {"name": "trades", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "window_start", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "window_end", "type": "TIMESTAMP", "mode": "NULLABLE"},
        {"name": "updated_at", "type": "TIMESTAMP", "mode": "NULLABLE"},
    ]}
    emu.post(f"/bigquery/v2/projects/{PROJECT}/datasets/{BQ_DATASET}/tables",
             {"tableReference": {"projectId": PROJECT, "datasetId": BQ_DATASET,
                                 "tableId": BQ_TABLE},
              "schema": schema})


def configure_emulator() -> None:
    """Apply the demo's combined emulator config (design §5, SPK6 baseline)."""
    log("configure the emulator (k8s Spark + Kafka + Cloud Run, throttle off)")
    kubectl(["set", "env", "deployment/jaiscloud-gcp",
             "JAISCLOUD_SPARK_EXECUTOR_MODE=k8s",
             "JAISCLOUD_KAFKA_BROKER_MODE=k8s",
             "JAISCLOUD_CLOUDRUN_EXECUTOR_MODE=k8s",
             "JAISCLOUD_PLATFORM_TLS_ENABLED=false",
             "JAISCLOUD_GCP_THROTTLE="])
    kubectl(["rollout", "status", "deployment/jaiscloud-gcp", "--timeout=240s"])


def stage_provision(args) -> None:
    emu = Emulator(ensure_port_forward(args.port))
    ensure_project(emu)
    configure_emulator()
    outputs = terraform_apply(emu)
    ensure_metastore(emu)
    kafka = ensure_kafka(emu)
    ensure_bigquery(emu)
    state = load_state()
    state.update({
        "endpoint": emu.base,
        "project": PROJECT,
        "region": REGION,
        "namespace": NS,
        "buckets": outputs["buckets"],
        "service_account": outputs["service_account_email"],
        "feed_secret": outputs["feed_secret_id"],
        "events_topic": outputs["events_topic"],
        "events_subscription": outputs["events_subscription"],
        "kafka": kafka,
        "hms_service": f"projects/{PROJECT}/locations/{REGION}/services/{METASTORE_SERVICE}",
        "dataproc_cluster": DATAPROC_CLUSTER,
    })
    save_state(state)
    log("provision complete")
    info(f"state -> {STATE_FILE}")


# ── object store helpers ─────────────────────────────────────────────────────
def gcs_upload_object(emu: Emulator, bucket: str, name: str, data: bytes,
                      content_type="application/octet-stream") -> None:
    q = urllib.parse.urlencode({"uploadType": "media", "name": name})
    emu.post(f"/upload/storage/v1/b/{bucket}/o?{q}", raw=data,
             content_type=content_type, expect=(200, 201))


def gcs_read_object(emu: Emulator, bucket: str, name: str):
    obj, _ = emu.get(f"/storage/v1/b/{bucket}/o/{urllib.parse.quote(name, safe='')}?alt=media")
    return obj


# ── stage 3 helpers: stage jars + driver on GCS ──────────────────────────────
def stage_spark_assets(emu: Emulator, state: dict) -> dict:
    jobs_bucket = state["buckets"]["jobs"]
    log("stage Iceberg + Kafka jars and the PySpark driver on GCS")
    jar_names = [ICEBERG_JAR, *KAFKA_JARS]
    jar_uris = []
    for name in jar_names:
        local = JARS_DIR / name
        if not local.exists():
            raise SystemExit(f"missing {local} — run demo/crypto-medallion/spark/download-jars.sh")
        gcs_upload_object(emu, jobs_bucket, f"jars/{name}", local.read_bytes(),
                          "application/java-archive")
        jar_uris.append(f"gs://{jobs_bucket}/jars/{name}")
        info(f"gs://{jobs_bucket}/jars/{name}")
    driver = (SPARK_DIR / "stream_medallion.py").read_bytes()
    gcs_upload_object(emu, jobs_bucket, "jobs/stream_medallion.py", driver, "text/x-python")
    return {
        "jar_uris": jar_uris,
        "driver_uri": f"gs://{jobs_bucket}/jobs/stream_medallion.py",
    }


def ensure_dataproc_cluster(emu: Emulator) -> None:
    path = f"/v1/projects/{PROJECT}/regions/{REGION}/clusters/{DATAPROC_CLUSTER}"
    body, status = emu.get(path, expect=(200, 404))
    if status == 200:
        info(f"cluster {DATAPROC_CLUSTER} exists")
        return
    log(f"Dataproc GKE-virtual cluster {DATAPROC_CLUSTER}")
    payload = {
        "clusterName": DATAPROC_CLUSTER,
        "virtualClusterConfig": {
            "kubernetesClusterConfig": {
                "gkeClusterConfig": {"gkeClusterTarget": GKE_TARGET}
            },
            "auxiliaryServicesConfig": {
                "metastoreConfig": {
                    "dataprocMetastoreService":
                        f"projects/{PROJECT}/locations/{REGION}/services/{METASTORE_SERVICE}"
                }
            },
        },
    }
    emu.post(f"/v1/projects/{PROJECT}/regions/{REGION}/clusters", payload)
    for _ in range(60):
        c, _ = emu.get(path, expect=(200, 404))
        if c and c.get("status", {}).get("state") in ("RUNNING", "ERROR"):
            if c["status"]["state"] == "ERROR":
                raise SystemExit("dataproc cluster entered ERROR")
            return
        time.sleep(2)
    raise SystemExit("dataproc cluster did not reach RUNNING")


def spark_properties(state: dict, extra: dict | None = None) -> dict:
    jobs = state["buckets"]["jobs"]
    gold = state["buckets"]["gold"]
    props = {
        "spark.sql.catalog.hms": "org.apache.iceberg.spark.SparkCatalog",
        "spark.sql.catalog.hms.catalog-impl": "org.apache.iceberg.hive.HiveCatalog",
        "spark.sql.catalog.hms.uri": f"thrift://{HMS}",
        "spark.sql.catalog.hms.warehouse": f"gs://{gold}/warehouse/",
        "spark.sql.catalog.hms.io-impl": "org.apache.iceberg.hadoop.HadoopFileIO",
        "spark.driver.memory": "450m",
        "spark.driver.memoryOverhead": "768m",
        "spark.executor.memory": "512m",
        "spark.executor.memoryOverhead": "1g",
        "spark.executor.instances": "1",
        "spark.sql.shuffle.partitions": "1",
        "spark.sql.streaming.checkpointLocation": f"gs://{jobs}/checkpoints/",
    }
    if extra:
        props.update(extra)
    return props


def submit_pyspark(emu: Emulator, job_id: str, main_uri: str, jar_uris: list,
                   props: dict, args: list | None = None) -> dict:
    payload = {"job": {
        "reference": {"jobId": job_id},
        "placement": {"clusterName": DATAPROC_CLUSTER},
        "pysparkJob": {
            "mainPythonFileUri": main_uri,
            "jarFileUris": jar_uris,
            "properties": props,
            "args": args or [],
        },
    }}
    resp, _ = emu.post(f"/v1/projects/{PROJECT}/regions/{REGION}/jobs:submit", payload,
                       expect=(200, 201))
    return resp


def submit_sparksql(emu: Emulator, job_id: str, queries: list, jar_uris: list,
                    props: dict) -> dict:
    payload = {"job": {
        "reference": {"jobId": job_id},
        "placement": {"clusterName": DATAPROC_CLUSTER},
        "sparkSqlJob": {
            "queryList": {"queries": queries},
            "jarFileUris": jar_uris,
            "properties": props,
        },
    }}
    resp, _ = emu.post(f"/v1/projects/{PROJECT}/regions/{REGION}/jobs:submit", payload,
                       expect=(200, 201))
    return resp


def job_state(emu: Emulator, job_id: str) -> str:
    resp, status = emu.get(f"/v1/projects/{PROJECT}/regions/{REGION}/jobs/{job_id}",
                           expect=(200, 404))
    if status != 200 or not resp:
        return ""
    return resp.get("status", {}).get("state", "")


def stage_process(args) -> None:
    emu, _ = state_emulator(args)
    state = load_state()
    assets = stage_spark_assets(emu, state)
    ensure_dataproc_cluster(emu)

    buckets = state["buckets"]
    stream_args = [
        "--bootstrap", state["kafka"]["bootstrap"],
        "--topic", state["kafka"]["topic"],
        "--bronze", f"gs://{buckets['bronze']}/trades",
        "--silver", f"gs://{buckets['silver']}/trades",
        "--quarantine", f"gs://{buckets['silver']}/quarantine",
        "--leaderboard", f"gs://{buckets['leaderboard']}/latest.json",
        "--iceberg-db", ICE_DB,
        "--iceberg-table", ICE_TABLE,
        "--group", "cm-medallion",
    ]
    job_id = f"cm-streaming-{int(time.time())}"
    log(f"submit streaming job {job_id}")
    submit_pyspark(emu, job_id, assets["driver_uri"], assets["jar_uris"],
                   spark_properties(state), stream_args)
    state["spark"] = assets
    state["streaming_job"] = job_id
    save_state(state)


def rollup_queries(state: dict) -> list:
    rollup = f"gs://{state['buckets']['rollup']}/rollup"
    return [
        f"""INSERT OVERWRITE DIRECTORY '{rollup}' USING json
SELECT symbol,
       ROUND(MAX(vwap), 4) AS vwap,
       ROUND(SUM(volume), 6) AS volume,
       SUM(trades) AS trades,
       MIN(window_start) AS window_start,
       MAX(window_end) AS window_end
FROM hms.{ICE_DB}.{ICE_TABLE}
GROUP BY symbol
ORDER BY volume DESC""",
    ]


def stage_rollup(args) -> None:
    emu, _ = state_emulator(args)
    state = load_state()
    assets = state.get("spark") or stage_spark_assets(emu, state)
    ensure_dataproc_cluster(emu)
    job_id = f"cm-rollup-{int(time.time())}"
    log(f"submit Spark SQL rollup {job_id}")
    submit_sparksql(emu, job_id, rollup_queries(state), assets["jar_uris"],
                    spark_properties(state))


# ── image build/push ─────────────────────────────────────────────────────────
def build_image(name: str, dockerfile: Path, context: Path, sources: list) -> str:
    """Build+push an image tagged by the content hash of its sources.

    k8s defaults to IfNotPresent for a non-:latest tag, so a rebuilt image under
    a fixed tag would keep running the cached old layer. A content hash forces a
    fresh pull exactly when the sources change, and stays stable when they do not.
    """
    digest = hashlib.sha1()
    for src in sources:
        digest.update(Path(src).read_bytes())
    image = f"{DOCKER_REGISTRY}/{name}:demo-{digest.hexdigest()[:12]}"
    log(f"build+push {image}")
    run(["docker", "build", "-t", image, "-f", str(dockerfile), str(context)],
        capture=False)
    run(["docker", "push", image], capture=False)
    return image


def stage_bridge(args) -> None:
    state = load_state()
    emu, _ = state_emulator(args)
    image = build_image("crypto-medallion-bridge",
                        IMAGES_DIR / "bridge" / "Dockerfile", DEMO_DIR,
                        [IMAGES_DIR / "bridge" / "Dockerfile",
                         IMAGES_DIR / "bridge" / "bridge.py",
                         IMAGES_DIR / "bridge" / "requirements.txt",
                         DEMO_DIR / "feed" / "replay.jsonl.gz"])
    state["images"] = {**state.get("images", {}), "bridge": image}
    save_state(state)
    render_k8s(emu, "bridge.yaml", state, extra={"BRIDGE_IMAGE": image})
    kubectl(["rollout", "status", "deployment/cm-bridge", "--timeout=180s"])


# ── k8s rendering ────────────────────────────────────────────────────────────
def kubectl(args, *, check=True, input_text=None):
    return run(["kubectl", "-n", NS, *args], check=check, input_text=input_text)


def render_k8s(emu: Emulator, manifest: str, state: dict, extra: dict | None = None) -> None:
    text = (K8S_DIR / manifest).read_text()
    subs = {
        "PROJECT": PROJECT,
        "REGION": REGION,
        "NAMESPACE": NS,
        "EMULATOR": IN_CLUSTER_EMULATOR,
        "KAFKA_BOOTSTRAP": state["kafka"]["bootstrap"],
        "KAFKA_TOPIC": state["kafka"]["topic"],
        "CAPTURE_BUCKET": state["buckets"]["capture"],
        "FEED_SECRET": state["feed_secret"],
        "SERVICE_ACCOUNT": state["service_account"],
    }
    if extra:
        subs.update(extra)
    for key, value in subs.items():
        text = text.replace(f"__{key}__", str(value))
    kubectl(["apply", "-f", "-"], input_text=text)


# ── stage 4: serve ───────────────────────────────────────────────────────────
def deploy_run_service(emu: Emulator, service_id: str, image: str, port: int,
                       env: dict) -> str:
    path = f"/v2/projects/{PROJECT}/locations/{REGION}/services"
    env_list = [{"name": k, "value": str(v)} for k, v in env.items()]
    body = {"template": {"containers": [{
        "image": image, "ports": [{"containerPort": port}], "env": env_list,
    }]}}
    existing, status = emu.get(f"{path}/{service_id}", expect=(200, 404))
    if status == 200:
        # Update in place so the emulator rolls a new revision instead of a
        # delete/create race that can reap the fresh pod.
        emu.patch(f"{path}/{service_id}?updateMask=template", body)
    else:
        emu.post(f"{path}?serviceId={service_id}", body)
    return wait_run_ready(emu, service_id)


def wait_run_ready(emu: Emulator, service_id: str, timeout=120) -> str:
    path = f"/v2/projects/{PROJECT}/locations/{REGION}/services/{service_id}"
    deadline = time.time() + timeout
    last = ""
    while time.time() < deadline:
        svc, _ = emu.get(path, expect=(200, 404))
        if svc:
            last = svc.get("uri", "")
            cond = svc.get("terminalCondition", {})
            if last and cond.get("state") == "CONDITION_SUCCEEDED":
                return last
            if cond.get("state") in ("CONDITION_FAILED",):
                raise SystemExit(f"Cloud Run service {service_id} not ready: {cond}")
        time.sleep(2)
    raise SystemExit(f"Cloud Run service {service_id} not ready (uri={last})")


def run_authority(emu: Emulator) -> str:
    resp, _ = emu.get(f"/v2/projects/{PROJECT}/locations/{REGION}/services/{RUN_LEADERBOARD}")
    return resp.get("uri", "")


def ensure_eventarc_trigger(emu: Emulator) -> None:
    log(f"Eventarc trigger {EVENTARC_TRIGGER} (GCS finalize -> publisher)")
    path = f"/v1/projects/{PROJECT}/locations/{REGION}/triggers"
    emu.delete(f"{path}/{EVENTARC_TRIGGER}", expect=(200, 204, 404))
    body = {
        "destination": {"cloudRun": {
            "service": f"projects/{PROJECT}/locations/{REGION}/services/{RUN_PUBLISHER}",
            "region": REGION,
        }},
        "eventFilters": [
            {"attribute": "type", "value": "google.cloud.storage.object.v1.finalized"},
            {"attribute": "bucket", "value": load_state()["buckets"]["leaderboard"]},
        ],
    }
    emu.post(f"{path}?triggerId={EVENTARC_TRIGGER}", body)


def ensure_scheduler_job(emu: Emulator) -> None:
    log(f"Cloud Scheduler job {SCHEDULER_JOB} (*/2) -> submitter")
    uri = f"http://cm-submitter.{NS}.svc.cluster.local:8080/"
    path = f"/v1/projects/{PROJECT}/locations/{REGION}/jobs"
    emu.delete(f"{path}/{SCHEDULER_JOB}", expect=(200, 204, 404))
    body = {
        "name": f"projects/{PROJECT}/locations/{REGION}/jobs/{SCHEDULER_JOB}",
        "schedule": "*/2 * * * *",
        "timeZone": "UTC",
        "httpTarget": {
            "uri": uri,
            "httpMethod": "POST",
            "body": base64.b64encode(b'{"kind":"rollup"}').decode(),
            "oidcToken": {
                "serviceAccountEmail": f"demo-runner@{PROJECT}.iam.gserviceaccount.com",
                "audience": uri,
            },
        },
        "retryConfig": {"retryCount": 1, "minBackoffDuration": "15s"},
    }
    emu.post(path, body)


def stage_serve(args) -> None:
    state = load_state()
    emu, _ = state_emulator(args)
    if not state.get("spark"):
        state["spark"] = stage_spark_assets(emu, state)
    buckets = state["buckets"]
    publisher_image = build_image("crypto-medallion-publisher",
                                  IMAGES_DIR / "publisher" / "Dockerfile",
                                  IMAGES_DIR / "publisher",
                                  [IMAGES_DIR / "publisher" / "Dockerfile",
                                   IMAGES_DIR / "publisher" / "publisher.py"])
    leaderboard_image = build_image("crypto-medallion-leaderboard",
                                    IMAGES_DIR / "leaderboard" / "Dockerfile",
                                    IMAGES_DIR / "leaderboard",
                                    [IMAGES_DIR / "leaderboard" / "Dockerfile",
                                     IMAGES_DIR / "leaderboard" / "leaderboard.py"])
    submitter_image = build_image("crypto-medallion-submitter",
                                  IMAGES_DIR / "submitter" / "Dockerfile",
                                  IMAGES_DIR / "submitter",
                                  [IMAGES_DIR / "submitter" / "Dockerfile",
                                   IMAGES_DIR / "submitter" / "submitter.py"])
    state["images"] = {**state.get("images", {}),
                       "publisher": publisher_image,
                       "leaderboard": leaderboard_image,
                       "submitter": submitter_image}
    save_state(state)

    log("Cloud Run publisher")
    deploy_run_service(emu, RUN_PUBLISHER, publisher_image, 8080, {
        "PROJECT": PROJECT,
        "EMULATOR": IN_CLUSTER_EMULATOR,
        "BQ_DATASET": BQ_DATASET,
        "BQ_TABLE": BQ_TABLE,
        "FIRESTORE_COLLECTION": FIRESTORE_COLLECTION,
        "LEADERBOARD_BUCKET": buckets["leaderboard"],
        "MONITORING_METRIC": METRIC_TYPE,
    })
    log("Cloud Run leaderboard")
    deploy_run_service(emu, RUN_LEADERBOARD, leaderboard_image, 8080, {
        "PROJECT": PROJECT,
        "EMULATOR": IN_CLUSTER_EMULATOR,
        "FIRESTORE_COLLECTION": FIRESTORE_COLLECTION,
    })

    props_json = json.dumps(spark_properties(state), separators=(",", ":"))
    rollup_sql = rollup_queries(state)[0]
    render_k8s(emu, "submitter.yaml", state, extra={
        "SUBMITTER_IMAGE": submitter_image,
        "CLUSTER": DATAPROC_CLUSTER,
        "JARS": ",".join(state["spark"]["jar_uris"]),
        "PROPERTIES_B64": base64.b64encode(props_json.encode()).decode(),
        "ROLLUP_SQL_B64": base64.b64encode(rollup_sql.encode()).decode(),
        "BQ_DATASET": BQ_DATASET,
        "BQ_TABLE": BQ_TABLE,
        "ROLLUP_PREFIX": f"gs://{state['buckets']['rollup']}/rollup",
    })
    kubectl(["rollout", "status", "deployment/cm-submitter", "--timeout=180s"])
    ensure_eventarc_trigger(emu)
    ensure_scheduler_job(emu)
    log("serve complete")


# ── stage 5: observe + automate ──────────────────────────────────────────────
def ensure_monitoring(emu: Emulator) -> None:
    state = load_state()
    log("Monitoring notification channel + alert policy")
    topic = state["events_topic"]
    ch, _ = emu.post(f"/v3/projects/{PROJECT}/notificationChannels", {
        "type": "pubsub",
        "displayName": "crypto-medallion incidents",
        "enabled": True,
        "labels": {"topic": topic},
    })
    channel_name = ch.get("name", "")
    policies, _ = emu.get(f"/v3/projects/{PROJECT}/alertPolicies")
    existing = {p.get("displayName") for p in policies.get("alertPolicies", [])}
    if ALERT_POLICY in existing:
        info("alert policy exists")
        return
    emu.post(f"/v3/projects/{PROJECT}/alertPolicies", {
        "displayName": ALERT_POLICY,
        "combiner": "OR",
        "enabled": True,
        "notificationChannels": [channel_name],
        "conditions": [{
            "displayName": "trades per minute high",
            "conditionThreshold": {
                "filter": f'metric.type = "{METRIC_TYPE}" AND resource.type = "global"',
                "comparison": "COMPARISON_GT",
                "thresholdValue": 5,
                "duration": "0s",
            },
        }],
    })


def stage_observe(args) -> None:
    emu, _ = state_emulator(args)
    ensure_monitoring(emu)
    log("observe complete (metric is written by the publisher per delivery)")


def stage_automate(args) -> None:
    emu, _ = state_emulator(args)
    log("force Cloud Scheduler to fire once")
    emu.post("/_jaiscloud/scheduler-tick")


# ── status / reset ───────────────────────────────────────────────────────────
def stage_status(args) -> None:
    state = load_state()
    if not state:
        print("no state — run provision first")
        return
    emu, _ = state_emulator(args)
    print(json.dumps({
        "endpoint": state["endpoint"],
        "kafka": state["kafka"],
        "leaderboard_uri": run_authority(emu) if True else "",
    }, indent=2))


def stage_preflight(args) -> None:
    """Verify every link is live before a take (design §9)."""
    emu, state = state_emulator(args)
    checks: list[tuple[str, bool, str]] = []

    ok = True
    try:
        emu.get("/_jaiscloud/health")
    except SystemExit:
        ok = False
    checks.append(("emulator healthy", ok, emu.base))

    jid = state.get("streaming_job", "")
    st = job_state(emu, jid) if jid else ""
    checks.append(("streaming job RUNNING", st == "RUNNING", f"{jid}={st}"))

    try:
        bridge = kubectl(["get", "deploy", "cm-bridge", "-o",
                          "jsonpath={.status.readyReplicas}"], check=False)
    except SystemExit:
        bridge = "0"
    checks.append(("bridge ready", bridge == "1", f"ready={bridge}"))

    for svc in (RUN_PUBLISHER, RUN_LEADERBOARD):
        svc_obj, status = emu.get(f"/v2/projects/{PROJECT}/locations/{REGION}/services/{svc}",
                                  expect=(200, 404))
        ready = bool(svc_obj and svc_obj.get("uri"))
        checks.append((f"Cloud Run {svc} ready", ready, (svc_obj or {}).get("uri", "")))

    obj, status = emu.get(f"/storage/v1/b/{state['buckets']['leaderboard']}/o/latest.json",
                          expect=(200, 404))
    checks.append(("latest.json present", status == 200, (obj or {}).get("updated", "")))

    print(json.dumps({"leaderboard_uri": run_authority(emu),
                      "kafka_bootstrap": state["kafka"]["bootstrap"]}, indent=2))
    failed = 0
    for name, passed, detail in checks:
        print(f"  {'PASS' if passed else 'FAIL'}  {name}  ({detail})")
        failed += 0 if passed else 1
    if failed:
        raise SystemExit(f"{failed} pre-flight check(s) failed")


def stage_reset(args) -> None:
    state = load_state()
    endpoint = state.get("endpoint") or f"http://localhost:{args.port}"
    emu = Emulator(endpoint)
    log("delete demo resources")

    # Dataproc jobs: this run's streaming job and every Scheduler-submitted rollup.
    jobs, _ = emu.get(f"/v1/projects/{PROJECT}/regions/{REGION}/jobs", expect=(200, 404))
    for job in (jobs or {}).get("jobs", []):
        job_id = job.get("reference", {}).get("jobId", "")
        if job_id.startswith("cm-"):
            emu.delete(f"/v1/projects/{PROJECT}/regions/{REGION}/jobs/{job_id}",
                       expect=(200, 204, 404, 405))

    for path in (
        f"/v1/projects/{PROJECT}/locations/{REGION}/triggers/{EVENTARC_TRIGGER}",
        f"/v1/projects/{PROJECT}/locations/{REGION}/jobs/{SCHEDULER_JOB}",
        f"/v2/projects/{PROJECT}/locations/{REGION}/services/{RUN_PUBLISHER}",
        f"/v2/projects/{PROJECT}/locations/{REGION}/services/{RUN_LEADERBOARD}",
        f"/v1/projects/{PROJECT}/regions/{REGION}/clusters/{DATAPROC_CLUSTER}",
        f"/v1/projects/{PROJECT}/locations/{REGION}/clusters/{KAFKA_CLUSTER}",
        f"/bigquery/v2/projects/{PROJECT}/datasets/{BQ_DATASET}?deleteContents=true",
    ):
        emu.delete(path, expect=(200, 204, 404, 405))

    # Firestore read model.
    docs, _ = emu.get(f"/v1/projects/{PROJECT}/databases/(default)/documents/"
                      f"{FIRESTORE_COLLECTION}", expect=(200, 404))
    for doc in (docs or {}).get("documents", []):
        emu.delete(f"/{doc['name']}", expect=(200, 204, 404))

    kubectl(["delete", "deployment", "cm-bridge", "cm-submitter", "--ignore-not-found"])
    kubectl(["delete", "service", "cm-submitter", "--ignore-not-found"])
    kubectl(["delete", "configmap", "cm-submitter", "--ignore-not-found"], check=False)

    if args.pp:
        log("terraform destroy")
        env = {"GOOGLE_OAUTH_ACCESS_TOKEN": "demo-token", "GOOGLE_CLOUD_PROJECT": PROJECT}
        run(["terraform", f"-chdir={TF_DIR}", "destroy", "-input=false",
             "-auto-approve", "-no-color", f"-var=endpoint={endpoint}",
             f"-var=project={PROJECT}", f"-var=region={REGION}"], env=env, check=False)
    if args.full:
        log("POST /_jaiscloud/reset")
        emu.post("/_jaiscloud/reset")
        if STATE_FILE.exists():
            STATE_FILE.unlink()
    log("reset complete")


def stage_up(args) -> None:
    stage_provision(args)
    stage_bridge(args)
    stage_process(args)
    stage_serve(args)
    if not args.no_observe:
        stage_observe(args)


# ── stage 7: record the take (design §8) ─────────────────────────────────────
def lan_ip() -> str:
    return os.environ.get("DEMO_LAN_IP") or run(["hostname", "-I"]).split()[0]


def mux(video: Path, narration: Path, out: Path) -> None:
    log(f"mux {out}")
    run(["ffmpeg", "-y", "-loglevel", "error", "-i", str(video), "-i",
         str(narration), "-c:v", "copy", "-c:a", "aac", "-shortest", str(out)],
        capture=False)


def start_forward(local: int, remote: int) -> subprocess.Popen:
    log(f"forward svc/jaiscloud-gcp {local}:{remote} on 0.0.0.0")
    return subprocess.Popen(
        ["kubectl", "-n", NS, "port-forward", "--address", "0.0.0.0",
         "svc/jaiscloud-gcp", f"{local}:{remote}"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def enable_console() -> None:
    """Serve the console from the main emulator process.

    The console mounts the same in-process providers as the wire API, so it only
    shows the demo's resources when it runs in the process that owns them (the
    separate `jaiscloud-gcp-ui` Deployment is an ephemeral second emulator).
    Idempotently add `--ui --ui-port 4567`, expose the port, and default the
    console to the demo project (design §7).
    """
    log("enable the console on the main emulator (design §7)")
    args = json.loads(kubectl(["get", "deploy", "jaiscloud-gcp", "-o",
                               "jsonpath={.spec.template.spec.containers[0].args}"]))
    if "--ui" not in args:
        kubectl(["patch", "deploy", "jaiscloud-gcp", "--type=json", "-p",
                 '[{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--ui"},'
                 '{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"--ui-port"},'
                 '{"op":"add","path":"/spec/template/spec/containers/0/args/-","value":"4567"}]'])
    ports = json.loads(kubectl(["get", "deploy", "jaiscloud-gcp", "-o",
                                "jsonpath={.spec.template.spec.containers[0].ports}"]))
    if not any(p.get("name") == "ui" for p in ports):
        kubectl(["patch", "deploy", "jaiscloud-gcp", "--type=json", "-p",
                 '[{"op":"add","path":"/spec/template/spec/containers/0/ports/-",'
                 '"value":{"containerPort":4567,"name":"ui","protocol":"TCP"}}]'])
    svc_ports = json.loads(kubectl(["get", "svc", "jaiscloud-gcp", "-o",
                                    "jsonpath={.spec.ports}"]))
    if not any(p.get("name") == "ui" for p in svc_ports):
        kubectl(["patch", "svc", "jaiscloud-gcp", "--type=json", "-p",
                 '[{"op":"add","path":"/spec/ports/-",'
                 '"value":{"name":"ui","port":4567,"targetPort":4567,"protocol":"TCP"}}]'])
    kubectl(["set", "env", "deployment/jaiscloud-gcp",
             f"JAISCLOUD_GCP_PROJECT_ID={PROJECT}"])
    kubectl(["rollout", "status", "deployment/jaiscloud-gcp", "--timeout=240s"])


def _leaderboard_url(emu: Emulator, suffix: str, port: int) -> str:
    svc, _ = emu.get(f"/v2/projects/{PROJECT}/locations/{REGION}/services/{RUN_LEADERBOARD}")
    # The stored uri was minted under the previous suffix; rewrite its authority
    # to the LAN suffix the emulator now routes on.
    u = urllib.parse.urlsplit(svc.get("uri", ""))
    labels = (u.hostname or "").split(".")
    authority = f"{labels[0]}.{labels[1]}.{suffix}:{port}"
    url = f"{u.scheme}://{authority}/"
    log(f"leaderboard URL: {url}")
    req = urllib.request.Request(url + "api/leaderboard")
    with urllib.request.urlopen(req, timeout=15) as resp:
        if resp.status != 200:
            raise SystemExit(f"LAN authority {authority} did not route (HTTP {resp.status})")
    info(f"LAN authority routed ({authority})")
    return url


def setup_lan_authority(args, port: int, suffix: str) -> tuple[str, subprocess.Popen]:
    """Switch the Cloud Run authority to the LAN suffix; return (url, forward)."""
    state = load_state()
    log(f"switch the Cloud Run authority to {suffix}:{port}")
    run(["pkill", "-f", "port-forward svc/jaiscloud-gcp"], check=False)
    kubectl(["set", "env", "deployment/jaiscloud-gcp",
             f"JAISCLOUD_CLOUDRUN_URL_SUFFIX={suffix}",
             f"JAISCLOUD_CLOUDRUN_URL_PORT={port}"])
    kubectl(["rollout", "status", "deployment/jaiscloud-gcp", "--timeout=180s"])
    pf = start_forward(port, 8080)
    wait_healthy(f"http://localhost:{port}", timeout=40)
    emu = Emulator(f"http://localhost:{port}")
    # The runtime registers revisions under the authority current at ensure time,
    # so re-deploy the demo services after the switch.
    log("re-deploy Cloud Run services under the LAN authority")
    buckets = state["buckets"]
    deploy_run_service(emu, RUN_PUBLISHER, state["images"]["publisher"], 8080, {
        "PROJECT": PROJECT, "EMULATOR": IN_CLUSTER_EMULATOR,
        "BQ_DATASET": BQ_DATASET, "BQ_TABLE": BQ_TABLE,
        "FIRESTORE_COLLECTION": FIRESTORE_COLLECTION,
        "LEADERBOARD_BUCKET": buckets["leaderboard"],
        "MONITORING_METRIC": METRIC_TYPE})
    deploy_run_service(emu, RUN_LEADERBOARD, state["images"]["leaderboard"], 8080, {
        "PROJECT": PROJECT, "EMULATOR": IN_CLUSTER_EMULATOR,
        "FIRESTORE_COLLECTION": FIRESTORE_COLLECTION})
    return _leaderboard_url(emu, suffix, port), pf


def ffprobe_seconds(path: Path) -> float:
    data = json.loads(run(["ffprobe", "-v", "error", "-show_entries",
                           "format=duration", "-of", "json", str(path)]))
    return float(data["format"]["duration"])


def build_narration(narration: Path) -> None:
    run([sys.executable, str(DEMO_DIR / "scripts" / "make-narration.py"),
         "--out", str(narration)], capture=False)


def stage_record(args) -> None:
    """Serve the leaderboard on the LAN nip.io authority and capture a take."""
    ip = args.ip or lan_ip()
    port = args.lan_port
    stage_preflight(args)
    url, pf = setup_lan_authority(args, port, f"run.{ip}.nip.io")
    try:
        narration = Path(args.narration)
        build_narration(narration)
        seconds = ffprobe_seconds(narration)
        run([str(REPO_ROOT / "scripts" / "demo-record.sh"), "--url", url,
             "--duration", str(int(seconds + 8)), "--out", args.out], capture=False)
        mux(Path(args.out), narration, Path(args.out_muxed))
        log(f"take ready: {args.out_muxed}")
    finally:
        pf.terminate()


# ── stage 7b: split-screen take (console | leaderboard) ──────────────────────
def _find_chrome() -> str:
    for c in ("google-chrome", "google-chrome-stable", "chromium", "chromium-browser"):
        if shutil.which(c):
            return shutil.which(c)
    raise SystemExit("no google-chrome/chromium on PATH")


def _cdp_pages() -> list:
    try:
        with urllib.request.urlopen("http://127.0.0.1:9222/json/list", timeout=5) as r:
            return [t for t in json.load(r) if t.get("type") == "page"]
    except Exception:
        return []


def _cdp_ws_url(app_prefix: str = "") -> str:
    pages = _cdp_pages()
    if app_prefix:
        for t in pages:
            if app_prefix in (t.get("url") or ""):
                return t["webSocketDebuggerUrl"]
    return pages[0]["webSocketDebuggerUrl"] if pages else ""


async def _cdp_eval(ws_url: str, expression: str) -> None:
    import websockets
    async with websockets.connect(ws_url, max_size=None) as ws:
        await ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate",
                                  "params": {"expression": expression,
                                             "returnByValue": True}}))
        await ws.recv()


def cdp_eval(app_prefix: str, expression: str) -> bool:
    """Run JS in the console app target. React Router is driven client-side so a
    route change is instant (a full Page.navigate reload is too slow to paint
    inside a beat's hold)."""
    ws = _cdp_ws_url(app_prefix)
    if not ws:
        return False
    import asyncio
    asyncio.run(_cdp_eval(ws, expression))
    return True


async def _cdp_eval_value(ws_url: str, expression: str):
    import websockets
    async with websockets.connect(ws_url, max_size=None) as ws:
        await ws.send(json.dumps({"id": 1, "method": "Runtime.evaluate",
                                  "params": {"expression": expression,
                                             "returnByValue": True}}))
        while True:
            msg = json.loads(await ws.recv())
            if msg.get("id") == 1:
                return (msg.get("result", {}).get("result", {}) or {}).get("value")


def cdp_eval_value(app_prefix: str, expression: str):
    ws = _cdp_ws_url(app_prefix)
    if not ws:
        return None
    import asyncio
    return asyncio.run(_cdp_eval_value(ws, expression))


# ── OS pointer choreography (xdotool) ────────────────────────────────────────
def _xdo(display: str, args: list) -> None:
    subprocess.run(["xdotool", *args], env={**os.environ, "DISPLAY": display},
                   check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def move_cursor(display: str, x: int, y: int, steps: int = 10) -> None:
    """Glide the pointer to (x, y) so x11grab shows real motion."""
    import time as _time
    try:
        out = subprocess.run(["xdotool", "getmouselocation", "--shell"],
                             env={**os.environ, "DISPLAY": display},
                             capture_output=True, text=True, check=True).stdout
        cur = dict(line.split("=", 1) for line in out.splitlines() if "=" in line)
        cx, cy = int(cur.get("X", 0)), int(cur.get("Y", 0))
    except Exception:
        cx, cy = 0, 0
    for i in range(1, steps + 1):
        _xdo(display, ["mousemove", str(int(cx + (x - cx) * i / steps)),
                       str(int(cy + (y - cy) * i / steps))])
        _time.sleep(0.012)


def click_cursor(display: str) -> None:
    _xdo(display, ["click", "1"])


def route_hrefs(route: str) -> list:
    """Sidebar link candidates for a route: itself then each ancestor section."""
    parts = [p for p in route.split("/") if p]
    return ["/ui/" + "/".join(parts[:i]) for i in range(len(parts), 0, -1)]


# Routes that are sidebar destinations (collection pages). Anything else is a
# detail page, which loads slower and gets a longer head start in the recording.
COLLECTION_ROUTES = {
    "/gcp", "/gcp/storage/buckets", "/gcp/kms/keyrings",
    "/gcp/secretmanager/secrets", "/gcp/pubsub/topics", "/gcp/pubsub/subscriptions",
    "/gcp/iam/service-accounts", "/gcp/managedkafka/clusters", "/gcp/managedkafka/topics",
    "/gcp/dataproc/clusters", "/gcp/dataproc/jobs", "/gcp/run/services",
    "/gcp/eventarc/triggers", "/gcp/firestore/collections", "/gcp/bigquery/datasets",
    "/gcp/bigquery/jobs", "/gcp/monitoring/metrics", "/gcp/monitoring/alerting",
    "/gcp/scheduler/jobs", "/gcp/logging/entries",
}


def nav_link_center(app_prefix: str, route: str):
    """Center of the sidebar link for a route (or its nearest section), scrolled
    into view, plus whether the link is the exact route (a detail route matches
    only its ancestor section). Restricted to the left sidebar."""
    bases = route_hrefs(route)
    exact = "/ui" + route
    js = ("(() => { const bs=%s, exact=%s; for (const b of bs) {"
          " const a=Array.from(document.querySelectorAll('a')).find(x => {"
          "   const h=x.getAttribute('href')||''; return h===b || h.startsWith(b+'/'); });"
          " if (a) { a.scrollIntoView({block:'center'}); const r=a.getBoundingClientRect();"
          "   if (r.width>0 && r.height>0 && r.left<260 && r.top>60)"
          "     return {x: Math.round(r.left+r.width/2), y: Math.round(r.top+r.height/2),"
          "             exact: a.getAttribute('href')===exact};"
          " } } return null; })()" % (json.dumps(bases), json.dumps(exact)))
    return cdp_eval_value(app_prefix, js)


def stage_record_split(args) -> None:
    """Record a take with the console left and the leaderboard right, driving the
    console to each stage's resources as the narration plays (design §7/§8)."""
    import tempfile
    import time as _time

    ip = args.ip or lan_ip()
    port = args.lan_port
    console_port = args.console_port
    width, height = 960, 1080
    total_w, total_h = width * 2, height

    stage_preflight(args)
    enable_console()
    url, pf = setup_lan_authority(args, port, f"run.{ip}.nip.io")
    console_pf = start_forward(console_port, 4567)
    chrome = None
    xvfb = None
    ff = None
    profile_console = profile_leader = ""
    try:
        console_base = f"http://{ip}:{console_port}"
        meta = None
        for _ in range(40):
            try:
                meta, _ = Emulator(console_base).get("/api/ui/v1/meta")
                break
            except Exception:
                _time.sleep(1)
        if not meta:
            raise SystemExit(f"console not reachable at {console_base}")
        if meta.get("accountId") != PROJECT:
            raise SystemExit(f"console is on {meta.get('accountId')}, expected {PROJECT}")
        info(f"console at {console_base}/ui on {PROJECT}")

        narration = Path(args.narration)
        build_narration(narration)
        spec = json.loads((DEMO_DIR / "narration" / "beats.json").read_text())
        clips = DEMO_DIR / "narration" / "clips"
        durations = {b["id"]: ffprobe_seconds(clips / f"{b['id']}.mp3") for b in spec["beats"]}

        log(f"start Xvfb :{args.display_num} ({total_w}x{total_h})")
        xvfb = subprocess.Popen(
            ["Xvfb", f":{args.display_num}", "-screen", "0",
             f"{total_w}x{total_h}x24", "-nolisten", "tcp"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        for _ in range(60):
            if Path(f"/tmp/.X11-unix/X{args.display_num}").exists():
                break
            _time.sleep(0.2)
        env_display = {**os.environ, "DISPLAY": f":{args.display_num}"}
        env_display.pop("WAYLAND_DISPLAY", None)

        chrome = _find_chrome()
        profile_console = tempfile.mkdtemp(prefix="/tmp/opencode/cm-console-")
        profile_leader = tempfile.mkdtemp(prefix="/tmp/opencode/cm-leader-")
        common = ["--ozone-platform=x11", "--no-first-run", "--no-default-browser-check",
                  "--disable-dev-shm-usage", "--disable-features=Translate"]

        out_dir = Path(args.out).parent
        out_dir.mkdir(parents=True, exist_ok=True)

        def launch(profile, pos, app, logname, debug=False):
            cmd = [chrome, f"--user-data-dir={profile}", *common,
                   f"--window-position={pos}", f"--window-size={width},{height}",
                   f"--app={app}"]
            if debug:
                cmd += ["--remote-debugging-port=9222", "--remote-allow-origins=*"]
            logf = open(out_dir / logname, "w")
            return subprocess.Popen(cmd, env=env_display, stdout=logf,
                                    stderr=subprocess.STDOUT)

        log("open the console (left)")
        launch(profile_console, "0,0", f"{console_base}/ui/gcp",
               "console-chrome.log", debug=True)
        for _ in range(60):
            if _cdp_ws_url(console_base + "/ui"):
                break
            _time.sleep(0.5)
        else:
            clog_path = out_dir / "console-chrome.log"
            if clog_path.exists():
                print(clog_path.read_text()[-800:])
            raise SystemExit("console Chrome DevTools endpoint not reachable on :9222")
        log("open the leaderboard (right)")
        launch(profile_leader, f"{width},0", url, "leaderboard-chrome.log")
        _time.sleep(args.warmup)

        # Console-switch schedule: each stop fires when the voice names it (mark
        # time within the beat), falling back to even spacing and, for a beat
        # with no marks, to the beat start.
        state = load_state()
        app_prefix = console_base + "/ui"
        display = f":{args.display_num}"
        cursor = bool(shutil.which("xdotool"))
        tp_path = DEMO_DIR / "narration" / "timepoints.json"
        marks = json.loads(tp_path.read_text()).get("beats", {}) if tp_path.exists() else {}

        def resolve(route: str) -> str:
            if "{streaming_job}" in route:
                jid = state.get("streaming_job", "")
                return route.replace("{streaming_job}", jid) if jid else "/gcp/dataproc/jobs"
            return route

        starts, run_total = {}, 0.0
        for b in spec["beats"]:
            starts[b["id"]] = run_total
            run_total += durations[b["id"]]
        events = []
        for b in spec["beats"]:
            stops = b.get("console") or [{"route": "/gcp"}]
            bmarks = marks.get(b["id"], {})
            n = len(stops)
            for k, stop in enumerate(stops):
                if stop.get("mark") and stop["mark"] in bmarks:
                    t = starts[b["id"]] + bmarks[stop["mark"]]
                else:
                    t = starts[b["id"]] + durations[b["id"]] * k / n
                route = resolve(stop["route"])
                # Detail pages load slower, so switch them a touch earlier so they
                # are painted by the time the word is spoken.
                preload = 0.7 if route in COLLECTION_ROUTES else 1.3
                events.append((max(0.0, t - preload), route))
        events.sort(key=lambda e: e[0])

        out = Path(args.out)
        progress = out.with_suffix(".progress")
        if progress.exists():
            progress.unlink()
        log(f"capture {run_total + 1:.0f}s ({total_w}x{total_h}@{args.framerate}) -> {out}")
        ff = subprocess.Popen(
            ["ffmpeg", "-y", "-loglevel", "error", "-f", "x11grab", "-draw_mouse", "1",
             "-video_size", f"{total_w}x{total_h}", "-framerate", str(args.framerate),
             "-i", f":{args.display_num}", "-t", str(int(run_total + 1)),
             "-c:v", "libx264", "-preset", "veryfast", "-crf", "18",
             "-pix_fmt", "yuv420p", "-progress", str(progress), "-nostats", str(out)],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Anchor t=0 to the first captured frame so audio time lines up with video
        # time (x11grab takes a moment to start).
        w0 = None
        deadline = _time.monotonic() + 25
        while _time.monotonic() < deadline and w0 is None:
            if progress.exists():
                for line in progress.read_text().splitlines():
                    if line.startswith("out_time_us="):
                        raw = line.split("=", 1)[1].strip()
                        if raw.isdigit() and int(raw) > 0:
                            w0 = _time.monotonic() - int(raw) / 1_000_000
                            break
            _time.sleep(0.03)
        w0 = w0 if w0 is not None else _time.monotonic()
        info("capture anchored to the first frame")

        for t, route in events:
            wait = w0 + t - _time.monotonic()
            if wait > 0:
                _time.sleep(wait)
            # Switch first (synced to the voice), then move the cursor — the
            # click is cosmetic and lands on the now-active nav item.
            js = (f"history.pushState({{}}, '', '/ui{route}');"
                  "window.dispatchEvent(new PopStateEvent('popstate'));")
            if not cdp_eval(app_prefix, js):
                raise SystemExit("CDP eval failed")
            info(f"console -> {route} @ {t:.1f}s")
            if cursor and route != "/gcp":
                pos = nav_link_center(app_prefix, route)
                if pos and 0 <= pos.get("x", width) < width:
                    move_cursor(display, pos["x"], pos["y"], steps=6)
                    # Click only when the link is the exact route; for a detail
                    # route the matched link is an ancestor section, and clicking
                    # it would navigate away from the detail.
                    if pos.get("exact"):
                        click_cursor(display)
        ff.wait()
        if ff.returncode != 0:
            raise SystemExit("ffmpeg capture failed")
        mux(out, narration, Path(args.out_muxed))
        log(f"take ready: {args.out_muxed}")
    finally:
        pf.terminate()
        console_pf.terminate()
        if ff and ff.poll() is None:
            ff.terminate()
        for profile in (profile_console, profile_leader):
            if profile:
                subprocess.run(["pkill", "-f", profile], check=False)
        if xvfb:
            xvfb.terminate()


# ── CLI ──────────────────────────────────────────────────────────────────────
def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("stage", choices=[
        "provision", "bridge", "process", "rollup", "serve", "observe",
        "automate", "up", "status", "preflight", "record", "record-split", "reset"])
    ap.add_argument("--port", type=int, default=18080,
                    help="host port for the emulator REST port-forward")
    ap.add_argument("--no-observe", action="store_true", help="up: skip observe")
    ap.add_argument("--pp", action="store_true", help="reset: also terraform destroy")
    ap.add_argument("--full", action="store_true", help="reset: POST /_jaiscloud/reset")
    ap.add_argument("--ip", default="", help="record: emulator-host LAN IP")
    ap.add_argument("--lan-port", type=int, default=18080,
                    help="record: host port forwarded on 0.0.0.0 for the LAN authority")
    ap.add_argument("--console-port", type=int, default=4567,
                    help="record-split: host port forwarded on 0.0.0.0 for the console")
    ap.add_argument("--display-num", type=int, default=99,
                    help="record-split: Xvfb display number")
    ap.add_argument("--framerate", type=int, default=30)
    ap.add_argument("--warmup", type=float, default=6.0)
    ap.add_argument("--narration", default=str(DEMO_DIR / "narration" / "narration.mp3"))
    ap.add_argument("--out", default=str(DEMO_DIR / "out" / "take-browser.mp4"),
                    help="record: raw capture path")
    ap.add_argument("--out-muxed", default=str(DEMO_DIR / "out" / "take.mp4"),
                    help="record: muxed take path")
    args = ap.parse_args()
    if args.stage == "record-split" and args.out == str(DEMO_DIR / "out" / "take-browser.mp4"):
        args.out = str(DEMO_DIR / "out" / "take-split-browser.mp4")
    if args.stage == "record-split" and args.out_muxed == str(DEMO_DIR / "out" / "take.mp4"):
        args.out_muxed = str(DEMO_DIR / "out" / "take-split.mp4")
    {
        "provision": stage_provision, "bridge": stage_bridge,
        "process": stage_process, "rollup": stage_rollup, "serve": stage_serve,
        "observe": stage_observe, "automate": stage_automate, "up": stage_up,
        "status": stage_status, "preflight": stage_preflight,
        "record": stage_record, "record-split": stage_record_split,
        "reset": stage_reset,
    }[args.stage](args)


if __name__ == "__main__":
    main()
