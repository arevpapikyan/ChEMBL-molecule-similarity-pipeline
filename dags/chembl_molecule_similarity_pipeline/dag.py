import json
import logging
import os
import subprocess
import time
from datetime import datetime, timedelta

import boto3
import psycopg2
import requests
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.sdk import DAG, task
from docker.types import Mount

logger = logging.getLogger(__name__)

DOCKER_IMAGE = "pipeline_worker"

# The allowlist of variables forwarded INTO the spawned worker containers -- not
# the pipeline's whole config.
WORKER_ENV_VARS = [
    "S3_BUCKET", "S3_PREFIX", "S3_REGION",
    "DWH_URL", "N_SOURCE_MOLECULES", "RANDOM_SEED", "TOP_K",
    "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
]

# Variables the DAG itself needs present, whatever the deployment.
REQUIRED_ENV_VARS = ["S3_BUCKET", "S3_PREFIX", "DWH_URL"]


def worker_env() -> dict:
    return {var: os.environ.get(var, "") for var in WORKER_ENV_VARS}


def _require_env() -> None:
    """Fail at parse time if config the DAG depends on is absent."""
    missing = [v for v in REQUIRED_ENV_VARS if not os.environ.get(v)]
    if missing:
        raise RuntimeError(
            "Missing required environment variable(s): "
            f"{', '.join(missing)}. In this stack they come from compose "
            "env_file (.env); under another deployment supply them another way."
        )
    # TEAMS_WEBHOOK_URL is required unless alerting was explicitly opted out.
    alerts_optional = os.environ.get("TEAMS_ALERTS_OPTIONAL", "").lower() in ("1", "true", "yes")
    if not alerts_optional and not os.environ.get("TEAMS_WEBHOOK_URL"):
        raise RuntimeError(
            "TEAMS_WEBHOOK_URL is not set, so pipeline failures cannot be "
            "reported. Set it, or set TEAMS_ALERTS_OPTIONAL=true to run "
            "without alerting."
        )


def docker_run_base(command_args: list[str]) -> list[str]:
    return [
        "docker", "run", "--rm",
        "--add-host=host.docker.internal:host-gateway",
        *[f"--env={k}={v}" for k, v in worker_env().items()],
        DOCKER_IMAGE, *command_args,
    ]


# redact_cmd() masks these before any error message can reach logs/Teams
SENSITIVE_ENV_VARS = {
    "DWH_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN",
}


def redact_cmd(cmd: list[str]) -> list[str]:
    """
    Replace "--env=KEY=value" with "--env=KEY=***REDACTED***" for every
    KEY in SENSITIVE_ENV_VARS. Only ever used for error messages/logs.
    """
    redacted = []
    for arg in cmd:
        if arg.startswith("--env="):
            key = arg[len("--env="):].split("=", 1)[0]
            if key in SENSITIVE_ENV_VARS:
                redacted.append(f"--env={key}=***REDACTED***")
                continue
        redacted.append(arg)
    return redacted


def run_worker(command_args: list[str]) -> str:
    cmd = docker_run_base(command_args)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, check=True)
    except subprocess.CalledProcessError as exc:
        stderr_tail = (exc.stderr or "").strip()[-2000:]

        raise RuntimeError(
            f"Command {redact_cmd(cmd)} returned non-zero exit status {exc.returncode}.\n"
            f"stderr (tail): {stderr_tail}"
        ) from None
    return result.stdout.strip()


class TeamsNotificationError(RuntimeError):
    """Raised when a Teams notification could not be delivered."""


def _adaptive_card(title: str, facts: list[tuple[str, str]], subtitle: str,
                   color: str = "Attention") -> dict:
    """Builds the Adaptive Card envelope a Teams Workflow webhook expects."""
    return {
        "type": "message",
        "attachments": [
            {
                "contentType": "application/vnd.microsoft.card.adaptive",
                "content": {
                    "type": "AdaptiveCard",
                    "$schema": "http://adaptivecards.io/schemas/adaptive-card.json",
                    "version": "1.4",
                    "body": [
                        {
                            "type": "TextBlock",
                            "text": title,
                            "weight": "Bolder",
                            "size": "Medium",
                            "color": color,
                            "wrap": True,
                        },
                        {
                            "type": "FactSet",
                            "facts": [{"title": t, "value": v} for t, v in facts],
                        },
                        {
                            "type": "TextBlock",
                            "text": subtitle,
                            "wrap": True,
                            "isSubtle": True,
                        },
                    ],
                },
            }
        ],
    }


def _post_to_teams(payload: dict) -> None:
    """Delivers an Adaptive Card, raising TeamsNotificationError on any failure."""

    webhook = os.environ.get("TEAMS_WEBHOOK_URL")
    if not webhook:
        raise TeamsNotificationError(
            "TEAMS_WEBHOOK_URL is not set, so pipeline failures cannot be reported. "
            "Set it in .env, or set TEAMS_ALERTS_OPTIONAL=true to run without alerting."
        )
    try:
        response = requests.post(webhook, json=payload, timeout=10)
    except requests.RequestException as exc:
        raise TeamsNotificationError(f"POST to Teams webhook failed: {exc}") from exc

    if not response.ok:
        raise TeamsNotificationError(
            f"Teams webhook returned HTTP {response.status_code}: {response.text[:200]}"
        )
    logger.info("Teams notification delivered: HTTP %s", response.status_code)


def notify_failure(context) -> None:
    """Posts a failure card to Teams."""
    ti = context["task_instance"]
    exc = context.get("exception")
    reason = type(exc).__name__ if exc is not None else "Unknown (no exception object)"
    map_index_suffix = f" (map_index={ti.map_index})" if ti.map_index != -1 else ""

    _post_to_teams(_adaptive_card(
        "ChEMBL pipeline task failed",
        [
            ("DAG", ti.dag_id),
            ("Task", f"{ti.task_id}{map_index_suffix}"),
            ("Attempt", str(ti.try_number)),
            ("Error", reason),
        ],
        f"Run: {context['run_id']}",
    ))


default_args = {
    "owner": os.environ.get("DAG_OWNER", "unknown"),
    "retries": 2,
    "retry_delay": timedelta(minutes=3),
    "on_failure_callback": notify_failure,
}


_require_env()

with DAG(
    dag_id="chembl_similarity_pipeline",
    schedule=None,
    start_date=datetime(2026, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["chembl", "similarity"],
    max_active_runs=1,
):

    @task(
        retries=1,
        retry_delay=timedelta(seconds=30),
        # Don't Teams-notify about Teams being unreachable: the callback would
        # just fail again and bury the real message under a second traceback.
        on_failure_callback=None,
    )
    def check_teams_webhook(**context) -> None:
        """
        Fails the run immediately if failures could not be reported.
        Set TEAMS_ALERTS_OPTIONAL=true to run without a webhook deliberately.
        """
        if os.environ.get("TEAMS_ALERTS_OPTIONAL", "").lower() in ("1", "true", "yes"):
            logger.info("TEAMS_ALERTS_OPTIONAL set; skipping webhook preflight.")
            return

        _post_to_teams(_adaptive_card(
            "ChEMBL pipeline starting",
            [
                ("DAG", "chembl_similarity_pipeline"),
                ("Run", str(context.get("run_id", "unknown"))),
                ("Owner", os.environ.get("DAG_OWNER", "unknown")),
            ],
            "Preflight: alerting channel reachable. Failures will be reported here.",
            color="Good",
        ))

    ingest_bronze = DockerOperator(
        task_id="ingest_bronze",
        image=DOCKER_IMAGE,
        command="ingest",
        environment=worker_env(),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        extra_hosts={"host.docker.internal": "host-gateway"},
        mounts=[Mount(source="chembl_downloader_cache", target="/root/.data", type="volume")],
        mount_tmp_dir=False,
        auto_remove="success",
        execution_timeout=timedelta(hours=4),
    )

    @task
    def quality_check_bronze() -> None:
        """Structural check: bronze row counts sane, four tables present."""
        with psycopg2.connect(os.environ["DWH_URL"]) as conn, conn.cursor() as cur:
            for table in [
                "chembl_id_lookup", "molecule_dictionary",
                "compound_properties", "compound_structures",
            ]:
                cur.execute(f"SELECT COUNT(*) FROM raw.{table}")
                count = cur.fetchone()[0]
                if count == 0:
                    raise ValueError(f"raw.{table} is empty after ingestion")
                logger.info("raw.%s: %s rows", table, count)

    compute_fingerprints = DockerOperator(
        task_id="compute_fingerprints",
        image=DOCKER_IMAGE,
        command="fingerprints",
        environment=worker_env(),
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
        extra_hosts={"host.docker.internal": "host-gateway"},
        mount_tmp_dir=False,
        auto_remove="success",
    )

    @task
    def quality_check_fingerprints() -> None:
        os.environ.pop("AWS_PROFILE", None)
        session = boto3.Session(
            aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID") or None,
            aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY") or None,
            aws_session_token=os.environ.get("AWS_SESSION_TOKEN") or None,
            region_name=os.environ.get("S3_REGION", "us-east-1"),
        )
        client = session.client("s3")
        key = f"{os.environ['S3_PREFIX']}/silver/fingerprints/fingerprints.parquet"
        head = client.head_object(Bucket=os.environ["S3_BUCKET"], Key=key)
        if head["ContentLength"] == 0:
            raise ValueError(f"{key} is empty")

    def _dwh_connect(attempts: int = 3, backoff_seconds: int = 5):
        """Connect to the DWH with a bounded timeout and retries."""
        last_exc: Exception | None = None
        for attempt in range(1, attempts + 1):
            try:
                return psycopg2.connect(
                    os.environ["DWH_URL"],
                    connect_timeout=30,
                    # Server-side ceiling: a query that somehow runs long is
                    # cancelled by Postgres rather than leaving the task
                    # blocked on a read that never returns.
                    options="-c statement_timeout=600000",
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=3,
                )
            except psycopg2.OperationalError as exc:
                last_exc = exc
                if attempt < attempts:
                    logger.warning(
                        "DWH connection attempt %s/%s failed (%s); retrying in %ss "
                        "-- if this persists, check the ssm_tunnel container",
                        attempt, attempts, exc, backoff_seconds,
                    )
                    time.sleep(backoff_seconds)
        raise RuntimeError(
            f"Could not connect to the DWH after {attempts} attempts. The SSM "
            f"tunnel is likely wedged or its session expired; restart the "
            f"ssm_tunnel service."
        ) from last_exc

    @task
    def choose_source_molecules() -> list[str]:
        n = int(os.environ.get("N_SOURCE_MOLECULES", "100"))
        seed = os.environ.get("RANDOM_SEED", "42")

        with _dwh_connect() as conn, conn.cursor() as cur:
            cur.execute("SET max_parallel_workers_per_gather = 0")
            cur.execute(
                """
                -- No DISTINCT: chembl_id is the PRIMARY KEY of all three tables,
                -- so the join is strictly 1:1:1 and cannot duplicate.

                SELECT md.chembl_id
                FROM raw.molecule_dictionary md
                JOIN raw.compound_structures cs ON cs.chembl_id = md.chembl_id
                JOIN raw.compound_properties cp ON cp.chembl_id = md.chembl_id
                WHERE cs.canonical_smiles IS NOT NULL
                ORDER BY md5(md.chembl_id || %s)
                LIMIT %s
                """,
                (seed, n),
            )
            chosen = sorted(r[0] for r in cur.fetchall())

        if len(chosen) < n:
            raise ValueError(
                f"Asked for {n} source molecules but Bronze only yielded {len(chosen)}; "
                "check that ingest_bronze populated raw.* fully."
            )
        logger.info("Chose %s source molecules (seed=%s)", len(chosen), seed)
        return chosen

    @task
    def compute_similarity(source_chembl_ids: list[str]) -> list[str]:
        run_worker(["similarity-all", "--source-ids", json.dumps(source_chembl_ids)])
        return source_chembl_ids

    @task
    def select_top_k(source_chembl_ids: list[str]) -> list[str]:
        k = os.environ.get("TOP_K", "10")
        run_worker(["topk-all", "--source-ids", json.dumps(source_chembl_ids), "--k", k])
        return source_chembl_ids

    @task
    def load_mart(_topk_done: list[str]) -> dict:
        return json.loads(run_worker(["mart-load"]))

    @task
    def regenerate_pivot_view() -> str:
        return run_worker(["regenerate-pivot"])

    @task
    def quality_check_mart(mart_stats: dict) -> None:
        if mart_stats.get("fact_similarity_rows", 0) == 0:
            raise ValueError("mart.fact_similarity received zero rows")
        logger.info("Mart load OK: %s", mart_stats)

    teams_ready = check_teams_webhook()
    check_bronze = quality_check_bronze()
    check_fp = quality_check_fingerprints()
    sources = choose_source_molecules()
    similarity_results = compute_similarity(sources)
    topk_results = select_top_k(similarity_results)
    mart_stats = load_mart(topk_results)
    pivot = regenerate_pivot_view()

    teams_ready >> ingest_bronze >> check_bronze >> compute_fingerprints >> check_fp >> sources
    sources >> similarity_results >> topk_results >> mart_stats >> pivot >> quality_check_mart(mart_stats)
