import os
import secrets
import subprocess
import sys
from datetime import UTC, datetime


def utc_today_str() -> str:
    """Return today's date in UTC as YYYY-MM-DD."""
    return datetime.now(UTC).strftime("%Y-%m-%d")


def compute_run_id(run_date: str) -> str:
    """Build a run_id from run_date + short hex suffix."""
    suffix = secrets.token_hex(4)  # 8 hex chars
    return f"{run_date}_{suffix}"


def run_service(service: str, extra_env: dict[str, str]) -> int:
    """
    Invoke a service via `docker compose run --rm <service>`, passing extra_env.

    Returns the subprocess return code. Any OS-level failure (e.g. docker/compose
    not installed) is caught and reported as exit code 1.
    """
    env = os.environ.copy()
    env.update(extra_env)
    cmd = ["docker", "compose", "run", "--rm", service]
    print(f"orchestrator: running {' '.join(cmd)} with RUN_ID={extra_env.get('RUN_ID')} RUN_DATE={extra_env.get('RUN_DATE')}", flush=True)
    try:
        result = subprocess.run(cmd, env=env)
        return result.returncode
    except Exception as e:
        print(
            f"orchestrator: failed to execute {service}: {type(e).__name__}: {e}",
            flush=True,
        )
        return 1


def main() -> int:
    # Respect externally provided RUN_DATE; generate only if missing.
    run_date = (os.environ.get("RUN_DATE") or utc_today_str()).strip()
    run_id = (os.environ.get("RUN_ID") or "").strip()

    if not run_id:
        run_id = compute_run_id(run_date)

    print(f"orchestrator: run_id={run_id} run_date={run_date}", flush=True)

    # Optional record selection for ingestion; pass through if present.
    ingestion_env = {
        "RUN_ID": run_id,
        "RUN_DATE": run_date,
    }
    for var in ("RECORD_IDS", "RECORD_RANGE", "RECORD_LIMIT"):
        if var in os.environ:
            ingestion_env[var] = os.environ.get(var, "")

    # 1) Ingestion
    code = run_service("ingestion", ingestion_env)
    if code != 0:
        print(f"orchestrator: ingestion failed with exit code {code}", flush=True)
        return code

    # 2) Processing
    processing_env = {
        "RUN_ID": run_id,
        "RUN_DATE": run_date,
    }
    code = run_service("processing", processing_env)
    if code != 0:
        print(f"orchestrator: processing failed with exit code {code}", flush=True)
        return code

    # 3) Aggregation
    aggregation_env = {
        "RUN_ID": run_id,
        "RUN_DATE": run_date,
    }
    # Allow orchestrator caller to control AGG_OVERWRITE if desired.
    if "AGG_OVERWRITE" in os.environ:
        aggregation_env["AGG_OVERWRITE"] = os.environ["AGG_OVERWRITE"]

    code = run_service("aggregation", aggregation_env)
    if code != 0:
        print(f"orchestrator: aggregation failed with exit code {code}", flush=True)
    else:
        print("orchestrator: pipeline completed successfully", flush=True)
    return code


if __name__ == "__main__":
    sys.exit(main())

