#!/usr/bin/env python3
"""Build workers and test archival against isolated local services.

Requires Python 3.13, Java 21 (java and javac), Maven, Temporal CLI 1.5.1,
and Docker. CLI installation: https://docs.temporal.io/cli/setup-cli
No Cloud credentials are needed.

From the repository root:
    python3.13 -m venv .venv-e2e
    .venv-e2e/bin/python -m pip install -r audit/tests/e2e/requirements.txt
    .venv-e2e/bin/python scripts/test_archival_e2e.py

The runner builds both Java workers and creates/removes an isolated Postgres 17
container and volume on a random local port. Each test starts a Temporal dev
server and creates/removes its own database schema. Existing demo services are
untouched. Logs: out/verification/archival-*.log.

To use existing Postgres instead of Docker, set E2E_PG_DSN to a disposable test
database. Its user needs schema and role creation privileges; the connection
recovery test temporarily disables a test role. CI uses this mode. Missing
prerequisites fail the suite rather than skip it.
"""

import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from uuid import uuid4

import psycopg

ROOT = Path(__file__).resolve().parents[1]
OUTPUT = ROOT / "out/verification"


def checked(*args):
    return subprocess.check_output(args, text=True, cwd=ROOT).strip()


def logged(command, filename, env):
    path = OUTPUT / filename
    print(f"Running {' '.join(command)}; log: {path}", flush=True)
    with path.open("w") as log:
        try:
            result = subprocess.run(command, cwd=ROOT, env=env, stdout=log,
                                    stderr=subprocess.STDOUT, timeout=600)
        except subprocess.TimeoutExpired:
            print(path.read_text(), flush=True)
            raise
    print(path.read_text(), flush=True)
    return result.returncode


def main():
    for tool in ["temporal", "java", "javac", "mvn"]:
        if not shutil.which(tool):
            raise SystemExit(f"{tool} required; see the docstring in scripts/test_archival_e2e.py")
    OUTPUT.mkdir(parents=True, exist_ok=True)
    env = os.environ.copy()
    container = None
    try:
        if not env.get("E2E_PG_DSN"):
            if not shutil.which("docker"):
                raise SystemExit("Docker or E2E_PG_DSN is required")
            container = "temporal-archive-e2e-" + uuid4().hex[:12]
            checked("docker", "run", "--detach", "--name", container,
                    "--env", "POSTGRES_USER=archive_test", "--env", "POSTGRES_PASSWORD=archive_test",
                    "--env", "POSTGRES_DB=archive_test", "--publish", "127.0.0.1::5432", "postgres:17")
            port = checked("docker", "port", container, "5432/tcp").rsplit(":", 1)[1]
            env["E2E_PG_DSN"] = f"postgresql://archive_test:archive_test@127.0.0.1:{port}/archive_test"
        deadline = time.monotonic() + 30
        while True:
            try:
                with psycopg.connect(env["E2E_PG_DSN"], connect_timeout=2):
                    break
            except psycopg.OperationalError:
                if time.monotonic() >= deadline:
                    raise SystemExit("Test Postgres did not become ready within 30 seconds")
                time.sleep(0.2)
        code = logged(["mvn", "-B", "-ntp", "-f", "workers/pom.xml", "-DskipTests", "package"], "archival-build.log", env)
        if code:
            return code
        return logged([sys.executable, "-m", "unittest", "discover", "-s", "audit/tests/e2e", "-v"], "archival-e2e.log", env)
    finally:
        if container:
            subprocess.run(["docker", "rm", "--force", "--volumes", container], check=True)


if __name__ == "__main__":
    sys.exit(main())
