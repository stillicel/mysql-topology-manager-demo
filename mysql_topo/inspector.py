"""
Cluster inspection engine.

Bridges the main repo's metadata store and connection logic with the
checker plugin system from mysql-cluster-inspector.
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime

import pymysql

from mysql_topo import db
from mysql_topo import mock as mk
from mysql_topo.checkers import get_all_checkers

logger = logging.getLogger(__name__)


def _build_cluster_meta(cluster: dict, nodes: list[dict]) -> dict:
    """Convert SQLite cluster/nodes rows into the checker-expected format.

    Returns:
        dict with keys 'version', 'master' (dict), 'slaves' (list[dict]).
    """
    masters = [n for n in nodes if n["role"] == "master"]
    slaves = [n for n in nodes if n["role"] == "slave"]

    master_node = masters[0] if masters else nodes[0]
    version = master_node.get("version", "8.0")

    return {
        "version": version,
        "master": {
            "host": master_node["host"],
            "port": master_node["port"],
            "role": "master",
            "user": master_node.get("user", "root"),
            "password": master_node.get("password", ""),
        },
        "slaves": [
            {
                "host": s["host"],
                "port": s["port"],
                "role": "slave",
                "user": s.get("user", "root"),
                "password": s.get("password", ""),
            }
            for s in slaves
        ],
    }


def _make_connect_func(cluster_meta: dict) -> callable:
    """Return a connect_func(host, port) that creates PyMySQL connections.

    Reuses credentials from the cluster metadata (sourced from the main
    repo's SQLite store), avoiding redundant configuration.
    """
    master_user = cluster_meta["master"].get("user", "root")
    master_password = cluster_meta["master"].get("password", "")
    version = cluster_meta.get("version", "")

    # Build a per-node credential lookup for cases where nodes have
    # different credentials.
    node_creds = {}
    node_creds[(cluster_meta["master"]["host"], cluster_meta["master"]["port"])] = (
        cluster_meta["master"].get("user", master_user),
        cluster_meta["master"].get("password", master_password),
    )
    for s in cluster_meta["slaves"]:
        node_creds[(s["host"], s["port"])] = (
            s.get("user", master_user),
            s.get("password", master_password),
        )

    def connect(host, port):
        user, password = node_creds.get((host, port), (master_user, master_password))
        logger.debug("Connecting to %s:%d as %s", host, port, user)
        conn = pymysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            connect_timeout=5,
            read_timeout=10,
        )
        conn._mysql_version = version
        return conn
    return connect


def _make_mock_connect_func(version_hint: str) -> callable:
    """Return a mock connect_func for --mock mode.

    Creates a lightweight mock connection object that returns simulated
    query results so checkers can run without live MySQL instances.
    """
    import random

    class MockCursor:
        def __init__(self):
            self._result = None

        def execute(self, sql):
            sql_lower = sql.strip().lower()
            if "threads_connected" in sql_lower:
                self._result = [("Threads_connected", str(random.randint(5, 120)))]
            elif "information_schema.schemata" in sql_lower:
                self._result = [(random.randint(0, 3),)]
            elif "information_schema.tables" in sql_lower:
                self._result = [(random.randint(100, 5000),)]
            else:
                self._result = []

        def fetchone(self):
            return self._result[0] if self._result else None

        def fetchall(self):
            return self._result

        def close(self):
            pass

    class MockConnection:
        def __init__(self):
            self._mysql_version = mk.mock_version(version_hint)

        def cursor(self):
            return MockCursor()

        def close(self):
            pass

    def connect(host, port):
        return MockConnection()

    return connect


def run_inspection(cluster_uuid: str, use_mock: bool = False,
                   output_dir: str | None = None) -> dict:
    """Run all registered checkers against the target cluster.

    Args:
        cluster_uuid: The target cluster UUID (or name).
        use_mock: If True, use mock data instead of live connections.
        output_dir: If provided, write JSON/text reports to this directory.

    Returns:
        dict of checker results keyed by checker name.
    """
    # Step 1: Resolve cluster from the main repo's metadata store
    cluster, nodes = db.get_cluster(cluster_uuid)
    if not cluster:
        raise ValueError(f"Cluster '{cluster_uuid}' not found in topology store.")

    # Step 2: Build cluster_meta in checker-expected format
    cluster_meta = _build_cluster_meta(cluster, nodes)

    # Step 3: Build the connection factory
    if use_mock:
        connect_func = _make_mock_connect_func(cluster_meta["version"])
    else:
        connect_func = _make_connect_func(cluster_meta)

    # Step 4: Execute each registered checker
    checkers = get_all_checkers()
    results = {}

    for name, entry in checkers.items():
        desc = entry["description"]
        func = entry["func"]
        logger.info("Running checker: %s — %s", name, desc)

        try:
            result = func(cluster_meta, connect_func)
            results[name] = result
            logger.info("Checker '%s' completed — status=%s", name, result.get("status"))
        except Exception as exc:
            logger.exception("Checker '%s' crashed: %s", name, exc)
            results[name] = {
                "status": "Unhealthy",
                "error": f"Checker crashed: {exc}",
            }

    # Step 5: Optionally write file reports
    if output_dir:
        run_timestamp = datetime.now()
        _write_json_report(results, cluster_uuid, run_timestamp, output_dir)

    return results


def _write_json_report(results: dict, cluster_uuid: str,
                       run_timestamp: datetime, output_dir: str) -> str:
    """Write a structured JSON report."""
    ts = run_timestamp.strftime("%Y%m%d_%H%M%S")
    filename = f"{cluster_uuid}_{ts}_report.json"
    filepath = os.path.join(output_dir, filename)

    overall_healthy = all(
        r.get("status") == "Healthy" for r in results.values()
    )
    report = {
        "cluster_uuid": cluster_uuid,
        "timestamp": run_timestamp.isoformat(),
        "overall_status": "Healthy" if overall_healthy else "Unhealthy",
        "checks": results,
    }
    with open(filepath, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, ensure_ascii=False)

    logger.info("JSON report written to %s", filepath)
    return filepath
