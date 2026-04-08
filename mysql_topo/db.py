"""SQLite metadata store for cluster topology."""

import sqlite3
import uuid
import json
import os

DB_PATH = os.path.join(os.path.expanduser("~"), ".mysql_topo", "topology.db")


def _ensure_dir():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)


def get_conn() -> sqlite3.Connection:
    _ensure_dir()
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db():
    conn = get_conn()
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS clusters (
            uuid        TEXT PRIMARY KEY,
            name        TEXT NOT NULL UNIQUE,
            description TEXT DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS nodes (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            cluster_uuid TEXT NOT NULL REFERENCES clusters(uuid) ON DELETE CASCADE,
            host        TEXT NOT NULL,
            port        INTEGER NOT NULL DEFAULT 3306,
            role        TEXT NOT NULL CHECK(role IN ('master','slave')),
            master_host TEXT,
            master_port INTEGER,
            user        TEXT DEFAULT 'root',
            password    TEXT DEFAULT '',
            version     TEXT DEFAULT '',
            UNIQUE(host, port)
        );
    """)
    conn.commit()
    conn.close()


def list_clusters():
    conn = get_conn()
    rows = conn.execute("""
        SELECT c.uuid, c.name, c.description, COUNT(n.id) AS node_count
        FROM clusters c
        LEFT JOIN nodes n ON n.cluster_uuid = c.uuid
        GROUP BY c.uuid
    """).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def get_cluster(identifier: str):
    """Look up cluster by UUID or name."""
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM clusters WHERE uuid = ? OR name = ?",
        (identifier, identifier),
    ).fetchone()
    if not row:
        conn.close()
        return None, []
    nodes = conn.execute(
        "SELECT * FROM nodes WHERE cluster_uuid = ? ORDER BY role, host",
        (row["uuid"],),
    ).fetchall()
    conn.close()
    return dict(row), [dict(n) for n in nodes]


def get_node_by_host(host: str):
    conn = get_conn()
    row = conn.execute(
        "SELECT * FROM nodes WHERE host = ?", (host,)
    ).fetchone()
    conn.close()
    return dict(row) if row else None


def import_config(data: dict):
    """Import clusters and nodes from a parsed JSON config."""
    conn = get_conn()
    for cluster in data.get("clusters", []):
        cluster_uuid = cluster.get("uuid", str(uuid.uuid4()))
        conn.execute(
            "INSERT OR REPLACE INTO clusters (uuid, name, description) VALUES (?, ?, ?)",
            (cluster_uuid, cluster["name"], cluster.get("description", "")),
        )
        for node in cluster.get("nodes", []):
            conn.execute(
                """INSERT OR REPLACE INTO nodes
                   (cluster_uuid, host, port, role, master_host, master_port, user, password, version)
                   VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    cluster_uuid,
                    node["host"],
                    node.get("port", 3306),
                    node["role"],
                    node.get("master_host", ""),
                    node.get("master_port", 0),
                    node.get("user", "root"),
                    node.get("password", ""),
                    node.get("version", ""),
                ),
            )
    conn.commit()
    conn.close()
