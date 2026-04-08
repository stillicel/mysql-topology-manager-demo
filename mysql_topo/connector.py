"""MySQL connector with version-aware query adapters."""

from __future__ import annotations

import re
import pymysql
from mysql_topo import mock as mk


class MySQLClient:
    """Thin wrapper around a single MySQL node connection."""

    def __init__(self, host: str, port: int, user: str, password: str,
                 version_hint: str = "", use_mock: bool = False, role: str = "master"):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.version_hint = version_hint
        self.use_mock = use_mock
        self.role = role
        self._conn: pymysql.connections.Connection | None = None
        self._version: str | None = None

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    def _connect(self):
        if self._conn is None:
            self._conn = pymysql.connect(
                host=self.host, port=self.port,
                user=self.user, password=self.password,
                connect_timeout=5, read_timeout=10,
                cursorclass=pymysql.cursors.DictCursor,
            )

    def close(self):
        if self._conn:
            self._conn.close()
            self._conn = None

    def _query(self, sql: str) -> list[dict]:
        self._connect()
        with self._conn.cursor() as cur:
            cur.execute(sql)
            return cur.fetchall()

    # ------------------------------------------------------------------
    # Version detection
    # ------------------------------------------------------------------

    def get_version(self) -> str:
        if self.use_mock:
            self._version = mk.mock_version(self.version_hint or "8.0")
            return self._version
        rows = self._query("SELECT VERSION() AS v")
        self._version = rows[0]["v"]
        return self._version

    def _ver(self) -> str:
        """Return cached version string, fetching if needed."""
        if self._version is None:
            self.get_version()
        return self._version

    def _is_57(self) -> bool:
        return self._ver().startswith("5.7")

    # ------------------------------------------------------------------
    # Global status / variables
    # ------------------------------------------------------------------

    def get_global_status(self) -> dict:
        if self.use_mock:
            return mk.mock_global_status(self.version_hint)
        rows = self._query("SHOW GLOBAL STATUS")
        return {r["Variable_name"]: r["Value"] for r in rows}

    # ------------------------------------------------------------------
    # Replication
    # ------------------------------------------------------------------

    def get_slave_status(self) -> dict | None:
        if self.use_mock:
            return mk.mock_slave_status(self.version_hint, self.role)
        ver = self._ver()
        if ver.startswith("8.4"):
            sql = "SHOW REPLICA STATUS"
        else:
            sql = "SHOW SLAVE STATUS"
        rows = self._query(sql)
        if not rows:
            return None
        row = rows[0]
        sbm_key = "Seconds_Behind_Source" if ver.startswith("8.4") else "Seconds_Behind_Master"
        io_key = "Replica_IO_Running" if ver.startswith("8.4") else "Slave_IO_Running"
        sql_key = "Replica_SQL_Running" if ver.startswith("8.4") else "Slave_SQL_Running"
        return {
            "Seconds_Behind_Master": row.get(sbm_key),
            "Slave_IO_Running": row.get(io_key, ""),
            "Slave_SQL_Running": row.get(sql_key, ""),
            "Master_Host": row.get("Master_Host", row.get("Source_Host", "")),
            "Master_Port": row.get("Master_Port", row.get("Source_Port", 0)),
        }

    # ------------------------------------------------------------------
    # Semi-sync
    # ------------------------------------------------------------------

    def get_semi_sync_status(self) -> dict:
        """Return semi-sync replication variables (ON/OFF)."""
        if self.use_mock:
            return mk.mock_semi_sync_status(self.version_hint, self.role)
        rows = self._query(
            "SHOW GLOBAL VARIABLES LIKE 'rpl_semi_sync%'"
        )
        return {r["Variable_name"]: r["Value"] for r in rows}

    # ------------------------------------------------------------------
    # Processlist aggregation
    # ------------------------------------------------------------------

    def get_processlist_summary(self) -> dict:
        if self.use_mock:
            return mk.mock_processlist_summary()
        rows = self._query("SELECT command, user FROM information_schema.processlist")
        by_command: dict[str, int] = {}
        by_user: dict[str, int] = {}
        for r in rows:
            cmd = r["command"] or "Unknown"
            usr = r["user"] or "Unknown"
            by_command[cmd] = by_command.get(cmd, 0) + 1
            by_user[usr] = by_user.get(usr, 0) + 1
        return {"by_command": by_command, "by_user": by_user}

    # ------------------------------------------------------------------
    # InnoDB status & deadlock
    # ------------------------------------------------------------------

    def get_innodb_status(self) -> str:
        if self.use_mock:
            return mk.mock_innodb_status(self.version_hint)
        rows = self._query("SHOW ENGINE INNODB STATUS")
        return rows[0].get("Status", "") if rows else ""

    @staticmethod
    def parse_deadlock_timestamp(innodb_status: str) -> str | None:
        """Extract the timestamp from LATEST DETECTED DEADLOCK section."""
        m = re.search(
            r"LATEST DETECTED DEADLOCK\s*\n-+\n(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})",
            innodb_status,
        )
        return m.group(1) if m else None

    # ------------------------------------------------------------------
    # Lock summary
    # ------------------------------------------------------------------

    def get_lock_summary(self) -> dict:
        if self.use_mock:
            return mk.mock_lock_summary(self.version_hint)
        status = self.get_global_status()
        row_locks = {
            "current_row_locks": int(status.get("Innodb_row_lock_current_waits", 0)),
            "row_lock_waits": int(status.get("Innodb_row_lock_waits", 0)),
            "row_lock_time_avg_ms": int(status.get("Innodb_row_lock_time_avg", 0)),
        }
        # MDL locks from performance_schema (available 5.7+)
        mdl_locks = []
        try:
            mdl_rows = self._query(
                "SELECT OBJECT_SCHEMA, OBJECT_NAME, LOCK_TYPE, LOCK_STATUS, OWNER_THREAD_ID "
                "FROM performance_schema.metadata_locks "
                "WHERE OBJECT_TYPE = 'TABLE' LIMIT 50"
            )
            for r in mdl_rows:
                mdl_locks.append({
                    "object_schema": r["OBJECT_SCHEMA"],
                    "object_name": r["OBJECT_NAME"],
                    "lock_type": r["LOCK_TYPE"],
                    "lock_status": r["LOCK_STATUS"],
                    "owner_thread_id": r["OWNER_THREAD_ID"],
                })
        except Exception:
            pass
        return {"row_locks": row_locks, "mdl_locks": mdl_locks}

    # ------------------------------------------------------------------
    # Databases
    # ------------------------------------------------------------------

    def get_databases(self) -> list[str]:
        if self.use_mock:
            return mk.mock_databases()
        rows = self._query("SHOW DATABASES")
        return [r["Database"] for r in rows]
