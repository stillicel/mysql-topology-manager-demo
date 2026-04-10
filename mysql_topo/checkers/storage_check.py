"""Checker: Storage (Physical Space Monitoring)

Uses INNODB_SYS_TABLESPACES (MySQL 5.7) or INNODB_TABLESPACES (MySQL 8.0/8.4)
with ALLOCATED_SIZE to account for Transparent Page Compression.

Thresholds:
- Table level:  any tablespace with ALLOCATED_SIZE > 10 GB  -> Unhealthy
- Schema level: total ALLOCATED_SIZE per schema > 300 GB    -> Unhealthy

System schemas (mysql, sys, information_schema, performance_schema) are excluded.
"""

import logging

from mysql_topo.checkers import register_checker

logger = logging.getLogger(__name__)

TABLE_SIZE_THRESHOLD = 10 * 1024 * 1024 * 1024       # 10 GB
SCHEMA_SIZE_THRESHOLD = 300 * 1024 * 1024 * 1024      # 300 GB

SYSTEM_SCHEMAS = ("mysql", "sys", "information_schema", "performance_schema")

# SQL for MySQL 8.0 / 8.4 — information_schema.INNODB_TABLESPACES
_SQL_80 = (
    "SELECT t.NAME AS tablespace_name, t.ALLOCATED_SIZE "
    "FROM information_schema.INNODB_TABLESPACES t "
    "WHERE t.NAME NOT LIKE 'mysql/%%' "
    "AND t.NAME NOT LIKE 'sys/%%' "
    "AND t.NAME NOT LIKE 'information_schema/%%' "
    "AND t.NAME NOT LIKE 'performance_schema/%%' "
    "AND t.SPACE_TYPE = 'Single'"
)

# SQL for MySQL 5.7 — information_schema.INNODB_SYS_TABLESPACES
_SQL_57 = (
    "SELECT t.NAME AS tablespace_name, t.ALLOCATED_SIZE "
    "FROM information_schema.INNODB_SYS_TABLESPACES t "
    "WHERE t.NAME NOT LIKE 'mysql/%%' "
    "AND t.NAME NOT LIKE 'sys/%%' "
    "AND t.NAME NOT LIKE 'information_schema/%%' "
    "AND t.NAME NOT LIKE 'performance_schema/%%' "
    "AND t.SPACE_TYPE = 'Single'"
)


def _detect_version(cluster_meta):
    """Return the version string from cluster metadata."""
    return cluster_meta.get("version", "8.0")


@register_checker(
    name="storage_check",
    description="Check physical tablespace sizes using ALLOCATED_SIZE (InnoDB)",
)
def check_storage(cluster_meta, connect_func):
    """Check physical storage on the master node.

    Args:
        cluster_meta: Dict with keys 'master', 'slaves', 'version'.
        connect_func: Callable(host, port) -> mysql connection.

    Returns:
        dict with 'status' and details about oversized tables/schemas.
    """
    master = cluster_meta["master"]
    host, port = master["host"], master["port"]
    version = _detect_version(cluster_meta)

    try:
        conn = connect_func(host, port)
        cursor = conn.cursor()

        sql = _SQL_57 if version.startswith("5.7") else _SQL_80
        cursor.execute(sql)
        rows = cursor.fetchall()

        cursor.close()
        conn.close()

        oversized_tables = []
        schema_totals = {}  # schema_name -> total ALLOCATED_SIZE

        for row in rows:
            # tablespace_name format: "schema/table"
            ts_name = row[0] if isinstance(row, (list, tuple)) else row.get("tablespace_name", row.get("NAME", ""))
            alloc = int(row[1] if isinstance(row, (list, tuple)) else row.get("ALLOCATED_SIZE", 0))

            if "/" not in ts_name:
                continue

            schema, table = ts_name.split("/", 1)

            if schema.lower() in SYSTEM_SCHEMAS:
                continue

            schema_totals[schema] = schema_totals.get(schema, 0) + alloc

            if alloc > TABLE_SIZE_THRESHOLD:
                oversized_tables.append({
                    "schema": schema,
                    "table": table,
                    "allocated_size_bytes": alloc,
                    "allocated_size_gb": round(alloc / (1024 ** 3), 2),
                })

        oversized_schemas = []
        for schema, total in schema_totals.items():
            if total > SCHEMA_SIZE_THRESHOLD:
                oversized_schemas.append({
                    "schema": schema,
                    "total_allocated_bytes": total,
                    "total_allocated_gb": round(total / (1024 ** 3), 2),
                })

        is_healthy = len(oversized_tables) == 0 and len(oversized_schemas) == 0

        logger.info(
            "Storage check — oversized_tables=%d, oversized_schemas=%d",
            len(oversized_tables), len(oversized_schemas),
        )

        return {
            "status": "Healthy" if is_healthy else "Unhealthy",
            "table_threshold_gb": 10,
            "schema_threshold_gb": 300,
            "oversized_tables": oversized_tables,
            "oversized_schemas": oversized_schemas,
            "checked_node": {"host": host, "port": port},
            "innodb_catalog": "INNODB_SYS_TABLESPACES" if version.startswith("5.7") else "INNODB_TABLESPACES",
        }

    except Exception as exc:
        logger.error("Failed storage check on %s:%s — %s", host, port, exc)
        return {
            "status": "Unhealthy",
            "oversized_tables": None,
            "oversized_schemas": None,
            "checked_node": {"host": host, "port": port},
            "error": str(exc),
        }
