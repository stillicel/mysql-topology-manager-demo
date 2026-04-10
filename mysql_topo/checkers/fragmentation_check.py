"""Checker: InnoDB Fragmentation

Only inspects tables whose physical size (ALLOCATED_SIZE) exceeds 100 MB.
Uses information_schema.TABLES to compute:

    ratio = data_free / (data_length + index_length + data_free)

If ratio > 30 %, the table is marked Unhealthy.

Version-aware: queries INNODB_SYS_TABLESPACES (5.7) or INNODB_TABLESPACES
(8.0/8.4) to obtain ALLOCATED_SIZE for the 100 MB filter.
"""

import logging

from mysql_topo.checkers import register_checker

logger = logging.getLogger(__name__)

MIN_SIZE_BYTES = 100 * 1024 * 1024          # 100 MB — skip smaller tables
FRAGMENTATION_THRESHOLD = 0.30              # 30 %

SYSTEM_SCHEMAS = ("mysql", "sys", "information_schema", "performance_schema")

# Collect ALLOCATED_SIZE keyed by schema/table for the size filter.
_ALLOC_SQL_80 = (
    "SELECT NAME, ALLOCATED_SIZE "
    "FROM information_schema.INNODB_TABLESPACES "
    "WHERE SPACE_TYPE = 'Single'"
)

_ALLOC_SQL_57 = (
    "SELECT NAME, ALLOCATED_SIZE "
    "FROM information_schema.INNODB_SYS_TABLESPACES "
    "WHERE SPACE_TYPE = 'Single'"
)

# Fragmentation data from information_schema.TABLES
_FRAG_SQL = (
    "SELECT TABLE_SCHEMA, TABLE_NAME, DATA_LENGTH, INDEX_LENGTH, DATA_FREE "
    "FROM information_schema.TABLES "
    "WHERE ENGINE = 'InnoDB' "
    "AND TABLE_SCHEMA NOT IN ('mysql','sys','information_schema','performance_schema')"
)


@register_checker(
    name="fragmentation_check",
    description="Check InnoDB fragmentation ratio on tables larger than 100 MB",
)
def check_fragmentation(cluster_meta, connect_func):
    """Check InnoDB fragmentation on the master node.

    Args:
        cluster_meta: Dict with keys 'master', 'slaves', 'version'.
        connect_func: Callable(host, port) -> mysql connection.

    Returns:
        dict with 'status' and per-table fragmentation details.
    """
    master = cluster_meta["master"]
    host, port = master["host"], master["port"]
    version = cluster_meta.get("version", "8.0")

    try:
        conn = connect_func(host, port)
        cursor = conn.cursor()

        # Step 1: build a set of tablespaces that exceed 100 MB
        alloc_sql = _ALLOC_SQL_57 if version.startswith("5.7") else _ALLOC_SQL_80
        cursor.execute(alloc_sql)
        alloc_rows = cursor.fetchall()

        large_tables = set()  # (schema, table) pairs with ALLOCATED_SIZE > 100 MB
        for row in alloc_rows:
            ts_name = row[0] if isinstance(row, (list, tuple)) else row.get("NAME", "")
            alloc = int(row[1] if isinstance(row, (list, tuple)) else row.get("ALLOCATED_SIZE", 0))
            if "/" not in ts_name:
                continue
            schema, table = ts_name.split("/", 1)
            if schema.lower() in SYSTEM_SCHEMAS:
                continue
            if alloc > MIN_SIZE_BYTES:
                large_tables.add((schema, table))

        # Step 2: compute fragmentation ratio for qualifying tables
        cursor.execute(_FRAG_SQL)
        frag_rows = cursor.fetchall()

        cursor.close()
        conn.close()

        fragmented = []
        checked_count = 0

        for row in frag_rows:
            if isinstance(row, (list, tuple)):
                schema, table, data_len, idx_len, data_free = row
            else:
                schema = row.get("TABLE_SCHEMA", "")
                table = row.get("TABLE_NAME", "")
                data_len = row.get("DATA_LENGTH", 0)
                idx_len = row.get("INDEX_LENGTH", 0)
                data_free = row.get("DATA_FREE", 0)

            if (schema, table) not in large_tables:
                continue

            data_len = int(data_len or 0)
            idx_len = int(idx_len or 0)
            data_free = int(data_free or 0)

            total = data_len + idx_len + data_free
            if total == 0:
                continue

            checked_count += 1
            ratio = data_free / total

            if ratio > FRAGMENTATION_THRESHOLD:
                fragmented.append({
                    "schema": schema,
                    "table": table,
                    "data_free_bytes": data_free,
                    "total_bytes": total,
                    "fragmentation_ratio": round(ratio * 100, 2),
                })

        is_healthy = len(fragmented) == 0

        logger.info(
            "Fragmentation check — checked=%d tables (>100MB), fragmented=%d",
            checked_count, len(fragmented),
        )

        return {
            "status": "Healthy" if is_healthy else "Unhealthy",
            "min_size_mb": 100,
            "threshold_pct": 30,
            "tables_checked": checked_count,
            "fragmented_tables": fragmented,
            "checked_node": {"host": host, "port": port},
        }

    except Exception as exc:
        logger.error("Failed fragmentation check on %s:%s — %s", host, port, exc)
        return {
            "status": "Unhealthy",
            "fragmented_tables": None,
            "checked_node": {"host": host, "port": port},
            "error": str(exc),
        }
