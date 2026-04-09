"""Checker: Schema Scale

Two sub-checks on the master node:
1. If user databases (names starting with 'shopee_') exceed 5 -> Unhealthy.
2. If total InnoDB tables across the instance exceed 10,000 -> Unhealthy.
"""

import logging

from mysql_topo.checkers import register_checker

logger = logging.getLogger(__name__)

MAX_USER_DATABASES = 5
MAX_INNODB_TABLES = 10000


@register_checker(
    name="schema_scale",
    description="Check user database count and InnoDB table count on master",
)
def check_schema_scale(cluster_meta, connect_func):
    """Check schema scale on the master node.

    Args:
        cluster_meta: Dict with keys 'master', 'slaves', 'version'.
        connect_func: Callable(host, port) -> mysql connection.

    Returns:
        dict with 'status' and sub-check details.
    """
    master = cluster_meta["master"]
    host, port = master["host"], master["port"]

    try:
        conn = connect_func(host, port)
        cursor = conn.cursor()

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.SCHEMATA "
            "WHERE SCHEMA_NAME LIKE 'shopee\\_%'"
        )
        user_db_count = cursor.fetchone()[0]

        cursor.execute(
            "SELECT COUNT(*) FROM information_schema.TABLES "
            "WHERE ENGINE = 'InnoDB'"
        )
        innodb_table_count = cursor.fetchone()[0]

        cursor.close()
        conn.close()

        db_healthy = user_db_count <= MAX_USER_DATABASES
        table_healthy = innodb_table_count <= MAX_INNODB_TABLES
        is_healthy = db_healthy and table_healthy

        logger.info(
            "Schema scale — user_databases=%d (max=%d), innodb_tables=%d (max=%d)",
            user_db_count, MAX_USER_DATABASES,
            innodb_table_count, MAX_INNODB_TABLES,
        )

        return {
            "status": "Healthy" if is_healthy else "Unhealthy",
            "user_databases": {
                "count": user_db_count,
                "max_allowed": MAX_USER_DATABASES,
                "healthy": db_healthy,
            },
            "innodb_tables": {
                "count": innodb_table_count,
                "max_allowed": MAX_INNODB_TABLES,
                "healthy": table_healthy,
            },
            "checked_node": {"host": host, "port": port},
        }

    except Exception as exc:
        logger.error("Failed to check schema scale on %s:%s — %s", host, port, exc)
        return {
            "status": "Unhealthy",
            "user_databases": None,
            "innodb_tables": None,
            "checked_node": {"host": host, "port": port},
            "error": str(exc),
        }
