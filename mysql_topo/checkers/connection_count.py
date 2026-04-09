"""Checker: Connection Count

Inspects Threads_connected on all nodes (master + slaves).
If any node has Threads_connected > 3500, the cluster is marked Unhealthy.
"""

import logging

from mysql_topo.checkers import register_checker

logger = logging.getLogger(__name__)

THRESHOLD = 3500


@register_checker(
    name="connection_count",
    description="Check if Threads_connected exceeds threshold on any node",
)
def check_connection_count(cluster_meta, connect_func):
    """Check Threads_connected on every node.

    Args:
        cluster_meta: Dict with keys 'master', 'slaves', 'version'.
        connect_func: Callable(host, port) -> mysql connection.

    Returns:
        dict with 'status', 'threshold', and per-node 'nodes' data.
    """
    all_nodes = [cluster_meta["master"]] + cluster_meta["slaves"]
    nodes_result = []
    is_healthy = True

    for node in all_nodes:
        host, port = node["host"], node["port"]
        role = node.get("role", "unknown")
        try:
            conn = connect_func(host, port)
            cursor = conn.cursor()
            cursor.execute("SHOW GLOBAL STATUS LIKE 'Threads_connected'")
            row = cursor.fetchone()
            threads_connected = int(row[1]) if row else 0
            cursor.close()
            conn.close()

            node_healthy = threads_connected <= THRESHOLD
            if not node_healthy:
                is_healthy = False

            nodes_result.append({
                "host": host,
                "port": port,
                "role": role,
                "threads_connected": threads_connected,
                "threshold": THRESHOLD,
                "healthy": node_healthy,
            })
            logger.info(
                "Node %s:%s (%s) — Threads_connected=%d (threshold=%d)",
                host, port, role, threads_connected, THRESHOLD,
            )
        except Exception as exc:
            is_healthy = False
            nodes_result.append({
                "host": host,
                "port": port,
                "role": role,
                "threads_connected": None,
                "threshold": THRESHOLD,
                "healthy": False,
                "error": str(exc),
            })
            logger.error(
                "Failed to check connection count on %s:%s — %s",
                host, port, exc,
            )

    return {
        "status": "Healthy" if is_healthy else "Unhealthy",
        "threshold": THRESHOLD,
        "nodes": nodes_result,
    }
