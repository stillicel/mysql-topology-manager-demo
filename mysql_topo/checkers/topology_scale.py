"""Checker: Topology Scale

If the number of slaves in the cluster exceeds 5, mark as Unhealthy.
"""

import logging

from mysql_topo.checkers import register_checker

logger = logging.getLogger(__name__)

MAX_SLAVES = 5


@register_checker(
    name="topology_scale",
    description="Check if slave count exceeds the maximum allowed",
)
def check_topology_scale(cluster_meta, connect_func):
    """Check the number of slave nodes.

    Args:
        cluster_meta: Dict with keys 'master', 'slaves', 'version'.
        connect_func: Callable(host, port) -> mysql connection (unused here).

    Returns:
        dict with 'status', 'slave_count', and 'max_slaves'.
    """
    slave_count = len(cluster_meta["slaves"])
    is_healthy = slave_count <= MAX_SLAVES

    logger.info(
        "Topology scale — slave_count=%d, max_allowed=%d, healthy=%s",
        slave_count, MAX_SLAVES, is_healthy,
    )

    return {
        "status": "Healthy" if is_healthy else "Unhealthy",
        "slave_count": slave_count,
        "max_slaves": MAX_SLAVES,
    }
