"""Mock data engine simulating MySQL metrics for 5.7, 8.0, and 8.4."""

import random
import datetime

# ---------------------------------------------------------------------------
# Version-specific mock profiles
# ---------------------------------------------------------------------------

_VERSION_PROFILES = {
    "5.7": {
        "version_string": "5.7.44-log",
        "semi_sync_var_prefix": "rpl_semi_sync",
    },
    "8.0": {
        "version_string": "8.0.39-commercial",
        "semi_sync_var_prefix": "rpl_semi_sync",
    },
    "8.4": {
        "version_string": "8.4.5-commercial",
        "semi_sync_var_prefix": "rpl_semi_sync",
    },
}


def _profile(version: str) -> dict:
    for key, val in _VERSION_PROFILES.items():
        if version.startswith(key):
            return val
    return _VERSION_PROFILES["8.0"]


# ---------------------------------------------------------------------------
# Public mock helpers
# ---------------------------------------------------------------------------


def mock_global_status(version: str) -> dict:
    return {
        "Threads_connected": str(random.randint(5, 120)),
        "Threads_running": str(random.randint(1, 30)),
        "Questions": str(random.randint(100000, 9999999)),
        "Uptime": str(random.randint(86400, 8640000)),
        "Connections": str(random.randint(5000, 500000)),
    }


def mock_version(version: str) -> str:
    p = _profile(version)
    return p["version_string"]


def mock_slave_status(version: str, role: str) -> dict | None:
    if role == "master":
        return None
    lag = random.choice([0, 0, 0, 1, 2, 5, 30, None])
    io_running = random.choice(["Yes", "Yes", "Yes", "No"])
    sql_running = "Yes" if io_running == "Yes" else random.choice(["Yes", "No"])
    return {
        "Seconds_Behind_Master": lag,
        "Slave_IO_Running": io_running,
        "Slave_SQL_Running": sql_running,
        "Master_Host": "mock-master",
        "Master_Port": 3306,
    }


def mock_semi_sync_status(version: str, role: str) -> dict:
    p = _profile(version)
    prefix = p["semi_sync_var_prefix"]
    if role == "master":
        enabled = random.choice(["ON", "ON", "OFF"])
        return {
            f"{prefix}_master_enabled": enabled,
            f"{prefix}_master_clients": str(random.randint(0, 3)) if enabled == "ON" else "0",
        }
    else:
        enabled = random.choice(["ON", "ON", "OFF"])
        return {
            f"{prefix}_slave_enabled": enabled,
        }


def mock_processlist_summary() -> dict:
    return {
        "by_command": {
            "Query": random.randint(1, 20),
            "Sleep": random.randint(10, 80),
            "Binlog Dump": random.randint(0, 3),
            "Connect": random.randint(0, 2),
            "Daemon": 1,
        },
        "by_user": {
            "app_user": random.randint(15, 60),
            "repl_user": random.randint(1, 3),
            "root": random.randint(1, 5),
            "monitor": random.randint(1, 2),
        },
    }


def mock_innodb_status(version: str) -> str:
    ts = datetime.datetime.now() - datetime.timedelta(
        hours=random.randint(0, 48), minutes=random.randint(0, 59)
    )
    ts_str = ts.strftime("%Y-%m-%d %H:%M:%S")
    # Simulate a portion of SHOW ENGINE INNODB STATUS with deadlock info
    return f"""
=====================================
{ts_str} {hex(random.randint(0x1000, 0xFFFF))} INNODB MONITOR OUTPUT
=====================================
Per second averages calculated from the last 30 seconds
-----------------
BACKGROUND THREAD
-----------------
srv_master_thread loops: 542130 srv_active, 0 srv_shutdown, 21032 srv_idle
------------------------
LATEST DETECTED DEADLOCK
------------------------
{ts_str} {hex(random.randint(0x1000, 0xFFFF))}
*** (1) TRANSACTION:
TRANSACTION 84839243, ACTIVE 0 sec starting index read
mysql tables in use 1, locked 1
LOCK WAIT 3 lock struct(s), heap size 1136, 2 row lock(s)
MySQL thread id 1234, OS thread handle {hex(random.randint(0x1000, 0xFFFF))}, query id 999 localhost app_user updating
UPDATE orders SET status='shipped' WHERE id=42
*** (1) HOLDS THE LOCK(S):
RECORD LOCKS space id 100 page no 5 n bits 72 index PRIMARY of table `shop`.`orders`
*** (1) WAITING FOR THIS LOCK TO BE GRANTED:
RECORD LOCKS space id 101 page no 3 n bits 80 index PRIMARY of table `shop`.`inventory`

*** (2) TRANSACTION:
TRANSACTION 84839244, ACTIVE 0 sec starting index read
mysql tables in use 1, locked 1
3 lock struct(s), heap size 1136, 2 row lock(s)
MySQL thread id 1235, OS thread handle {hex(random.randint(0x1000, 0xFFFF))}, query id 1000 localhost app_user updating
UPDATE inventory SET qty=qty-1 WHERE item_id=42
*** (2) HOLDS THE LOCK(S):
RECORD LOCKS space id 101 page no 3 n bits 80 index PRIMARY of table `shop`.`inventory`
*** (2) WAITING FOR THIS LOCK TO BE GRANTED:
RECORD LOCKS space id 100 page no 5 n bits 72 index PRIMARY of table `shop`.`orders`

*** WE ROLL BACK TRANSACTION (2)
------------
TRANSACTIONS
------------
---TRANSACTION 84839300, not started
---
ROW OPERATIONS
--------------
0 queries inside InnoDB, 0 queries in queue
1 read views open inside InnoDB
Main thread id {hex(random.randint(0x1000, 0xFFFF))}, state: sleeping
"""


def mock_lock_summary(version: str) -> dict:
    row_locks = {
        "current_row_locks": random.randint(0, 50),
        "row_lock_waits": random.randint(0, 10),
        "row_lock_time_avg_ms": random.randint(0, 500),
    }
    mdl_locks = []
    if version.startswith("5.7") or version.startswith("8."):
        for _ in range(random.randint(0, 3)):
            mdl_locks.append({
                "object_schema": random.choice(["shop", "analytics", "sys"]),
                "object_name": random.choice(["orders", "users", "inventory"]),
                "lock_type": random.choice(["SHARED_READ", "SHARED_WRITE", "EXCLUSIVE"]),
                "lock_status": random.choice(["GRANTED", "PENDING"]),
                "owner_thread_id": random.randint(100, 9999),
            })
    return {"row_locks": row_locks, "mdl_locks": mdl_locks}


def mock_databases() -> list[str]:
    return [
        "information_schema",
        "mysql",
        "performance_schema",
        "sys",
        "shop",
        "analytics",
        "user_service",
    ]


def mock_innodb_tpc_status(version: str) -> list[dict]:
    """Simulate rows from INNODB_SYS_TABLESPACES / INNODB_TABLESPACES."""
    schemas = {
        "shop": ["orders", "inventory", "customers", "products", "reviews"],
        "analytics": ["events", "sessions", "pageviews"],
        "user_service": ["users", "profiles", "auth_tokens", "permissions"],
    }
    rows = []
    for db_name, tables in schemas.items():
        for tbl in tables:
            file_size = random.randint(8 * 1024 * 1024, 2 * 1024 * 1024 * 1024)
            compressed = random.choice([True, True, False])
            if compressed:
                ratio = random.uniform(0.3, 0.85)
                allocated_size = int(file_size * ratio)
                compression = random.choice(["zlib", "lz4"])
            else:
                allocated_size = file_size
                compression = "None"
            rows.append({
                "NAME": f"{db_name}/{tbl}",
                "FILE_SIZE": file_size,
                "ALLOCATED_SIZE": allocated_size,
                "COMPRESSION": compression,
            })
    return rows
