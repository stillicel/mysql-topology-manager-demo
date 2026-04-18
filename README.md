# mysql-topo

**Current version: `v0.2.0`** — see [release-notes.txt](release-notes.txt) for the full changelog.

> **Note:** This project is a demo for testing purposes only and is not intended for use in a production environment.

A unified CLI tool for MySQL topology management and cluster health inspection. Supports MySQL 5.7, 8.0, and 8.4 with version-aware SQL adapters, automated health checks, and a plugin-based inspection framework.

## Features

- **Multi-Version Support** — Compatible with MySQL 5.7, 8.0, and 8.4. Automatically adapts replication queries (`SHOW SLAVE STATUS` vs `SHOW REPLICA STATUS`) and field names based on the detected version.
- **Topology Management** — Import cluster metadata from JSON, visualize master-slave topology trees, and monitor live node status (threads, replication lag, semi-sync).
- **Node Diagnostics** — Aggregated processlist summaries, deadlock detection from InnoDB status, row lock and metadata lock analysis, and database listing.
- **Cluster Health Inspection** — Plugin-based checker system that evaluates connection counts, topology scale, and schema scale against configurable thresholds.
- **Mock Mode** — A global `--mock` flag simulates all MySQL metrics for testing and demos without live database connections.
- **Rich Terminal UI** — Tables, trees, panels, and color-coded status indicators via the `rich` library.
- **K8s-Native DBaaS Compatible** — Metadata-driven architecture via SQLite makes it easy to integrate with Kubernetes operators and DBaaS platforms for automated cluster registration.

## Installation

Requires Python 3.10+.

```bash
pip install -e .
```

Or install dependencies manually:

```bash
pip install click rich PyMySQL
```

## CLI Usage

All commands support the `--mock` flag for simulated data.

```bash
mysql-topo [--mock] <command> [args]
```

### Import Cluster Configuration

Load cluster metadata from a JSON file into the local topology store:

```bash
mysql-topo import-config sample_config.json
```

### List Clusters

```bash
mysql-topo list-cluster
```

Output:

```
┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┳━━━━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━┓
┃ UUID                                 ┃ Cluster Name  ┃ Nodes ┃ Description         ┃
┡━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━╇━━━━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━┩
│ a1b2c3d4-e5f6-7890-abcd-ef1234567890 │ MySQL-order   │   3   │ Order service ...   │
└──────────────────────────────────────┴───────────────┴───────┴─────────────────────┘
```

### Show Cluster Info

View topology tree and live node status:

```bash
mysql-topo show-cluster-info <UUID|Name>
mysql-topo --mock show-cluster-info order
```

Displays a master-slave topology tree and a live status table with version, thread count, replication lag, and semi-sync status per node.

### Show Node Detail

Deep diagnostics for a single node:

```bash
mysql-topo show-node-detail <Host/IP>
mysql-topo --mock show-node-detail 10.0.1.10
```

Outputs:
- Processlist summary (by Command, by User)
- Latest detected deadlock timestamp
- Row lock and metadata lock (MDL) summary
- Database list

### Cluster Health Check

Run the full inspection suite on a cluster:

```bash
mysql-topo cluster-check <UUID|Name>
mysql-topo --mock cluster-check a1b2c3d4-e5f6-7890-abcd-ef1234567890
```

Optionally save a JSON report:

```bash
mysql-topo --mock cluster-check order --output-dir ./reports
```

Output:

```
╭──────────────── Cluster Health Inspection ─────────────────╮
│ MySQL-order  (a1b2c3d4-e5f6-7890-abcd-ef1234567890)       │
│ Nodes: 3  |  Mode: Mock                                   │
╰────────────────────────────────────────────────────────────╯
┏━━━━━━━━━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃ Check            ┃ Status  ┃ Details                     ┃
┡━━━━━━━━━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┩
│ connection_count │ Healthy │ 10.0.1.10:3306=42 ok; ...   │
│ topology_scale   │ Healthy │ slaves=2 (max=5)            │
│ schema_scale     │ Healthy │ user_dbs=1/5  tables=230/...│
└──────────────────┴─────────┴─────────────────────────────┘

Overall Status: Healthy
```

**Health checks performed:**

| Check | Description | Threshold |
|-------|-------------|-----------|
| `connection_count` | `Threads_connected` on all nodes | > 3,500 per node |
| `topology_scale` | Number of slave nodes | > 5 slaves |
| `schema_scale` | User database count (`shopee_*`) and InnoDB table count on master | > 5 DBs or > 10,000 tables |
| `storage_check` | Physical tablespace size via `ALLOCATED_SIZE` (InnoDB) on master | > 10 GB per table, > 300 GB per schema |
| `fragmentation_check` | InnoDB fragmentation ratio on tables > 100 MB on master | > 30% free ratio |

### Show Version Info

Print the installed `mysql-topo` version and the location of the release notes:

```bash
mysql-topo show-version-info
```

Output:

```
╭──────────────────── Version Info ─────────────────────╮
│ mysql-topo  v0.2.0                                    │
│ Release notes: /path/to/release-notes.txt             │
╰───────────────────────────────────────────────────────╯
```

See [release-notes.txt](release-notes.txt) for the full changelog.

### Show InnoDB TPC Status

Show InnoDB Transparent Page Compression (TPC) usage aggregated by database for a single node:

```bash
mysql-topo show-innodb-tpc-status <Host/IP>
mysql-topo --mock show-innodb-tpc-status 10.0.1.10
```

Output:

```
╭───────────────────────────── InnoDB TPC Status ──────────────────────────────╮
│ 10.0.1.10:3306  —  8.0.39-commercial                                        │
╰──────────────────────────────────────────────────────────────────────────────╯
               InnoDB Transparent Page Compression — by Database
┏━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━┳━━━━━━━━━━━━━┓
┃ Database   ┃ Total      ┃ Compressed ┃ Total      ┃ Total      ┃ Compression ┃
┃            ┃ Tables     ┃ Tables     ┃ Logic Size ┃ Phys. Size ┃ Ratio (%)   ┃
┡━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━╇━━━━━━━━━━━━━┩
│ analytics  │          3 │          2 │    4.39 GB │    2.52 GB │       57.3% │
│ shop       │          5 │          4 │    5.59 GB │    4.19 GB │       74.9% │
│ user_serv… │          4 │          2 │    4.42 GB │    3.70 GB │       83.7% │
└────────────┴────────────┴────────────┴────────────┴────────────┴─────────────┘
```

Queries `INNODB_SYS_TABLESPACES` (MySQL 5.7) or `INNODB_TABLESPACES` (MySQL 8.0/8.4) and aggregates per schema:
- **Total Tables** — count of all InnoDB tablespaces in the schema
- **Compressed Tables** — count of tables with TPC enabled (`COMPRESSION != 'None'`)
- **Total Logic Size** — sum of `FILE_SIZE` (logical data size)
- **Total Physical Size** — sum of `ALLOCATED_SIZE` (actual disk usage after punch-hole compression)
- **Compression Ratio** — `ALLOCATED_SIZE / FILE_SIZE * 100` (lower = better compression)

### Storage & Fragmentation Health Checks

Two additional checks focus on physical storage health using InnoDB internals rather than logical `TABLES.data_length` estimates.

**`storage_check` — Physical Space Monitoring**

Queries `INNODB_SYS_TABLESPACES` (MySQL 5.7) or `INNODB_TABLESPACES` (MySQL 8.0/8.4) and reads the `ALLOCATED_SIZE` column, which reflects actual disk usage including Transparent Page Compression. System schemas (`mysql`, `sys`, `information_schema`, `performance_schema`) are excluded.

* **Table-level threshold:** any single tablespace with `ALLOCATED_SIZE` > **10 GB** is flagged as Unhealthy.
* **Schema-level threshold:** total `ALLOCATED_SIZE` for all tables in a schema > **300 GB** is flagged as Unhealthy.

**`fragmentation_check` — InnoDB Fragmentation**

First filters tables by physical size: only tablespaces with `ALLOCATED_SIZE` > **100 MB** are evaluated (small tables are skipped to reduce noise). For qualifying tables, the fragmentation ratio is computed from `information_schema.TABLES`:

```
ratio = data_free / (data_length + index_length + data_free)
```

If the ratio exceeds **30%**, the table is marked as Unhealthy. This indicates significant wasted space that may benefit from `OPTIMIZE TABLE` or a table rebuild.

Both checks are version-aware and run on the master node only. They handle permission errors and version mismatches gracefully without crashing.

## Project Structure

```
mysql_topo/
├── __init__.py
├── cli.py              # CLI entry point — all commands registered here
├── connector.py        # MySQLClient — version-aware connection wrapper
├── db.py               # SQLite metadata store (topology.db)
├── mock.py             # Mock data engine for 5.7 / 8.0 / 8.4
├── inspector.py        # Cluster inspection engine (bridges metadata + checkers)
└── checkers/
    ├── __init__.py         # Plugin registry (@register_checker decorator)
    ├── connection_count.py     # Threads_connected threshold check
    ├── topology_scale.py       # Slave count limit check
    ├── schema_scale.py         # Database & table count check
    ├── storage_check.py        # Physical tablespace size check (ALLOCATED_SIZE)
    └── fragmentation_check.py  # InnoDB fragmentation ratio check

pyproject.toml          # Package metadata and dependencies
sample_config.json      # Example cluster configuration (3 clusters)
```

### Architecture

- **`cli.py`** — Click-based command group. Routes commands, manages the `--mock` flag, and renders output with Rich.
- **`connector.py`** — `MySQLClient` class wrapping PyMySQL with lazy connections, version detection, and version-adaptive SQL for replication, semi-sync, processlist, InnoDB status, and lock queries.
- **`db.py`** — SQLite-backed metadata store at `~/.mysql_topo/topology.db`. Stores cluster and node information with foreign key constraints.
- **`inspector.py`** — Inspection engine that resolves cluster metadata from SQLite, builds a connection factory using stored credentials, and dispatches registered checkers.
- **`checkers/`** — Self-registering plugin modules. Each checker receives `(cluster_meta, connect_func)` and returns a status dict. New checks can be added by creating a module with `@register_checker`.

## Configuration

Cluster metadata is stored in `~/.mysql_topo/topology.db` and imported via JSON:

```json
{
  "clusters": [
    {
      "uuid": "a1b2c3d4-...",
      "name": "order",
      "description": "Order service MySQL cluster",
      "nodes": [
        {
          "host": "10.0.1.10",
          "port": 3306,
          "role": "master",
          "user": "admin",
          "password": "",
          "version": "8.0"
        },
        {
          "host": "10.0.1.11",
          "port": 3306,
          "role": "slave",
          "master_host": "10.0.1.10",
          "master_port": 3306,
          "user": "admin",
          "password": "",
          "version": "8.0"
        }
      ]
    }
  ]
}
```

## MySQL Version Compatibility

| Feature | MySQL 5.7 | MySQL 8.0 | MySQL 8.4 |
|---------|-----------|-----------|-----------|
| Replication Status | `SHOW SLAVE STATUS` | `SHOW SLAVE STATUS` | `SHOW REPLICA STATUS` |
| Lag Field | `Seconds_Behind_Master` | `Seconds_Behind_Master` | `Seconds_Behind_Source` |
| IO Thread Field | `Slave_IO_Running` | `Slave_IO_Running` | `Replica_IO_Running` |
| SQL Thread Field | `Slave_SQL_Running` | `Slave_SQL_Running` | `Replica_SQL_Running` |
| Semi-Sync Variables | `rpl_semi_sync%` | `rpl_semi_sync%` | `rpl_semi_sync%` |
| InnoDB Tablespaces | `INNODB_SYS_TABLESPACES` | `INNODB_TABLESPACES` | `INNODB_TABLESPACES` |
| MDL Locks | `performance_schema` | `performance_schema` | `performance_schema` |

## License

This project is provided as-is for demonstration and internal tooling purposes.
