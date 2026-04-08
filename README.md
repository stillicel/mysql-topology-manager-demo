
*This is a demo and for testing purpose.*

*Here is the prompt I was used to create the project.*

### 🚀 Claude Code Task: MySQL Multi-Version Topology & Diagnostic Tool (`mysql-topo`)

**Role**: You are a Senior Python Developer and MySQL Expert. Your goal is to build a **read-only** CLI tool named `mysql-topo` for managing and diagnosing MySQL clusters.

**Core Objective**:
1. **Multi-Version Support**: Must be compatible with MySQL 5.7, 8.0, and 8.4.
2. **Metadata Driven**: Use a local `topology.db` (SQLite) to store cluster metadata.
3. **UI/UX**: Use the `rich` library for beautiful tables, tree views, and status highlighting (e.g., Red for Offline or high lag).
4. **GitHub Automation**: Once the code is verified, use the `gh` CLI to create a private repository and push the code.

**CLI Commands & Requirements**:

#### 1. `list-cluster`
* List all managed clusters in a table.
* **Naming Rule**: Display cluster names as `MySQL-{name}` (e.g., `MySQL-order`).
* Columns: UUID, Formatted Name, Node Count, Description.

#### 2. `show-cluster-info <UUID|Name>`
* **Topology Tree**: Print a recursive tree view showing Master and its Slaves.
* **Live Status Summary**: Connect to nodes in real-time and display: `Version`, `Threads_connected`, `Replication_Lag` (Seconds_Behind_Master).
* **Semi-Sync Check**: Encapsulate this in a function (e.g., `get_semi_sync_status`). For now, implement a simplified check using core variables (like `rpl_semi_sync_master_enabled`) to show if it's ON/OFF.

#### 3. `show-node-detail <Host/IP>`
* **Aggregated Processlist**: Do NOT print the raw list. Summarize by `Command` and `User` (e.g., `Query: 5, Sleep: 20`).
* **Deadlock Diagnosis**: Parse `SHOW ENGINE INNODB STATUS` to extract and display the **precise timestamp** of the `LATEST DETECTED DEADLOCK`.
* **Lock Status**: Summarize current Row Locks and Metadata Locks (MDL) using `information_schema` or `performance_schema` where applicable.
* **Database List**: List all database names within the instance.

**Technical Constraints**:
* **Import Management**: Provide an `import-config` command to batch-load metadata from a JSON file. No granular `add-node` commands are needed.
* **Error Handling**: Distinctly mark unreachable nodes as **Offline** in the UI.
* **Mock Mode**: Implement a global `--mock` flag. When enabled, the tool should simulate metrics, aggregated processlists, and InnoDB status strings (including deadlock timestamps) for versions 5.7, 8.0, and 8.4.
* **SQL Compatibility**: Ensure SQL queries for metadata and diagnostics adapt to version-specific system tables (especially differences between 5.7 and 8.0+).


