"""CLI entry-point for mysql-topo."""

from __future__ import annotations

import json
import sys

import click
from rich.console import Console
from rich.table import Table
from rich.tree import Tree
from rich.panel import Panel
from rich.text import Text

from mysql_topo import db
from mysql_topo.connector import MySQLClient
from mysql_topo.inspector import run_inspection

console = Console()


def _client(node: dict, use_mock: bool) -> MySQLClient:
    return MySQLClient(
        host=node["host"],
        port=node["port"],
        user=node.get("user", "root"),
        password=node.get("password", ""),
        version_hint=node.get("version", "8.0"),
        use_mock=use_mock,
        role=node.get("role", "master"),
    )


def _safe(fn, fallback="N/A"):
    """Run fn, return its result or fallback + offline flag on error."""
    try:
        return fn(), False
    except Exception:
        return fallback, True


# ======================================================================
# CLI group
# ======================================================================

@click.group()
@click.option("--mock", is_flag=True, default=False, help="Use simulated data instead of live MySQL connections.")
@click.pass_context
def cli(ctx, mock):
    """mysql-topo: MySQL Multi-Version Topology & Diagnostic Tool"""
    ctx.ensure_object(dict)
    ctx.obj["mock"] = mock
    db.init_db()


# ======================================================================
# import-config
# ======================================================================

@cli.command("import-config")
@click.argument("json_file", type=click.Path(exists=True))
@click.pass_context
def import_config(ctx, json_file):
    """Batch-import cluster metadata from a JSON file."""
    with open(json_file, "r") as f:
        data = json.load(f)
    db.import_config(data)
    count = len(data.get("clusters", []))
    console.print(f"[green]✓[/green] Imported {count} cluster(s) successfully.")


# ======================================================================
# list-cluster
# ======================================================================

@cli.command("list-cluster")
@click.pass_context
def list_cluster(ctx):
    """List all managed clusters."""
    clusters = db.list_clusters()
    if not clusters:
        console.print("[yellow]No clusters found.[/yellow] Use [bold]import-config[/bold] to add clusters.")
        return

    table = Table(title="Managed MySQL Clusters", show_lines=True)
    table.add_column("UUID", style="dim", no_wrap=True)
    table.add_column("Cluster Name", style="bold cyan")
    table.add_column("Nodes", justify="center")
    table.add_column("Description")

    for c in clusters:
        table.add_row(
            c["uuid"],
            f"MySQL-{c['name']}",
            str(c["node_count"]),
            c["description"],
        )
    console.print(table)


# ======================================================================
# show-cluster-info
# ======================================================================

@cli.command("show-cluster-info")
@click.argument("identifier")
@click.pass_context
def show_cluster_info(ctx, identifier):
    """Show topology tree and live status for a cluster (by UUID or name)."""
    use_mock = ctx.obj["mock"]
    cluster, nodes = db.get_cluster(identifier)
    if not cluster:
        console.print(f"[red]Cluster '{identifier}' not found.[/red]")
        sys.exit(1)

    # ---- Topology tree ----
    tree = Tree(f"[bold magenta]MySQL-{cluster['name']}[/bold magenta]  ({cluster['uuid'][:8]}…)")

    masters = [n for n in nodes if n["role"] == "master"]
    slaves = [n for n in nodes if n["role"] == "slave"]

    for m in masters:
        m_label = f"[bold green]★ Master[/bold green]  {m['host']}:{m['port']}"
        m_branch = tree.add(m_label)
        for s in slaves:
            if s.get("master_host") == m["host"]:
                m_branch.add(f"[cyan]↳ Slave[/cyan]  {s['host']}:{s['port']}")

    # Slaves not tied to a known master
    orphan_slaves = [s for s in slaves if s.get("master_host") not in [m["host"] for m in masters]]
    for s in orphan_slaves:
        tree.add(f"[cyan]↳ Slave[/cyan]  {s['host']}:{s['port']}  (master: {s.get('master_host', '?')})")

    console.print(tree)
    console.print()

    # ---- Live status table ----
    table = Table(title="Live Node Status", show_lines=True)
    table.add_column("Host", style="bold")
    table.add_column("Role")
    table.add_column("Version")
    table.add_column("Threads_conn", justify="right")
    table.add_column("Repl Lag (s)", justify="right")
    table.add_column("Semi-Sync", justify="center")
    table.add_column("Status", justify="center")

    for node in nodes:
        client = _client(node, use_mock)
        offline = False

        ver, off = _safe(client.get_version)
        offline = offline or off

        gs, off = _safe(lambda: client.get_global_status())
        offline = offline or off

        ss, off = _safe(lambda: client.get_slave_status())
        offline = offline or off

        semi, off = _safe(lambda: client.get_semi_sync_status(), {})
        offline = offline or off

        client.close()

        threads = gs.get("Threads_connected", "?") if isinstance(gs, dict) else "?"

        lag = "—"
        if node["role"] == "slave" and isinstance(ss, dict) and ss:
            raw_lag = ss.get("Seconds_Behind_Master")
            if raw_lag is None:
                lag = "[red]NULL[/red]"
            elif int(raw_lag) > 10:
                lag = f"[red]{raw_lag}[/red]"
            else:
                lag = str(raw_lag)

        # Semi-sync display
        semi_str = "—"
        if isinstance(semi, dict) and semi:
            for k, v in semi.items():
                if "enabled" in k:
                    semi_str = f"[green]ON[/green]" if v == "ON" else f"[red]OFF[/red]"
                    break

        status_str = "[red]● Offline[/red]" if offline else "[green]● Online[/green]"
        role_str = "[green]Master[/green]" if node["role"] == "master" else "[cyan]Slave[/cyan]"

        table.add_row(
            f"{node['host']}:{node['port']}",
            role_str,
            str(ver) if not offline else "?",
            str(threads),
            lag,
            semi_str,
            status_str,
        )

    console.print(table)


# ======================================================================
# show-node-detail
# ======================================================================

@cli.command("show-node-detail")
@click.argument("host")
@click.pass_context
def show_node_detail(ctx, host):
    """Show detailed diagnostics for a single node (by host/IP)."""
    use_mock = ctx.obj["mock"]
    node = db.get_node_by_host(host)
    if not node:
        console.print(f"[red]Node '{host}' not found in topology.db.[/red]")
        sys.exit(1)

    client = _client(node, use_mock)

    # ---- Version ----
    ver, offline = _safe(client.get_version)
    if offline:
        console.print(f"[red]● Node {host} is Offline[/red]")
        client.close()
        return

    console.print(Panel(f"[bold]{host}:{node['port']}[/bold]  —  {ver}  —  Role: {node['role']}", title="Node Detail"))

    # ---- Processlist summary ----
    ps, _ = _safe(client.get_processlist_summary, {})
    if isinstance(ps, dict) and ps:
        t1 = Table(title="Processlist Summary — by Command", show_lines=True)
        t1.add_column("Command", style="bold")
        t1.add_column("Count", justify="right")
        for cmd, cnt in sorted(ps.get("by_command", {}).items(), key=lambda x: -x[1]):
            t1.add_row(cmd, str(cnt))
        console.print(t1)

        t2 = Table(title="Processlist Summary — by User", show_lines=True)
        t2.add_column("User", style="bold")
        t2.add_column("Count", justify="right")
        for usr, cnt in sorted(ps.get("by_user", {}).items(), key=lambda x: -x[1]):
            t2.add_row(usr, str(cnt))
        console.print(t2)

    # ---- Deadlock diagnosis ----
    innodb_text, _ = _safe(client.get_innodb_status, "")
    if innodb_text:
        dl_ts = MySQLClient.parse_deadlock_timestamp(innodb_text)
        if dl_ts:
            console.print(f"\n[bold red]Latest Detected Deadlock:[/bold red]  {dl_ts}")
        else:
            console.print("\n[green]No deadlock detected in InnoDB status.[/green]")

    # ---- Lock summary ----
    locks, _ = _safe(client.get_lock_summary, {})
    if isinstance(locks, dict) and locks:
        rl = locks.get("row_locks", {})
        console.print(f"\n[bold]Row Locks:[/bold]  current_waits={rl.get('current_row_locks', 0)}  "
                      f"total_waits={rl.get('row_lock_waits', 0)}  avg_time={rl.get('row_lock_time_avg_ms', 0)}ms")
        mdl = locks.get("mdl_locks", [])
        if mdl:
            t3 = Table(title="Metadata Locks (MDL)", show_lines=True)
            t3.add_column("Schema")
            t3.add_column("Table")
            t3.add_column("Lock Type")
            t3.add_column("Status")
            t3.add_column("Thread ID", justify="right")
            for lk in mdl:
                status_style = "red" if lk["lock_status"] == "PENDING" else "green"
                t3.add_row(
                    lk["object_schema"], lk["object_name"],
                    lk["lock_type"],
                    f"[{status_style}]{lk['lock_status']}[/{status_style}]",
                    str(lk["owner_thread_id"]),
                )
            console.print(t3)
        else:
            console.print("[dim]No metadata locks.[/dim]")

    # ---- Databases ----
    dbs, _ = _safe(client.get_databases, [])
    if dbs:
        console.print(f"\n[bold]Databases ({len(dbs)}):[/bold]  {', '.join(dbs)}")

    client.close()


# ======================================================================
# cluster-check
# ======================================================================

@cli.command("cluster-check")
@click.argument("cluster_uuid")
@click.option("--output-dir", default=None, type=click.Path(),
              help="Directory for JSON report output.")
@click.pass_context
def cluster_check(ctx, cluster_uuid, output_dir):
    """Run health inspection suite on a cluster (by UUID or name).

    Executes Connection Count, Topology Scale, and Schema Scale checks
    against all nodes in the cluster. Supports MySQL 5.7, 8.0, and 8.4.
    """
    use_mock = ctx.obj["mock"]

    # Verify cluster exists
    cluster, nodes = db.get_cluster(cluster_uuid)
    if not cluster:
        console.print(f"[red]Cluster '{cluster_uuid}' not found.[/red]")
        sys.exit(1)

    console.print(Panel(
        f"[bold]MySQL-{cluster['name']}[/bold]  ({cluster['uuid']})\n"
        f"Nodes: {len(nodes)}  |  Mode: {'Mock' if use_mock else 'Live'}",
        title="Cluster Health Inspection",
        border_style="blue",
    ))

    try:
        results = run_inspection(cluster_uuid, use_mock=use_mock,
                                 output_dir=output_dir)
    except Exception as exc:
        console.print(f"[red]Inspection failed: {exc}[/red]")
        sys.exit(1)

    # ---- Summary table ----
    overall_healthy = True
    table = Table(title="Inspection Results", show_lines=True)
    table.add_column("Check", style="bold")
    table.add_column("Status", justify="center")
    table.add_column("Details")

    for name, result in results.items():
        status = result.get("status", "Unknown")
        if status != "Healthy":
            overall_healthy = False

        status_str = "[green]Healthy[/green]" if status == "Healthy" else "[red]Unhealthy[/red]"

        # Build a concise detail string per checker
        detail = _format_check_detail(name, result)
        table.add_row(name, status_str, detail)

    console.print(table)
    console.print()

    if overall_healthy:
        console.print("[bold green]Overall Status: Healthy[/bold green]")
    else:
        console.print("[bold red]Overall Status: Unhealthy[/bold red]")

    if output_dir:
        console.print(f"\n[dim]Report written to {output_dir}/[/dim]")


def _format_check_detail(name: str, result: dict) -> str:
    """Build a concise detail string for each checker result."""
    if "error" in result:
        return f"[red]{result['error']}[/red]"

    if name == "connection_count":
        nodes = result.get("nodes", [])
        parts = []
        for n in nodes:
            tc = n.get("threads_connected", "?")
            tag = "[green]ok[/green]" if n.get("healthy") else f"[red]{tc}>{n.get('threshold')}[/red]"
            parts.append(f"{n['host']}:{n['port']}={tc} {tag}")
        return "; ".join(parts) if parts else "—"

    if name == "topology_scale":
        sc = result.get("slave_count", "?")
        mx = result.get("max_slaves", "?")
        return f"slaves={sc} (max={mx})"

    if name == "schema_scale":
        ud = result.get("user_databases")
        it = result.get("innodb_tables")
        parts = []
        if isinstance(ud, dict):
            parts.append(f"user_dbs={ud['count']}/{ud['max_allowed']}")
        if isinstance(it, dict):
            parts.append(f"innodb_tables={it['count']}/{it['max_allowed']}")
        node = result.get("checked_node", {})
        if node:
            parts.append(f"on {node.get('host', '?')}:{node.get('port', '?')}")
        return "  ".join(parts) if parts else "—"

    # Generic fallback
    return str({k: v for k, v in result.items() if k != "status"})


# ======================================================================
# Entry point
# ======================================================================

def main():
    cli()


if __name__ == "__main__":
    main()
