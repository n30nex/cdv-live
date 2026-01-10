from __future__ import annotations

import sqlite3
import time
from pathlib import Path


SCHEMA = """
PRAGMA journal_mode = WAL;

CREATE TABLE IF NOT EXISTS packets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rx_time INTEGER,
    from_id INTEGER,
    to_id INTEGER,
    portnum INTEGER,
    portname TEXT,
    payload_b64 TEXT,
    text TEXT,
    details_json TEXT,
    rssi INTEGER,
    snr REAL,
    hop_limit INTEGER,
    hop_start INTEGER,
    via_mqtt INTEGER,
    channel INTEGER,
    gateway_id TEXT,
    created_at INTEGER
);

CREATE TABLE IF NOT EXISTS nodes (
    node_id INTEGER PRIMARY KEY,
    long_name TEXT,
    short_name TEXT,
    last_seen INTEGER
);

CREATE INDEX IF NOT EXISTS idx_packets_time ON packets (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_packets_port ON packets (portnum);
CREATE INDEX IF NOT EXISTS idx_packets_from_to ON packets (from_id, to_id);
"""

BROADCAST_ID = 0xFFFFFFFF


def _build_packet_conditions(
    table_alias: str | None,
    window_seconds: int | None,
    portnums: list[int] | None,
    channel: int | None,
    gateway_id: str | None,
) -> tuple[list[str], list[object]]:
    prefix = f"{table_alias}." if table_alias else ""
    conditions: list[str] = []
    params: list[object] = []
    if window_seconds is not None:
        cutoff = int(time.time()) - window_seconds
        conditions.append(f"{prefix}created_at >= ?")
        params.append(cutoff)
    if portnums:
        placeholders = ",".join("?" for _ in portnums)
        conditions.append(f"{prefix}portnum IN ({placeholders})")
        params.extend(portnums)
    if channel is not None:
        conditions.append(f"{prefix}channel = ?")
        params.append(channel)
    if gateway_id:
        conditions.append(f"{prefix}gateway_id = ?")
        params.append(gateway_id)
    return conditions, params


def _where_clause(conditions: list[str]) -> str:
    if not conditions:
        return ""
    return f"WHERE {' AND '.join(conditions)}"


def connect(db_path: Path) -> sqlite3.Connection:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.executescript(SCHEMA)
    return conn


def connect_read(db_path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(db_path), check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA busy_timeout = 5000")
    conn.execute("PRAGMA query_only = TRUE")
    return conn


def insert_packet(conn: sqlite3.Connection, packet: dict) -> int:
    payload = (
        packet.get("rx_time"),
        packet.get("from_id"),
        packet.get("to_id"),
        packet.get("portnum"),
        packet.get("portname"),
        packet.get("payload_b64"),
        packet.get("text"),
        packet.get("details_json"),
        packet.get("rssi"),
        packet.get("snr"),
        packet.get("hop_limit"),
        packet.get("hop_start"),
        int(packet.get("via_mqtt")) if packet.get("via_mqtt") is not None else None,
        packet.get("channel"),
        packet.get("gateway_id"),
        packet.get("created_at"),
    )
    cursor = conn.execute(
        """
        INSERT INTO packets (
            rx_time, from_id, to_id, portnum, portname, payload_b64, text, details_json,
            rssi, snr, hop_limit, hop_start, via_mqtt, channel, gateway_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        payload,
    )
    return cursor.lastrowid


def touch_node(conn: sqlite3.Connection, node_id: int | None) -> None:
    if node_id is None:
        return
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO nodes (node_id, last_seen)
        VALUES (?, ?)
        ON CONFLICT(node_id) DO UPDATE SET last_seen = excluded.last_seen
        """,
        (node_id, now),
    )


def update_node(conn: sqlite3.Connection, node_id: int, long_name: str | None, short_name: str | None) -> None:
    now = int(time.time())
    conn.execute(
        """
        INSERT INTO nodes (node_id, long_name, short_name, last_seen)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(node_id) DO UPDATE SET
            long_name = COALESCE(excluded.long_name, nodes.long_name),
            short_name = COALESCE(excluded.short_name, nodes.short_name),
            last_seen = excluded.last_seen
        """,
        (node_id, long_name, short_name, now),
    )


def fetch_packets(conn: sqlite3.Connection, limit: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT * FROM packets
        ORDER BY created_at DESC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_packets_filtered(
    conn: sqlite3.Connection,
    limit: int,
    window_seconds: int | None = None,
    portnums: list[int] | None = None,
    channel: int | None = None,
    node_id: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    if node_id is not None:
        conditions.append("(from_id = ? OR to_id = ?)")
        params.extend([node_id, node_id])
    where = _where_clause(conditions)
    params.append(limit)
    rows = conn.execute(
        f"""
        SELECT * FROM packets
        {where}
        ORDER BY created_at DESC
        LIMIT ?
        """,
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_nodes(conn: sqlite3.Connection) -> dict[int, dict]:
    rows = conn.execute("SELECT * FROM nodes").fetchall()
    return {row["node_id"]: dict(row) for row in rows}


def fetch_graph(
    conn: sqlite3.Connection,
    window_seconds: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    conditions.extend(["from_id IS NOT NULL", "to_id IS NOT NULL"])
    where = _where_clause(conditions)
    rows = conn.execute(
        """
        SELECT from_id, to_id, portnum, portname, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM packets
        {where}
        GROUP BY from_id, to_id, portnum
        """.format(where=where),
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_nodes_summary(
    conn: sqlite3.Connection,
    window_seconds: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        "p", window_seconds, portnums, channel, gateway_id
    )
    join_conditions = ["(p.from_id = n.node_id OR p.to_id = n.node_id)"] + conditions
    rows = conn.execute(
        """
        SELECT
            n.node_id,
            n.long_name,
            n.short_name,
            n.last_seen,
            COUNT(p.id) AS packet_count,
            AVG(p.rssi) AS avg_rssi,
            AVG(p.snr) AS avg_snr,
            MAX(p.created_at) AS last_packet
        FROM nodes n
        JOIN packets p
            ON {join_on}
        GROUP BY n.node_id
        ORDER BY packet_count DESC
        """.format(join_on=" AND ".join(join_conditions)),
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_node_packets(
    conn: sqlite3.Connection,
    node_id: int,
    window_seconds: int,
    limit: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    conditions.append("(from_id = ? OR to_id = ?)")
    params.extend([node_id, node_id])
    where = _where_clause(conditions)
    params.append(limit)
    rows = conn.execute(
        """
        SELECT * FROM packets
        {where}
        ORDER BY created_at DESC
        LIMIT ?
        """.format(where=where),
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_node_ports(
    conn: sqlite3.Connection,
    node_id: int,
    window_seconds: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    conditions.append("(from_id = ? OR to_id = ?)")
    params.extend([node_id, node_id])
    where = _where_clause(conditions)
    rows = conn.execute(
        """
        SELECT portnum, portname, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM packets
        {where}
        GROUP BY portnum, portname
        ORDER BY count DESC
        """.format(where=where),
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_node_peers(
    conn: sqlite3.Connection,
    node_id: int,
    window_seconds: int,
    limit: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    conditions.append("(from_id = ? OR to_id = ?)")
    params.extend([node_id, node_id])
    where = _where_clause(conditions)
    params.append(limit)
    rows = conn.execute(
        """
        SELECT
            CASE
                WHEN from_id = ? THEN to_id
                ELSE from_id
            END AS peer_id,
            COUNT(*) AS count,
            MAX(created_at) AS last_seen
        FROM packets
        {where}
        GROUP BY peer_id
        ORDER BY count DESC
        LIMIT ?
        """.format(where=where),
        [node_id, *params],
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_ports_summary(
    conn: sqlite3.Connection,
    window_seconds: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    where = _where_clause(conditions)
    rows = conn.execute(
        """
        SELECT portnum, portname, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM packets
        {where}
        GROUP BY portnum, portname
        ORDER BY count DESC
        """.format(where=where),
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_channels_summary(
    conn: sqlite3.Connection,
    window_seconds: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> list[dict]:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    where = _where_clause(conditions)
    rows = conn.execute(
        """
        SELECT channel, COUNT(*) AS count, MAX(created_at) AS last_seen
        FROM packets
        {where}
        GROUP BY channel
        ORDER BY count DESC
        """.format(where=where),
        params,
    ).fetchall()
    return [dict(row) for row in rows]


def fetch_metric_counts(
    conn: sqlite3.Connection,
    window_seconds: int,
    portnums: list[int] | None = None,
    channel: int | None = None,
    gateway_id: str | None = None,
) -> dict:
    conditions, params = _build_packet_conditions(
        None, window_seconds, portnums, channel, gateway_id
    )
    where = _where_clause(conditions)
    total = conn.execute(
        f"SELECT COUNT(*) FROM packets {where}",
        params,
    ).fetchone()[0]
    from_conditions = conditions + ["from_id IS NOT NULL", "from_id != ?"]
    to_conditions = conditions + ["to_id IS NOT NULL", "to_id != ?"]
    from_where = _where_clause(from_conditions)
    to_where = _where_clause(to_conditions)
    active_nodes = conn.execute(
        f"""
        SELECT COUNT(DISTINCT node_id)
        FROM (
            SELECT from_id AS node_id FROM packets {from_where}
            UNION
            SELECT to_id AS node_id FROM packets {to_where}
        )
        """,
        [*params, BROADCAST_ID, *params, BROADCAST_ID],
    ).fetchone()[0]
    top_ports = conn.execute(
        """
        SELECT portnum, portname, COUNT(*) AS count
        FROM packets
        {where}
        GROUP BY portnum, portname
        ORDER BY count DESC
        LIMIT 5
        """.format(where=where),
        params,
    ).fetchall()
    rssi_rows = conn.execute(
        """
        SELECT rssi FROM packets
        {where}
        ORDER BY rssi
        LIMIT 5000
        """.format(where=_where_clause(conditions + ["rssi IS NOT NULL"])),
        params,
    ).fetchall()
    snr_rows = conn.execute(
        """
        SELECT snr FROM packets
        {where}
        ORDER BY snr
        LIMIT 5000
        """.format(where=_where_clause(conditions + ["snr IS NOT NULL"])),
        params,
    ).fetchall()

    return {
        "total_packets": total,
        "active_nodes": active_nodes,
        "top_ports": [dict(row) for row in top_ports],
        "rssi_values": [row[0] for row in rssi_rows],
        "snr_values": [row[0] for row in snr_rows],
    }
