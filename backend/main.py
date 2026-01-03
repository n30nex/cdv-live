from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import threading
import time
from pathlib import Path

import uvicorn
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.staticfiles import StaticFiles
from paho.mqtt.client import Client, CallbackAPIVersion

from .config import load_config
from .db import (
    connect,
    fetch_channels_summary,
    fetch_graph,
    fetch_metric_counts,
    fetch_node_packets,
    fetch_node_peers,
    fetch_node_ports,
    fetch_nodes,
    fetch_nodes_summary,
    fetch_packets,
    fetch_packets_filtered,
    fetch_ports_summary,
    insert_packet,
    touch_node,
    update_node,
)
from .decoder import decode_envelope, decode_packet, portnum_name
from meshtastic.protobuf import portnums_pb2


class NoCacheStaticFiles(StaticFiles):
    async def get_response(self, path: str, scope):
        response = await super().get_response(path, scope)
        response.headers["Cache-Control"] = "no-store"
        response.headers["Pragma"] = "no-cache"
        response.headers["Expires"] = "0"
        return response


def _node_label(node_id: int | None, node_info: dict[int, dict]) -> str:
    if node_id is None:
        return "unknown"
    if node_id == 0xFFFFFFFF:
        return "broadcast"
    info = node_info.get(node_id)
    if info:
        return info.get("short_name") or info.get("long_name") or f"!{node_id:08x}"
    return f"!{node_id:08x}"


ALLOWED_DECODE_STATUSES = {"decoded", "decrypted"}


def _parse_portnums(portnum: str | None) -> list[int] | None:
    if not portnum:
        return None
    values = []
    for item in portnum.split(","):
        item = item.strip()
        if not item:
            continue
        try:
            values.append(int(item))
        except ValueError:
            continue
    return values or None


def _packet_for_api(row: dict, node_info: dict[int, dict]) -> dict:
    details = None
    if row.get("details_json"):
        try:
            details = json.loads(row["details_json"])
        except json.JSONDecodeError:
            details = None
    packet = {
        **row,
        "from_label": _node_label(row.get("from_id"), node_info),
        "to_label": _node_label(row.get("to_id"), node_info),
        "details": details,
    }
    packet.pop("details_json", None)
    _decorate_route_details(packet, node_info)
    return packet


def _include_in_feed(packet: dict) -> bool:
    return _should_store(packet.get("details"))


def _should_store(details: dict | None) -> bool:
    if not isinstance(details, dict):
        return False
    return details.get("decode_status") in ALLOWED_DECODE_STATUSES


def _decorate_route_details(packet: dict, node_info: dict[int, dict]) -> None:
    details = packet.get("details")
    if not isinstance(details, dict):
        return

    def coerce_node_id(value: object) -> int | None:
        if isinstance(value, int):
            return value
        if isinstance(value, str):
            try:
                return int(value)
            except ValueError:
                return None
        return None

    def decorate_block(block: dict) -> list[str]:
        text_parts = []
        for key, label in (("route", "Route"), ("route_back", "Return")):
            route = block.get(key)
            if not isinstance(route, list) or not route:
                continue
            labels = []
            for value in route:
                node_id = coerce_node_id(value)
                labels.append(_node_label(node_id, node_info) if node_id is not None else str(value))
            block[f"{key}_labels"] = labels
            block[f"{key}_text"] = " -> ".join(labels)
            text_parts.append(f"{label}: {block[f'{key}_text']}")
        if text_parts and not block.get("text"):
            block["text"] = " | ".join(text_parts)
        return text_parts

    portnum = packet.get("portnum")
    if portnum == portnums_pb2.PortNum.TRACEROUTE_APP:
        decorate_block(details)
        return

    if portnum != portnums_pb2.PortNum.ROUTING_APP:
        return

    combined_parts = []
    for key, label in (("route_request", "Request"), ("route_reply", "Reply")):
        block = details.get(key)
        if not isinstance(block, dict):
            continue
        parts = decorate_block(block)
        combined_parts.extend([f"{label} {part}" for part in parts])

    error_reason = details.get("error_reason")
    if (
        not combined_parts
        and isinstance(error_reason, str)
        and error_reason
        and error_reason != "NONE"
        and not details.get("text")
    ):
        details["text"] = f"Routing error: {error_reason}"
    elif combined_parts and not details.get("text"):
        details["text"] = " | ".join(combined_parts)


def _packet_signature(record: dict) -> str:
    parts = [
        record.get("from_id"),
        record.get("to_id"),
        record.get("portnum"),
        record.get("rx_time"),
        record.get("channel"),
        record.get("payload_b64"),
        record.get("text"),
        record.get("gateway_id"),
    ]
    raw = "|".join("" if value is None else str(value) for value in parts)
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def _median(values: list[float]) -> float | None:
    if not values:
        return None
    sorted_values = sorted(values)
    mid = len(sorted_values) // 2
    if len(sorted_values) % 2 == 0:
        return (sorted_values[mid - 1] + sorted_values[mid]) / 2
    return sorted_values[mid]


def _parse_channel_from_topic(topic: str) -> str | None:
    parts = topic.split("/")
    if len(parts) < 5:
        return None
    candidate = parts[4]
    if not candidate or candidate.startswith("!"):
        return None
    return candidate


def _make_mqtt_client(app: FastAPI, config):
    keys_b64 = config.decode_keys_b64 or [config.default_key_b64]

    db_lock = app.state.db_lock
    conn = app.state.db
    queue = app.state.queue
    loop = app.state.loop
    node_cache = app.state.node_cache

    def on_message(client, userdata, msg):
        envelope = decode_envelope(msg.payload)
        if envelope is None:
            return

        channel_name = _parse_channel_from_topic(msg.topic)
        decoded = decode_packet(envelope, keys_b64, channel_name=channel_name)
        if decoded is None:
            return

        record = decoded.record
        details = decoded.details or {}
        if not _should_store(details):
            return

        with db_lock:
            now = time.time()
            signature = _packet_signature(record)
            last_seen = app.state.dedupe.get(signature)
            if last_seen and (now - last_seen) < app.state.dedupe_window:
                return
            app.state.dedupe[signature] = now
            if len(app.state.dedupe) > 5000:
                cutoff = now - app.state.dedupe_window
                app.state.dedupe = {
                    sig: ts for sig, ts in app.state.dedupe.items() if ts >= cutoff
                }

            touch_node(conn, record.get("from_id"))
            touch_node(conn, record.get("to_id"))

            if record.get("portnum") == portnums_pb2.PortNum.NODEINFO_APP:
                user = details.get("user") if isinstance(details, dict) else None
                if not isinstance(user, dict) and isinstance(details, dict):
                    user = details
                long_name = user.get("long_name") if isinstance(user, dict) else None
                short_name = user.get("short_name") if isinstance(user, dict) else None
                if record.get("from_id") is not None:
                    update_node(conn, record["from_id"], long_name, short_name)
                    cached = node_cache.get(record["from_id"], {})
                    node_cache[record["from_id"]] = {
                        **cached,
                        "long_name": long_name or cached.get("long_name"),
                        "short_name": short_name or cached.get("short_name"),
                    }

            packet_id = insert_packet(conn, record)

        event = {
            **record,
            "id": packet_id,
            "details": details,
            "from_label": _node_label(record.get("from_id"), node_cache),
            "to_label": _node_label(record.get("to_id"), node_cache),
        }
        _decorate_route_details(event, node_cache)

        def _put_safe(q, item):
            try:
                q.put_nowait(item)
            except asyncio.QueueFull:
                pass

        if _include_in_feed(event):
            loop.call_soon_threadsafe(_put_safe, queue, event)

    client = Client(
        client_id="meshviz-decoder",
        callback_api_version=CallbackAPIVersion.VERSION2,
    )
    client.on_message = on_message

    if config.mqtt_user:
        client.username_pw_set(config.mqtt_user, config.mqtt_pass)

    client.connect(config.mqtt_broker, config.mqtt_port, 60)
    client.subscribe(config.mqtt_topic)
    client.loop_start()
    return client


def create_app() -> FastAPI:
    app = FastAPI()
    app.state.clients = set()
    app.state.queue = asyncio.Queue(maxsize=1000)
    app.state.db_lock = threading.Lock()
    app.state.loop = None
    app.state.dedupe = {}
    app.state.dedupe_window = int(os.environ.get("DEDUPE_WINDOW", "6"))

    base_dir = Path(__file__).resolve().parent.parent
    config_path = Path(os.environ.get("CONFIG_PATH", base_dir / "config.txt"))
    db_path = Path(os.environ.get("DB_PATH", base_dir / "data" / "mesh.db"))

    config = load_config(config_path, db_path)
    app.state.config = config
    app.state.db = connect(config.db_path)
    with app.state.db_lock:
        app.state.node_cache = fetch_nodes(app.state.db)

    @app.on_event("startup")
    async def _startup():
        app.state.loop = asyncio.get_running_loop()
        app.state.mqtt = _make_mqtt_client(app, config)
        app.state.broadcast_task = asyncio.create_task(_broadcast_loop(app))

    @app.on_event("shutdown")
    async def _shutdown():
        app.state.broadcast_task.cancel()
        try:
            await app.state.broadcast_task
        except Exception:
            pass
        app.state.mqtt.loop_stop()
        app.state.mqtt.disconnect()

    @app.get("/api/health")
    async def health():
        return {
            "status": "ok",
            "broker": config.mqtt_broker,
            "topic": config.mqtt_topic,
        }

    @app.get("/api/packets")
    async def packets(
        limit: int = 200,
        window: int | None = None,
        portnum: str | None = None,
        channel: int | None = None,
        node: int | None = None,
        gateway: str | None = None,
    ):
        portnums = _parse_portnums(portnum)
        with app.state.db_lock:
            if window or portnums or channel is not None or node is not None or gateway:
                rows = fetch_packets_filtered(
                    app.state.db,
                    min(limit, 1000),
                    window_seconds=min(window or 0, 86400 * 7) if window else None,
                    portnums=portnums,
                    channel=channel,
                    node_id=node,
                    gateway_id=gateway,
                )
            else:
                rows = fetch_packets(app.state.db, min(limit, 1000))
            nodes = fetch_nodes(app.state.db)
        packets = [_packet_for_api(row, nodes) for row in rows]
        return [packet for packet in packets if _include_in_feed(packet)]

    @app.get("/api/graph")
    async def graph(
        window: int = 3600,
        portnum: str | None = None,
        channel: int | None = None,
        gateway: str | None = None,
    ):
        portnums = _parse_portnums(portnum)
        with app.state.db_lock:
            edges = fetch_graph(
                app.state.db,
                min(window, 86400 * 7),
                portnums=portnums,
                channel=channel,
                gateway_id=gateway,
            )
            node_info = fetch_nodes(app.state.db)

        nodes = {}
        links = []
        for edge in edges:
            source = edge.get("from_id")
            target = edge.get("to_id")
            if source is None or target is None:
                continue
            for node_id in (source, target):
                if node_id not in nodes:
                    nodes[node_id] = {
                        "id": node_id,
                        "label": _node_label(node_id, node_info),
                    }
            links.append(
                {
                    "source": source,
                    "target": target,
                    "portnum": edge.get("portnum"),
                    "portname": edge.get("portname") or portnum_name(edge.get("portnum")),
                    "count": edge.get("count"),
                    "last_seen": edge.get("last_seen"),
                }
            )

        return {
            "nodes": list(nodes.values()),
            "links": links,
        }

    @app.get("/api/nodes")
    async def nodes(
        window: int = 3600,
        portnum: str | None = None,
        channel: int | None = None,
        gateway: str | None = None,
    ):
        portnums = _parse_portnums(portnum)
        with app.state.db_lock:
            rows = fetch_nodes_summary(
                app.state.db,
                min(window, 86400 * 7),
                portnums=portnums,
                channel=channel,
                gateway_id=gateway,
            )
        return rows

    @app.get("/api/node/{node_id}")
    async def node_detail(
        node_id: int,
        window: int = 3600,
        limit: int = 50,
        portnum: str | None = None,
        channel: int | None = None,
        gateway: str | None = None,
    ):
        portnums = _parse_portnums(portnum)
        with app.state.db_lock:
            nodes = fetch_nodes(app.state.db)
            packets = fetch_node_packets(
                app.state.db,
                node_id,
                min(window, 86400 * 7),
                min(limit, 200),
                portnums=portnums,
                channel=channel,
                gateway_id=gateway,
            )
            ports = fetch_node_ports(
                app.state.db,
                node_id,
                min(window, 86400 * 7),
                portnums=portnums,
                channel=channel,
                gateway_id=gateway,
            )
            peers = fetch_node_peers(
                app.state.db,
                node_id,
                min(window, 86400 * 7),
                20,
                portnums=portnums,
                channel=channel,
                gateway_id=gateway,
            )
        node_info = nodes.get(node_id, {"node_id": node_id})
        return {
            "node": node_info,
            "packets": [_packet_for_api(row, nodes) for row in packets],
            "ports": ports,
            "peers": peers,
        }

    @app.get("/api/metrics")
    async def metrics(
        window: int = 3600,
        portnum: str | None = None,
        channel: int | None = None,
        gateway: str | None = None,
    ):
        portnums = _parse_portnums(portnum)
        window = min(window, 86400 * 7)
        with app.state.db_lock:
            data = fetch_metric_counts(
                app.state.db,
                window,
                portnums=portnums,
                channel=channel,
                gateway_id=gateway,
            )
        packets_per_min = data["total_packets"] / max(window / 60, 1)
        return {
            "packets_per_min": round(packets_per_min, 2),
            "active_nodes": data["active_nodes"],
            "top_ports": data["top_ports"],
            "median_rssi": _median([value for value in data["rssi_values"] if value is not None]),
            "median_snr": _median([value for value in data["snr_values"] if value is not None]),
        }

    @app.get("/api/ports")
    async def ports(
        window: int = 3600,
        channel: int | None = None,
        gateway: str | None = None,
    ):
        with app.state.db_lock:
            rows = fetch_ports_summary(
                app.state.db,
                min(window, 86400 * 7),
                channel=channel,
                gateway_id=gateway,
            )
        return rows

    @app.get("/api/channels")
    async def channels(
        window: int = 3600,
        portnum: str | None = None,
        gateway: str | None = None,
    ):
        portnums = _parse_portnums(portnum)
        with app.state.db_lock:
            rows = fetch_channels_summary(
                app.state.db,
                min(window, 86400 * 7),
                portnums=portnums,
                gateway_id=gateway,
            )
        return rows

    @app.websocket("/ws")
    async def ws(websocket: WebSocket):
        await websocket.accept()
        app.state.clients.add(websocket)
        try:
            while True:
                await websocket.receive_text()
        except WebSocketDisconnect:
            app.state.clients.discard(websocket)

    web_dir = base_dir / "web"
    app.mount("/", NoCacheStaticFiles(directory=web_dir, html=True), name="static")

    return app


async def _broadcast_loop(app: FastAPI) -> None:
    while True:
        event = await app.state.queue.get()
        if not app.state.clients:
            continue
        payload = json.dumps(event)
        
        clients = list(app.state.clients)
        
        async def _send(ws: WebSocket):
            try:
                await asyncio.wait_for(ws.send_text(payload), timeout=2.0)
                return ws, None
            except Exception as e:
                return ws, e

        results = await asyncio.gather(*[_send(ws) for ws in clients])
        
        for ws, error in results:
            if error is not None:
                app.state.clients.discard(ws)


app = create_app()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    uvicorn.run(app, host="0.0.0.0", port=8000)
