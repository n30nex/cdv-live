from __future__ import annotations

import base64
import hashlib
import json
import time
import zlib
from dataclasses import dataclass

from Crypto.Cipher import AES
from google.protobuf.json_format import MessageToDict
from meshtastic.protobuf import admin_pb2, mesh_pb2, mqtt_pb2, paxcount_pb2, portnums_pb2, remote_hardware_pb2, storeforward_pb2, telemetry_pb2


@dataclass(frozen=True)
class DecodedPacket:
    record: dict
    details: dict | None


PORTNUM_NAMES = {v.number: v.name for v in portnums_pb2.PortNum.DESCRIPTOR.values}

PORTNUM_PROTO = {
    portnums_pb2.PortNum.POSITION_APP: mesh_pb2.Position,
    portnums_pb2.PortNum.ROUTING_APP: mesh_pb2.Routing,
    portnums_pb2.PortNum.WAYPOINT_APP: mesh_pb2.Waypoint,
    portnums_pb2.PortNum.NEIGHBORINFO_APP: mesh_pb2.NeighborInfo,
    portnums_pb2.PortNum.TELEMETRY_APP: telemetry_pb2.Telemetry,
    portnums_pb2.PortNum.ADMIN_APP: admin_pb2.AdminMessage,
    portnums_pb2.PortNum.REMOTE_HARDWARE_APP: remote_hardware_pb2.HardwareMessage,
    portnums_pb2.PortNum.PAXCOUNTER_APP: paxcount_pb2.Paxcount,
    portnums_pb2.PortNum.STORE_FORWARD_APP: storeforward_pb2.StoreAndForward,
    portnums_pb2.PortNum.MAP_REPORT_APP: mqtt_pb2.MapReport,
    portnums_pb2.PortNum.KEY_VERIFICATION_APP: mesh_pb2.KeyVerification,
    portnums_pb2.PortNum.TRACEROUTE_APP: mesh_pb2.RouteDiscovery,
}

DEFAULT_KEY_B64 = "1PG7OiApB1nwvP+rz05pAQ=="
DEFAULT_KEY = base64.b64decode(DEFAULT_KEY_B64)


def portnum_name(portnum: int | None) -> str:
    if portnum is None:
        return "UNKNOWN_APP"
    return PORTNUM_NAMES.get(portnum, f"UNKNOWN_{portnum}")


def derive_key_from_channel_name(channel_name: str | None, key: bytes) -> bytes:
    if not channel_name:
        return key
    hasher = hashlib.sha256()
    hasher.update(key)
    hasher.update(channel_name.encode("utf-8"))
    return hasher.digest()


def _normalize_psk(key: bytes) -> bytes | None:
    if len(key) == 1:
        return DEFAULT_KEY[:-1] + key
    if len(key) in (16, 24, 32):
        return key
    return None


def decrypt_payload(
    encrypted: bytes, key: bytes, packet_id: int | None, sender_id: int | None
) -> bytes | None:
    if not encrypted or packet_id is None or sender_id is None:
        return None
    try:
        packet_id_bytes = packet_id.to_bytes(8, byteorder="little", signed=False)
        sender_id_bytes = sender_id.to_bytes(8, byteorder="little", signed=False)
    except OverflowError:
        return None
    nonce = packet_id_bytes + sender_id_bytes
    counter = int.from_bytes(nonce, byteorder="big", signed=False)
    cipher = AES.new(key, AES.MODE_CTR, nonce=b"", initial_value=counter)
    return cipher.decrypt(encrypted)


def decode_envelope(payload: bytes) -> mqtt_pb2.ServiceEnvelope | None:
    env = mqtt_pb2.ServiceEnvelope()
    try:
        env.ParseFromString(payload)
    except Exception:
        return None
    return env


def _decode_proto(message_cls, payload: bytes) -> dict | None:
    if not payload:
        return None
    msg = message_cls()
    try:
        msg.ParseFromString(payload)
    except Exception:
        return None
    return MessageToDict(msg, preserving_proto_field_name=True)


def _decode_text(payload: bytes) -> tuple[str | None, dict | None]:
    if not payload:
        return None, None
    text = payload.decode("utf-8", errors="replace")
    return text, {"text": text}


def _decode_compressed(payload: bytes) -> tuple[str | None, dict | None]:
    if not payload:
        return None, None
    compressed = mesh_pb2.Compressed()
    try:
        compressed.ParseFromString(payload)
    except Exception:
        return None, None

    text = None
    details: dict[str, object] = {
        "portnum": portnum_name(compressed.portnum),
        "compressed_size": len(compressed.data),
    }
    try:
        inflated = zlib.decompress(compressed.data)
        text = inflated.decode("utf-8", errors="replace")
        details["text"] = text
    except Exception:
        details["data_b64"] = base64.b64encode(compressed.data).decode("ascii")
    return text, details


def decode_payload(portnum: int | None, payload: bytes) -> tuple[str | None, dict | None]:
    if portnum == portnums_pb2.PortNum.TEXT_MESSAGE_APP:
        return _decode_text(payload)
    if portnum == portnums_pb2.PortNum.TEXT_MESSAGE_COMPRESSED_APP:
        return _decode_compressed(payload)
    if portnum == portnums_pb2.PortNum.NODEINFO_APP:
        details = _decode_proto(mesh_pb2.User, payload)
        if details is None:
            details = _decode_proto(mesh_pb2.NodeInfo, payload)
        if details is None:
            return None, None
        return None, details

    message_cls = PORTNUM_PROTO.get(portnum)
    if message_cls:
        details = _decode_proto(message_cls, payload)
        if details is None:
            return None, None
        if portnum == portnums_pb2.PortNum.POSITION_APP:
            lat_i = details.get("latitude_i")
            lon_i = details.get("longitude_i")
            if isinstance(lat_i, (int, float)) and isinstance(lon_i, (int, float)):
                details["latitude"] = lat_i / 1e7
                details["longitude"] = lon_i / 1e7
        return None, details

    return None, None


def decode_packet(
    envelope: mqtt_pb2.ServiceEnvelope,
    keys_b64: list[str],
    channel_name: str | None = None,
) -> DecodedPacket | None:
    packet = envelope.packet
    if packet is None:
        return None

    data = None
    decode_status = "none"
    if packet.HasField("decoded"):
        data = packet.decoded
        decode_status = "decoded"
    elif packet.encrypted:
        resolved_channel = channel_name or envelope.channel_id or None
        for key_b64 in keys_b64:
            try:
                key = base64.b64decode(key_b64)
            except Exception:
                continue
            normalized = _normalize_psk(key)
            if normalized is None:
                continue
            candidates = [normalized]
            if resolved_channel:
                candidates.append(derive_key_from_channel_name(resolved_channel, normalized))
            for candidate_key in candidates:
                decrypted = decrypt_payload(
                    packet.encrypted,
                    candidate_key,
                    getattr(packet, "id", None),
                    getattr(packet, "from", None),
                )
                if not decrypted:
                    continue
                candidate = mesh_pb2.Data()
                try:
                    candidate.ParseFromString(decrypted)
                except Exception:
                    continue
                if candidate.portnum == portnums_pb2.PortNum.UNKNOWN_APP:
                    continue
                data = candidate
                decode_status = "decrypted"
                break
            if data is not None:
                break
        if data is None:
            decode_status = "decrypt_failed"
    else:
        decode_status = "no_payload"

    portnum = getattr(data, "portnum", None) if data else None
    payload = getattr(data, "payload", b"") if data else b""
    payload_b64 = base64.b64encode(payload).decode("ascii") if payload else None
    text, details = decode_payload(portnum, payload)
    details = details or {}
    details["decode_status"] = decode_status
    details["encrypted"] = bool(packet.encrypted)

    now = int(time.time())
    if portnum is None:
        if decode_status == "decrypt_failed":
            portname = "ENCRYPTED"
        elif decode_status == "no_payload":
            portname = "NO_PAYLOAD"
        else:
            portname = "UNKNOWN_APP"
    else:
        portname = portnum_name(portnum)
    record = {
        "rx_time": packet.rx_time,
        "from_id": getattr(packet, "from", None),
        "to_id": packet.to,
        "portnum": portnum,
        "portname": portname,
        "payload_b64": payload_b64,
        "text": text,
        "details_json": json.dumps(details) if details else None,
        "rssi": packet.rx_rssi,
        "snr": packet.rx_snr,
        "hop_limit": packet.hop_limit,
        "hop_start": packet.hop_start,
        "via_mqtt": packet.via_mqtt,
        "channel": packet.channel,
        "gateway_id": envelope.gateway_id or None,
        "created_at": now,
    }

    return DecodedPacket(record=record, details=details)
