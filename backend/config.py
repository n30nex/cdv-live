from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class AppConfig:
    mqtt_broker: str
    mqtt_port: int
    mqtt_user: str
    mqtt_pass: str
    mqtt_topic: str
    default_key_b64: str
    decode_keys_b64: list[str]
    db_path: Path


def _clean_value(value: str) -> str:
    value = value.strip()
    if value.startswith("\"") and value.endswith("\""):
        return value[1:-1]
    if value.startswith("'") and value.endswith("'"):
        return value[1:-1]
    return value


def load_config(path: Path, db_path: Path) -> AppConfig:
    raw: dict[str, str] = {}
    if not path.exists():
        raise FileNotFoundError(f"Missing config file: {path}")

    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        if "=" not in stripped:
            continue
        key, value = stripped.split("=", 1)
        raw[key.strip()] = _clean_value(value)

    mqtt_topic = raw.get("MQTT_TOPIC", "msh/#")
    if mqtt_topic.endswith("#/"):
        mqtt_topic = mqtt_topic[:-2] + "#"
    mqtt_topic = mqtt_topic.rstrip("/")

    default_key = raw.get("DEFAULT_KEY", "AQ==")
    raw_keys = raw.get("DECODE_KEYS", "")
    decode_keys = [item.strip() for item in raw_keys.split(",") if item.strip()]
    combined_keys = []
    seen = set()
    for key in [default_key, *decode_keys]:
        if key and key not in seen:
            combined_keys.append(key)
            seen.add(key)

    return AppConfig(
        mqtt_broker=raw.get("MQTT_BROKER", "localhost"),
        mqtt_port=int(raw.get("MQTT_PORT", "1883")),
        mqtt_user=raw.get("MQTT_USER", ""),
        mqtt_pass=raw.get("MQTT_PASS", ""),
        mqtt_topic=mqtt_topic,
        default_key_b64=default_key,
        decode_keys_b64=combined_keys,
        db_path=db_path,
    )
