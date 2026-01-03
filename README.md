# Meshtastic MQTT Traffic Visualizer

## Quick Start

1. Edit `config.txt` with your broker, topic, and default key.
2. Start the stack:
   ```bash
   docker compose up --build
   ```
3. Open the UI at `http://localhost:8000`.
   - TopoMap view: `http://localhost:8000/topomap.html`
4. Stop the stack:
   ```bash
   docker compose down
   ```

## Reset Database

1. Stop the stack:
   ```bash
   docker compose down
   ```
2. Remove the SQLite files in `data`:
   ```powershell
   Remove-Item -Force data/mesh.db*
   ```
3. Start the stack again:
   ```bash
   docker compose up --build
   ```

## Repository Notes

- `config.txt` is tracked and intended to be public.
- Local-only notes (`agents.md`, `projectmemories.md`) are ignored by git.

## Notes
- SQLite data is persisted in `./data/mesh.db`.
- The backend reads `config.txt` on startup; restart the container to apply changes.
- WebSocket live updates are pushed to the UI as packets arrive.
- For non-primary channel traffic, add `DECODE_KEYS` (comma-separated base64 keys) in `config.txt` to enable decryption.
