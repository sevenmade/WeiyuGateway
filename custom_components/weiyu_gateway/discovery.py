"""UDP discovery for Weiyu gateways."""

from __future__ import annotations

import asyncio
import json
import socket

DISCOVERY_PORT = 50500
DISCOVERY_PAYLOAD = b"seekdevice\r\n"


async def async_discover_gateways(timeout: float = 2.0) -> list[dict[str, str]]:
    """Discover Weiyu gateways in LAN via UDP broadcast."""

    def _discover() -> list[dict[str, str]]:
        found: dict[str, dict[str, str]] = {}
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
            sock.settimeout(0.3)
            sock.bind(("", 0))
            sock.sendto(DISCOVERY_PAYLOAD, ("255.255.255.255", DISCOVERY_PORT))

            loop_count = max(1, int(timeout / 0.3))
            for _ in range(loop_count):
                try:
                    data, addr = sock.recvfrom(2048)
                except TimeoutError:
                    continue

                text = data.decode("utf-8", errors="ignore").strip()
                if not text:
                    continue

                try:
                    payload = json.loads(text)
                except json.JSONDecodeError:
                    continue

                ip = str(payload.get("ip") or addr[0])
                if not ip:
                    continue
                found[ip] = {
                    "ip": ip,
                    "id": str(payload.get("id", "")),
                    "type": str(payload.get("type", "")),
                    "version": str(payload.get("version", "")),
                }
        finally:
            sock.close()

        return list(found.values())

    return await asyncio.to_thread(_discover)
