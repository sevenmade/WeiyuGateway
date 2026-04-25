"""Low-level gateway client for Weiyu protocol."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import socket
from collections.abc import Callable
from collections import deque
from time import monotonic

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)

# Per W3 TCP doc: JSON body length is bounded; oversize almost always means desync.
_MAX_FRAME_PAYLOAD_BYTES = 256 * 1024
# If no 0xFA appears, retain tail to survive chunk splits and avoid unbounded growth.
_MAX_BUFFER_NO_SYNC = 65536
_BUFFER_TRIM_KEEP = 8192


class WeiyuGatewayClient:
    """Manage Weiyu gateway discovery registration and TCP session.

    Data path is **push-only**: HA does not periodically ``report/subdev`` poll; child
    state and telemetry follow gateway ``subclass`` uploads. Missing uploads within a
    cycle leave prior HA entity state unchanged until the next frame.
    """

    def __init__(
        self,
        hass: HomeAssistant,
        gateway_host: str,
        gateway_model: str,
        listen_ip: str,
        listen_port: int,
    ) -> None:
        self.hass = hass
        self.gateway_host = gateway_host
        self.listen_ip = listen_ip
        self.listen_port = listen_port

        self._server: asyncio.AbstractServer | None = None
        self._gateway_reader: asyncio.StreamReader | None = None
        self._gateway_writer: asyncio.StreamWriter | None = None
        self._read_task: asyncio.Task | None = None
        self._poll_task: asyncio.Task | None = None
        self._register_task: asyncio.Task | None = None
        self._link_probe_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()
        self._re_register_lock = asyncio.Lock()
        self._io_generation: int = 0

        self.devices: dict[str, dict] = {}
        self._listeners: list[Callable[[set[str]], None]] = []
        self.gateway_info: dict[str, str | int] = {
            "host": gateway_host,
            "model": gateway_model or "Unknown",
            "version": "",
            "devno": "",
            "connected": 0,
        }
        self._alarm_cache: dict[str, bool] = {}
        self._target_report_cycle_seconds: int = 20
        self._pending_subdev_requests: deque[float] = deque(maxlen=256)
        self._gateway_activity_text: str = "未连接"
        self._last_gateway_payload_monotonic: float = monotonic()
        self._connected_since_monotonic: float | None = None
        self._last_register_ok_monotonic: float = 0.0
        self._last_heartbeat_monotonic: float = monotonic()
        self._skip_next_post_connect_scan: bool = False

    def set_gateway_activity(self, text: str) -> None:
        """Human-readable gateway task state for UI (not used for heartbeat send/recv text)."""
        if self._gateway_activity_text == text:
            return
        self._gateway_activity_text = text
        self._notify_listeners(set())

    def get_gateway_activity_text(self) -> str:
        """Return current gateway activity description."""
        return self._gateway_activity_text

    async def async_start(self) -> None:
        """Start TCP listener and register this HA endpoint to gateway."""
        self._server = await asyncio.start_server(
            self._handle_gateway_connected,
            host=self.listen_ip,
            port=self.listen_port,
        )
        _LOGGER.info("Weiyu server listening on %s:%s", self.listen_ip, self.listen_port)
        self.set_gateway_activity("注册中")
        try:
            await self._register_service_ip()
        except Exception:
            if self._server:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
            raise
        self.set_gateway_activity("待连接")
        if self._register_task and not self._register_task.done():
            self._register_task.cancel()
        self._register_task = self.hass.async_create_task(self._register_keepalive_loop())
        if self._link_probe_task and not self._link_probe_task.done():
            self._link_probe_task.cancel()
        self._link_probe_task = self.hass.async_create_task(self._link_probe_loop())

    async def async_stop(self) -> None:
        """Stop the client cleanly."""
        self.set_gateway_activity("已停止")
        if self._read_task:
            self._read_task.cancel()
        if self._poll_task:
            self._poll_task.cancel()
        if self._register_task:
            self._register_task.cancel()
        if self._link_probe_task:
            self._link_probe_task.cancel()
        if self._gateway_writer:
            self._gateway_writer.close()
            try:
                await self._gateway_writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
        if self._server:
            self._server.close()
            await self._server.wait_closed()

    def add_listener(self, listener: Callable[[set[str]], None]) -> Callable[[], None]:
        """Subscribe to device updates."""
        self._listeners.append(listener)

        def _unsub() -> None:
            self._listeners.remove(listener)

        return _unsub

    def get_device_name(self, devno: str) -> str:
        """Build a readable name from devno."""
        data = self.devices.get(devno, {})
        device_type = data.get("device_type", "断路器")
        suffix = devno[-6:] if len(devno) > 6 else devno
        return f"{device_type} {suffix}"

    def get_device_state(self, devno: str) -> bool:
        """Get switch state."""
        return bool(self.devices.get(devno, {}).get("state", 0))

    def get_device_data(self, devno: str) -> dict:
        """Get full cached device data."""
        return self.devices.get(devno, {})

    def get_all_devices_state(self) -> bool:
        """Return True only when all known child devices are on."""
        if not self.devices:
            return False
        return all(bool(device.get("state", 0)) for device in self.devices.values())

    def get_gateway_name(self) -> str:
        """Get display name for gateway device."""
        model = str(self.gateway_info.get("model") or "Unknown")
        return f"Weiyu网关 {model}"

    def get_gateway_identifiers(self) -> set[tuple[str, str]]:
        """Get HA device identifiers for gateway."""
        return {("weiyu_gateway", f"gateway_{self.gateway_host}")}

    def is_leakage_protection_device(self, devno: str) -> bool:
        """Return True if this child device supports leakage self-test."""
        data = self.devices.get(devno, {})
        devtag = str(data.get("meta", {}).get("devtag") or "")
        return "ldL" in devtag

    def is_two_p_device(self, devno: str) -> bool:
        """Return True if this child device is 2P and should expose N-line temperature."""
        data = self.devices.get(devno, {})
        raw = data.get("raw", {})
        if isinstance(raw, dict) and raw.get("ntemp") is not None:
            return True
        meta = data.get("meta", {})
        devtag = str(meta.get("devtag") or "").upper()
        category = str(meta.get("category") or "").upper()
        return "2P" in devtag or "2P" in category

    def _apply_report_cycle_from_gateway(self, seconds: int) -> None:
        """Apply report interval from gateway for silence-timeout tuning."""
        seconds = max(5, min(int(seconds), 86400))
        if self._target_report_cycle_seconds == seconds:
            return
        self._target_report_cycle_seconds = seconds
        self._notify_listeners(set())

    async def async_sync_gateway_time(self) -> None:
        """Sync gateway clock to HA current unix time."""
        ts = int(dt_util.utcnow().timestamp())
        payload = {
            "actionType": "service",
            "actionTarget": "time",
            "data": base64.b64encode(json.dumps({"time": ts}).encode("utf-8")).decode("utf-8"),
        }
        await self._async_send_packet(payload)

    async def _sync_gateway_time_after_connect(self) -> None:
        """Best-effort one-shot time sync each time gateway reconnects."""
        try:
            await asyncio.sleep(0.2)
            await self.async_sync_gateway_time()
            _LOGGER.debug("Gateway time sync submitted after reconnect")
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            _LOGGER.debug("Gateway time sync failed after reconnect: %s", exc)

    async def async_set_device_state(self, devno: str, state: bool) -> None:
        """Send on/off command to a child breaker."""
        payload_obj = {
            "actionType": "excuteList",
            "actionTarget": "subdev",
            "data": base64.b64encode(
                json.dumps({"list": [{"devno": devno, "state": 1 if state else 0}]}, ensure_ascii=False).encode(
                    "utf-8"
                )
            ).decode(),
        }
        await self._async_send_packet(payload_obj)

    async def async_set_all_devices_state(self, state: bool) -> tuple[int, int]:
        """Batch set all child devices to given switch state."""
        success = 0
        fail = 0
        for devno in list(self.devices):
            try:
                await self.async_set_device_state(devno, state)
                success += 1
            except Exception as exc:
                _LOGGER.warning("Batch set state failed for %s: %s", devno, exc)
                fail += 1
        return success, fail

    async def async_trigger_leakage_test(self, devno: str) -> None:
        """Trigger one-shot leakage self-test for leakage-protection breaker."""
        payload_obj = {
            "actionType": "excuteList",
            "actionTarget": "subdev",
            "data": base64.b64encode(
                json.dumps({"list": [{"devno": devno, "check": 1}]}, ensure_ascii=False).encode("utf-8")
            ).decode(),
        }
        await self._async_send_packet(payload_obj)

    async def async_request_subdevices(self) -> None:
        """Request ``report/subdev`` (optional; integration normally uses gateway push only)."""
        try:
            await self._async_send_packet({"actionType": "report", "actionTarget": "subdev"})
            self._pending_subdev_requests.append(monotonic())
            self._prune_pending_subdev_requests()
        except Exception:
            raise

    async def _register_service_ip(self, udp_timeout_s: float = 5.0, max_attempts: int = 3) -> None:
        """Register HA service address to gateway via UDP 50500 only."""
        register_payload = (
            json.dumps(
                {
                    "serviceIp": self.listen_ip,
                    "port": self.listen_port,
                    "role": "service",
                },
                ensure_ascii=False,
            )
            + "\r\n"
        )
        payload_bytes = register_payload.encode("utf-8")

        udp_last_exc: Exception | None = None
        udp_last_text = ""
        for attempt in range(1, max(1, int(max_attempts)) + 1):
            try:
                udp_text = await asyncio.to_thread(self._register_service_ip_udp, payload_bytes, udp_timeout_s)
                udp_last_text = udp_text
                _LOGGER.debug("Weiyu register UDP response (attempt %s): %s", attempt, udp_text)
                if "Set OK" in udp_text:
                    self._last_register_ok_monotonic = monotonic()
                    return
                await asyncio.sleep(0.35 * attempt)
            except Exception as exc:
                udp_last_exc = exc
                _LOGGER.debug("UDP register attempt %s failed: %s", attempt, exc)
                await asyncio.sleep(0.35 * attempt)

        raise RuntimeError(
            "Gateway register failed via UDP. "
            f"UDP error: {udp_last_exc!r}; UDP response: {udp_last_text or '<empty>'}"
        )

    def _register_service_ip_udp(self, payload_bytes: bytes, timeout_s: float) -> str:
        """Register service endpoint over UDP 50500."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.settimeout(max(0.5, float(timeout_s)))
            sock.sendto(payload_bytes, (self.gateway_host, 50500))
            resp, _ = sock.recvfrom(256)
            return resp.decode("utf-8", errors="ignore").strip()
        finally:
            sock.close()

    async def _handle_gateway_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle reverse TCP connection from gateway."""
        async with self._lock:
            self._io_generation += 1
            session_gen = self._io_generation
            if self._read_task and not self._read_task.done():
                self._read_task.cancel()
                try:
                    await self._read_task
                except asyncio.CancelledError:
                    pass
                except Exception:
                    pass
                self._read_task = None
            if self._gateway_writer:
                self._gateway_writer.close()
                try:
                    await self._gateway_writer.wait_closed()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
            self._gateway_reader = reader
            self._gateway_writer = writer
            self.gateway_info["connected"] = 1
            self._last_gateway_payload_monotonic = monotonic()
            self._connected_since_monotonic = monotonic()
            self._last_heartbeat_monotonic = monotonic()

        _LOGGER.info("Weiyu gateway connected from %s", writer.get_extra_info("peername"))
        self.set_gateway_activity("已连接")
        self._enable_tcp_keepalive(writer)
        self._pending_subdev_requests.clear()
        self._notify_listeners(set())
        await self._async_send_packet({"actionType": "report", "actionTarget": "subver"})
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
        self._poll_task = None
        # Reader must run before post-connect scan so subdev/subclass replies are not missed.
        self._read_task = self.hass.async_create_task(self._read_loop(session_gen))
        self.hass.async_create_task(self._sync_gateway_time_after_connect())
        if self._skip_next_post_connect_scan:
            self._skip_next_post_connect_scan = False
            self.set_gateway_activity("运行中")
            self._notify_listeners(set())
        else:
            self.hass.async_create_task(self._post_connect_scan_subdevices())

    async def _read_loop(self, session_gen: int) -> None:
        """Read and parse framed protocol packets."""
        reader = self._gateway_reader
        if reader is None:
            return

        buffer = bytearray()
        try:
            while True:
                chunk = await reader.read(4096)
                if not chunk:
                    break
                buffer.extend(chunk)
                for payload in self._extract_packets(buffer):
                    await self._handle_payload(payload)
        except asyncio.CancelledError:
            return
        finally:
            pass

        if session_gen != self._io_generation:
            _LOGGER.debug("Weiyu read loop ended for superseded TCP session (gen %s)", session_gen)
            return

        _LOGGER.debug("Gateway EOF received, immediately fast UDP re-register (up to 8 attempts)")
        for attempt in range(1, 9):
            if session_gen != self._io_generation:
                _LOGGER.debug("EOF fast re-register closed by new TCP session (gen switched)")
                return
            try:
                # EOF path uses single-shot short-timeout retries to avoid stacked wait.
                await self._register_service_ip(udp_timeout_s=1.2, max_attempts=1)
                self._skip_next_post_connect_scan = True
                self.set_gateway_activity("运行中")
                _LOGGER.debug("EOF fast UDP re-register attempt %s succeeded", attempt)
                return
            except Exception as exc:
                _LOGGER.debug("EOF fast UDP re-register attempt %s failed: %s", attempt, exc)
                await asyncio.sleep(0.2)

        connected_for = None
        if self._connected_since_monotonic is not None:
            connected_for = monotonic() - self._connected_since_monotonic
        if connected_for is None:
            _LOGGER.warning("Gateway disconnected")
        else:
            _LOGGER.warning("Gateway disconnected (session lasted %.1fs)", connected_for)
        self.gateway_info["connected"] = 0
        self._connected_since_monotonic = None
        self.set_gateway_activity("已断开")
        self._gateway_writer = None
        self._gateway_reader = None
        self._notify_listeners(set())
        if session_gen != self._io_generation:
            return
        self.hass.async_create_task(self._try_re_register())

    def _extract_packets(self, buffer: bytearray) -> list[dict]:
        """Extract as many valid packets as possible from stream buffer."""
        out: list[dict] = []
        while True:
            start = buffer.find(b"\xFA")
            if start < 0:
                if len(buffer) > _MAX_BUFFER_NO_SYNC:
                    _LOGGER.debug(
                        "Weiyu stream: no 0xFA in %s bytes (possible noise); keeping last %s bytes",
                        len(buffer),
                        _BUFFER_TRIM_KEEP,
                    )
                    del buffer[:-(_BUFFER_TRIM_KEEP)]
                break
            if start > 0:
                del buffer[:start]
            if len(buffer) < 7:
                break

            msg_len = int.from_bytes(buffer[1:5], byteorder="big")
            total_len = 1 + 4 + msg_len + 1 + 1
            if msg_len < 0 or msg_len > _MAX_FRAME_PAYLOAD_BYTES:
                _LOGGER.debug("Weiyu frame length invalid (%s), resync 1 byte", msg_len)
                del buffer[0:1]
                continue
            if len(buffer) < total_len:
                break

            if buffer[total_len - 1] != 0xFB:
                _LOGGER.debug("Weiyu frame footer not 0xFB, resync 1 byte")
                del buffer[0:1]
                continue

            payload_bytes = bytes(buffer[5 : 5 + msg_len])
            check = buffer[5 + msg_len]
            if (sum(payload_bytes) & 0xFF) != check:
                _LOGGER.debug("Weiyu frame checksum mismatch, resync 1 byte")
                del buffer[0:1]
                continue

            try:
                obj = json.loads(payload_bytes.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError):
                _LOGGER.debug("Weiyu frame JSON decode failed (framing was valid), discarding frame")
                del buffer[:total_len]
                continue

            del buffer[:total_len]
            if isinstance(obj, dict):
                out.append(obj)
            else:
                _LOGGER.debug("Weiyu JSON root is not object: %s", type(obj).__name__)
        return out

    @staticmethod
    def _find_heartbeat_pair(payload: dict) -> tuple[str | None, object]:
        """Find heartbeat key/value per W3 doc (Heartbeat vs heatbeat typo)."""
        for key, val in payload.items():
            if not isinstance(key, str):
                continue
            lowered = key.lower()
            if lowered in ("heartbeat", "heatbeat"):
                return key, val
        return None, None

    async def _handle_payload(self, payload: dict) -> None:
        """Handle decoded protocol payload."""
        if not isinstance(payload, dict):
            return
        self._last_gateway_payload_monotonic = monotonic()

        hb_key, hb_val = self._find_heartbeat_pair(payload)
        if hb_key is not None:
            self._last_heartbeat_monotonic = monotonic()
            if isinstance(hb_val, str) and hb_val.strip():
                if not self.gateway_info.get("model") or self.gateway_info.get("model") == "Unknown":
                    self.gateway_info["model"] = hb_val.strip()
            # W3 doc: echo same key as gateway; TCP must include connected (1=链路正常).
            try:
                await self._async_send_packet({hb_key: hb_val, "connected": 1})
            except Exception as exc:
                _LOGGER.warning("Heartbeat reply failed: %s", exc)
            self._notify_listeners(set())
            rest = {k: v for k, v in payload.items() if k != hb_key}
            if not rest:
                return
            payload = rest

        action_type = payload.get("actionType") or payload.get("actiontype")
        if isinstance(action_type, str):
            action_type = action_type.lower()

        if action_type == "version" and "data" in payload:
            self._update_gateway_version(payload["data"])
            return
        if action_type == "subclass" and "data" in payload:
            self._update_devices_from_subclass(payload["data"])
            return

        if action_type:
            _LOGGER.debug("Unhandled actionType payload: %s keys=%s", action_type, sorted(payload.keys()))
        elif payload:
            _LOGGER.debug("Unhandled Weiyu JSON payload keys=%s", sorted(payload.keys()))

    def _update_devices_from_subclass(self, b64_data: str) -> None:
        """Decode subclass data and update in-memory devices."""
        try:
            raw = base64.b64decode(b64_data)
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            _LOGGER.warning("Failed to decode subclass payload")
            return

        self._resolve_pending_subdev_request(True)

        changed: set[str] = set()
        value_blocks = data.get("Values")
        if not isinstance(value_blocks, list):
            value_blocks = data.get("values")
        if not isinstance(value_blocks, list):
            value_blocks = [data.get("Value", {})]
        if value_blocks == [None] or value_blocks == [{}]:
            alt_value = data.get("value")
            if isinstance(alt_value, dict):
                value_blocks = [alt_value]

        for value in value_blocks:
            if not isinstance(value, dict):
                continue
            base_class = (
                value.get("class")
                or value.get("Class")
                or value.get("devno")
                or value.get("DevNo")
                or value.get("devNo")
            )
            child_items = value.get("child") or value.get("Child") or []
            if isinstance(child_items, dict):
                child_items = [child_items]
            if not isinstance(child_items, list):
                continue

            for child in child_items:
                if not isinstance(child, dict):
                    continue
                devno = (
                    child.get("class")
                    or child.get("Class")
                    or child.get("devno")
                    or child.get("DevNo")
                    or child.get("devNo")
                    or base_class
                )
                if not devno:
                    continue

                # We only expose breaker subdevices (BK prefix) as requested.
                if not str(devno).startswith("BK"):
                    continue

                previous = self.devices.get(devno, {})
                old_state = previous.get("state")
                previous_raw = previous.get("raw", {})
                merged_raw = dict(previous_raw) if isinstance(previous_raw, dict) else {}
                merged_raw.update(child)

                if "state" in child:
                    next_state = int(child.get("state", 0))
                elif old_state is not None:
                    next_state = int(old_state)
                else:
                    next_state = 0

                previous_meta = previous.get("meta", {})
                merged_meta = dict(previous_meta) if isinstance(previous_meta, dict) else {}
                merged_meta.update(
                    {
                        "category": data.get("category", merged_meta.get("category")),
                        "version": data.get("version", merged_meta.get("version")),
                        "bus_id": data.get("BusID", merged_meta.get("bus_id")),
                        "devtag": data.get("devtag", merged_meta.get("devtag")),
                    }
                )

                if "connected" in data:
                    next_connected = int(data.get("connected", 0))
                elif "connected" in previous:
                    next_connected = int(previous.get("connected", 0))
                else:
                    next_connected = int(self.gateway_info.get("connected", 0) or 0)

                self.devices[devno] = {
                    "state": next_state,
                    "connected": next_connected,
                    "raw": merged_raw,
                    "meta": merged_meta,
                    "model": self._build_model_name(data) if data.get("category") or data.get("devtag") else previous.get("model", "未知型号"),
                    "device_type": self._build_device_type(data)
                    if data.get("devtag") is not None or data.get("category") is not None
                    else previous.get("device_type", "断路器"),
                }
                self._handle_alarm_transition(devno)
                if old_state is None or old_state != self.devices[devno]["state"]:
                    changed.add(devno)

        self._notify_listeners(changed)

    async def _async_send_packet(self, obj: dict) -> None:
        """Serialize and send one framed packet."""
        if not self._gateway_writer:
            raise HomeAssistantError("网关当前未连接，命令未发送")
        payload = json.dumps(obj, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        frame = bytearray()
        frame.append(0xFA)
        frame.extend(len(payload).to_bytes(4, byteorder="big"))
        frame.extend(payload)
        frame.append(sum(payload) & 0xFF)
        frame.append(0xFB)
        try:
            self._gateway_writer.write(bytes(frame))
            await self._gateway_writer.drain()
        except (ConnectionResetError, BrokenPipeError, OSError) as exc:
            await self._handle_send_connection_error(exc)
            raise HomeAssistantError("网关连接已断开，请稍后重试") from exc

    async def _handle_send_connection_error(self, exc: Exception) -> None:
        """Handle connection-lost errors during command sending."""
        _LOGGER.debug("Send failed due to connection issue: %s", exc)
        self._io_generation += 1
        self.gateway_info["connected"] = 0
        self._connected_since_monotonic = None
        self.set_gateway_activity("恢复中")
        writer = self._gateway_writer
        self._gateway_writer = None
        self._gateway_reader = None
        if writer:
            writer.close()
            try:
                await writer.wait_closed()
            except (BrokenPipeError, ConnectionResetError, OSError):
                pass
        self._notify_listeners(set())
        self.hass.async_create_task(self._try_re_register())

    def _notify_listeners(self, changed_devnos: set[str]) -> None:
        """Notify entity listeners."""
        for listener in list(self._listeners):
            listener(changed_devnos)

    def _prune_pending_subdev_requests(self, timeout_seconds: int = 30) -> None:
        """Expire timed-out subdev requests and mark as failed."""
        now = monotonic()
        while self._pending_subdev_requests and now - self._pending_subdev_requests[0] > timeout_seconds:
            self._pending_subdev_requests.popleft()

    def _resolve_pending_subdev_request(self, _success: bool) -> None:
        """Resolve one pending subdev request."""
        self._prune_pending_subdev_requests()
        if self._pending_subdev_requests:
            self._pending_subdev_requests.popleft()

    async def _try_re_register(self) -> None:
        """Best-effort re-register service endpoint after disconnect."""
        async with self._re_register_lock:
            if int(self.gateway_info.get("connected", 0) or 0) == 1 and self._gateway_writer is not None:
                return
            self.set_gateway_activity("重连中")
            try:
                await self._register_service_ip()
                self.set_gateway_activity("待连接")
            except Exception as exc:
                _LOGGER.debug("Re-register service endpoint failed: %s", exc)

    async def _register_keepalive_loop(self) -> None:
        """Periodically ensure gateway keeps service endpoint registration."""
        while True:
            try:
                await asyncio.sleep(5)
                connected = int(self.gateway_info.get("connected", 0) or 0)
                if connected == 1:
                    # Some gateway firmwares appear to expire service registration around ~190s.
                    # Refresh registration before expiry to reduce periodic forced disconnects.
                    if monotonic() - self._last_register_ok_monotonic > 120:
                        try:
                            await self._register_service_ip()
                            _LOGGER.debug("Periodic service registration refresh: ok")
                        except Exception as exc:
                            _LOGGER.debug("Periodic service registration refresh failed: %s", exc)

                    # Fallback payload-silence guard.
                    timeout_s = max(180, int(self._target_report_cycle_seconds) * 3 + 90)
                    silent_s = monotonic() - self._last_gateway_payload_monotonic
                    if silent_s > timeout_s:
                        _LOGGER.warning(
                            "Gateway silent for %.1fs (timeout=%ss), mark offline and re-register",
                            silent_s,
                            timeout_s,
                        )
                        self._io_generation += 1
                        read_task = self._read_task
                        self._read_task = None
                        if read_task and not read_task.done():
                            read_task.cancel()
                            try:
                                await read_task
                            except asyncio.CancelledError:
                                pass
                            except Exception:
                                pass
                        self.gateway_info["connected"] = 0
                        self._connected_since_monotonic = None
                        self.set_gateway_activity("超时重连")
                        writer = self._gateway_writer
                        self._gateway_writer = None
                        self._gateway_reader = None
                        if writer:
                            writer.close()
                            try:
                                await writer.wait_closed()
                            except (BrokenPipeError, ConnectionResetError, OSError):
                                pass
                        self._notify_listeners(set())
                        await self._try_re_register()
                        continue
                if connected == 0:
                    self.set_gateway_activity("重连中")
                    await self._try_re_register()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                _LOGGER.debug("Register keepalive loop error: %s", exc)

    async def _link_probe_loop(self) -> None:
        """Low-frequency probe to keep gateway session active across flaky LAN/NAT."""
        while True:
            try:
                await asyncio.sleep(50)
                if int(self.gateway_info.get("connected", 0) or 0) != 1:
                    continue
                if self._gateway_writer is None:
                    continue
                # Keep this very light and compatible with existing flow.
                await self._async_send_packet({"actionType": "report", "actionTarget": "subver"})
            except asyncio.CancelledError:
                return
            except HomeAssistantError:
                # _async_send_packet already transitions state/re-registers on link errors.
                continue
            except Exception as exc:
                _LOGGER.debug("Link probe loop error: %s", exc)

    async def _post_connect_scan_subdevices(self) -> None:
        """Send ``report/subdev`` repeatedly until device count stabilizes (multi-packet discovery)."""
        prev_count = -1
        stable_rounds = 0
        max_rounds = 24
        try:
            for attempt in range(1, max_rounds + 1):
                if not self._gateway_writer:
                    return
                try:
                    await self.async_request_subdevices()
                except Exception as exc:
                    _LOGGER.warning("Post-connect subdev attempt %s failed: %s", attempt, exc)
                await asyncio.sleep(1.1)
                if not self._gateway_writer:
                    return
                count = len(self.devices)
                self.set_gateway_activity(f"检索中({count}台)")
                if count > 0 and count == prev_count:
                    stable_rounds += 1
                    if stable_rounds >= 2:
                        _LOGGER.info(
                            "Post-connect: subdevice scan stable at %s devices after %s subdev round(s)",
                            count,
                            attempt,
                        )
                        break
                else:
                    stable_rounds = 0
                prev_count = count
            else:
                if self.devices:
                    _LOGGER.info(
                        "Post-connect: subdevice scan stopped at %s devices after %s rounds (no stability)",
                        len(self.devices),
                        max_rounds,
                    )
                else:
                    _LOGGER.warning(
                        "Post-connect: no subdevices after %s subdev rounds (check gateway / RS485); keep cached devices if any",
                        max_rounds,
                    )
        finally:
            if self._gateway_writer and int(self.gateway_info.get("connected", 0) or 0):
                self.set_gateway_activity("运行中" if self.devices else "运行中(无设备)")
                self._notify_listeners(set())

    @staticmethod
    def _build_model_name(data: dict) -> str:
        """Build readable model text from protocol fields."""
        category = str(data.get("category") or "").strip()
        devtag = str(data.get("devtag") or "").strip()
        if category and devtag:
            return f"{category} ({devtag})"
        if category:
            return category
        if devtag:
            return devtag
        return "未知型号"

    @staticmethod
    def _build_device_type(data: dict) -> str:
        """Classify breaker type using devtag/category from docs."""
        devtag = str(data.get("devtag") or "")
        if "ldL" in devtag:
            return "漏电保护断路器"
        return "断路器"

    def _update_gateway_version(self, b64_data: str) -> None:
        """Update gateway model/version from version report."""
        try:
            raw = base64.b64decode(b64_data)
            data = json.loads(raw.decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError):
            _LOGGER.debug("Failed to decode gateway version payload")
            return

        category = str(data.get("category") or "").strip()
        version = str(data.get("version") or "").strip()
        devno = str(data.get("devno") or "").strip()
        if category:
            self.gateway_info["model"] = category
        if version:
            self.gateway_info["version"] = version
        if devno:
            self.gateway_info["devno"] = devno
        rc = data.get("reportCycle")
        if rc is None:
            rc = data.get("reprotCycle")
        if rc is not None:
            try:
                self._apply_report_cycle_from_gateway(int(rc))
            except (TypeError, ValueError):
                pass
        self._notify_listeners(set())

    @staticmethod
    def _enable_tcp_keepalive(writer: asyncio.StreamWriter) -> None:
        """Enable TCP keepalive on accepted gateway socket."""
        sock = writer.get_extra_info("socket")
        if sock is None:
            return
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
        except OSError:
            return

    def _handle_alarm_transition(self, devno: str) -> None:
        """Emit event and notification when device enters alarm state."""
        data = self.devices.get(devno, {})
        raw = data.get("raw", {})
        wstate = int(raw.get("wstate", 0) or 0)
        alarm_effective = self._filter_alarm_bits_for_notify(int(raw.get("alarm", 0) or 0))
        fault_raw = int(raw.get("fault", 0) or 0)
        fault_effective = self._filter_fault_bits_for_notify(fault_raw)
        trip_effective = self._filter_trip_bits_for_notify(int(raw.get("trip", 0) or 0))
        pretrip_effective = self._filter_pretrip_bits_for_notify(int(raw.get("pretrip", 0) or 0))
        has_issue = any(
            [
                alarm_effective > 0,
                fault_effective > 0,
                pretrip_effective > 0,
                trip_effective > 0,
                bool(wstate & ((1 << 1) | (1 << 2) | (1 << 3))),
            ]
        )

        previous = self._alarm_cache.get(devno, False)
        self._alarm_cache[devno] = has_issue
        if not has_issue or previous:
            return

        device_name = self.get_device_name(devno)
        status_text = self._build_alarm_status_text(raw)
        alarm_bits_text = self._build_alarm_bits_text(alarm_effective)
        fault_bits_text = self._build_fault_bits_text(fault_raw)
        trip_bits_text = self._build_trip_bits_text(trip_effective)
        pretrip_bits_text = self._build_pretrip_bits_text(pretrip_effective)
        alarm_payload = {
            "devno": devno,
            "name": device_name,
            "status": status_text,
            "alarm": alarm_effective,
            "fault": fault_effective,
            "trip": trip_effective,
            "pretrip": pretrip_effective,
            "voltage": self._scale_metric(raw.get("voltage"), 100),
            "current": self._scale_metric(raw.get("electric"), 1000),
            "power": self._scale_metric(raw.get("powerrate"), 100),
            "line_temp": self._scale_metric(raw.get("ltemp"), 100),
            "chip_temp": self._scale_metric(raw.get("ctemp"), 100),
            "time": dt_util.utcnow().isoformat(),
        }
        record_payload = self._build_record_payload(raw)
        if record_payload:
            alarm_payload["record"] = record_payload
        self.hass.bus.async_fire(f"{DOMAIN}_alarm", alarm_payload)

        record_lines = self._build_record_lines(record_payload)
        record_block = ""
        if record_lines:
            record_block = "\n\n记录快照:\n" + "\n".join(record_lines)
        self.hass.async_create_task(
            self.hass.services.async_call(
                "persistent_notification",
                "create",
                {
                    "title": f"{device_name} 电气告警",
                    "message": (
                        "检测到设备出现电气异常，请尽快检查。\n\n"
                        f"设备: {device_name}\n"
                        f"状态: {alarm_payload['status']}\n"
                        f"告警状态: {alarm_bits_text}\n"
                        f"故障状态: {fault_bits_text}\n"
                        f"脱扣状态: {trip_bits_text}\n"
                        f"预脱扣状态: {pretrip_bits_text}\n"
                        f"电压: {self._fmt_metric(alarm_payload['voltage'], 'V')}\n"
                        f"电流: {self._fmt_metric(alarm_payload['current'], 'A')}\n"
                        f"有功功率: {self._fmt_metric(alarm_payload['power'], 'W')}\n"
                        f"火线温度: {self._fmt_metric(alarm_payload['line_temp'], '°C')}\n"
                        f"芯片温度: {self._fmt_metric(alarm_payload['chip_temp'], '°C')}"
                        f"{record_block}"
                    ),
                    "notification_id": f"weiyu_alarm_{devno}",
                },
                blocking=False,
            )
        )

    @staticmethod
    def _scale_metric(value: object, divisor: int) -> float | None:
        """Scale protocol raw value to human-readable float."""
        if value is None:
            return None
        try:
            return round(float(value) / divisor, 3)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _fmt_metric(value: float | None, unit: str) -> str:
        """Format metric with unit for notification text."""
        if value is None:
            return "-"
        return f"{value}{unit}"

    @classmethod
    def _pick_scaled_metric(cls, raw: dict, keys: tuple[str, ...], divisor: int) -> float | None:
        """Return first available numeric metric among candidate keys."""
        for key in keys:
            if key in raw and raw.get(key) is not None:
                val = cls._scale_metric(raw.get(key), divisor)
                if val is not None:
                    return val
        return None

    @staticmethod
    def _pick_raw_value(raw: dict, keys: tuple[str, ...]) -> str | None:
        """Return first non-empty raw value among candidate keys."""
        for key in keys:
            if key not in raw:
                continue
            val = raw.get(key)
            if val is None:
                continue
            text = str(val).strip()
            if text:
                return text
        return None

    @classmethod
    def _build_record_payload(cls, raw: dict) -> dict:
        """Build optional fault/alarm snapshot payload from vendor record fields."""
        fault = {
            "current": cls._pick_scaled_metric(raw, ("fault_electric", "fault_current", "fcurrent"), 10),
            "leakage_current": cls._pick_scaled_metric(raw, ("fault_leakagecurrent", "fault_leakage", "fleakage"), 10),
            "voltage": cls._pick_scaled_metric(raw, ("fault_voltage", "fvoltage"), 10),
            "active_power": cls._pick_scaled_metric(raw, ("fault_powerrate", "fault_power", "fpower"), 1),
            "reactive_power": cls._pick_scaled_metric(raw, ("fault_reactivepower", "fault_qpower", "fqpower"), 1),
            "power_factor": cls._pick_scaled_metric(raw, ("fault_powerfactor", "fpowerfactor"), 1000),
            "frequency": cls._pick_scaled_metric(raw, ("fault_frequency", "ffrequency"), 100),
            "line_temp": cls._pick_scaled_metric(raw, ("fault_ltemp", "fltemp"), 1),
            "chip_temp": cls._pick_scaled_metric(raw, ("fault_ctemp", "fctemp"), 1),
            "time": cls._pick_raw_value(raw, ("fault_time", "ftime")),
            "count": cls._pick_raw_value(raw, ("fault_count", "fcnt")),
        }
        alarm = {
            "current": cls._pick_scaled_metric(raw, ("alarm_electric", "alarm_current", "acurrent"), 10),
            "leakage_current": cls._pick_scaled_metric(raw, ("alarm_leakagecurrent", "alarm_leakage", "aleakage"), 10),
            "voltage": cls._pick_scaled_metric(raw, ("alarm_voltage", "avoltage"), 10),
            "active_power": cls._pick_scaled_metric(raw, ("alarm_powerrate", "alarm_power", "apower"), 1),
            "reactive_power": cls._pick_scaled_metric(raw, ("alarm_reactivepower", "alarm_qpower", "aqpower"), 1),
            "power_factor": cls._pick_scaled_metric(raw, ("alarm_powerfactor", "apowerfactor"), 1000),
            "frequency": cls._pick_scaled_metric(raw, ("alarm_frequency", "afrequency"), 100),
            "line_temp": cls._pick_scaled_metric(raw, ("alarm_ltemp", "altemp"), 1),
            "chip_temp": cls._pick_scaled_metric(raw, ("alarm_ctemp", "actemp"), 1),
            "time": cls._pick_raw_value(raw, ("alarm_time", "atime")),
            "count": cls._pick_raw_value(raw, ("alarm_count", "acnt")),
        }
        fault = {k: v for k, v in fault.items() if cls._is_meaningful_record_value(k, v)}
        alarm = {k: v for k, v in alarm.items() if cls._is_meaningful_record_value(k, v)}
        payload: dict[str, dict] = {}
        if fault:
            payload["fault_record"] = fault
        if alarm:
            payload["alarm_record"] = alarm
        return payload

    @staticmethod
    def _is_meaningful_record_value(key: str, value: object) -> bool:
        """Filter placeholder values from record snapshot (e.g., all-zero metrics)."""
        if value is None:
            return False
        if key in {"time", "count"}:
            text = str(value).strip()
            return text not in {"", "0", "0.0"}
        if isinstance(value, (int, float)):
            return abs(float(value)) > 1e-9
        return True

    @classmethod
    def _build_record_lines(cls, record_payload: dict) -> list[str]:
        """Render optional snapshot payload to notification lines."""
        lines: list[str] = []
        fault = record_payload.get("fault_record", {})
        alarm = record_payload.get("alarm_record", {})

        def _append_metric_line(bucket: dict, label: str, key: str, unit: str) -> None:
            val = bucket.get(key)
            if val is None:
                return
            lines.append(f"- {label}: {cls._fmt_metric(val, unit)}")

        if fault:
            lines.append("故障记录:")
            if "time" in fault:
                lines.append(f"- 时间: {fault['time']}")
            if "count" in fault:
                lines.append(f"- 次数: {fault['count']}")
            _append_metric_line(fault, "电流", "current", "A")
            _append_metric_line(fault, "漏电流", "leakage_current", "mA")
            _append_metric_line(fault, "电压", "voltage", "V")
            _append_metric_line(fault, "有功功率", "active_power", "W")
            _append_metric_line(fault, "无功功率", "reactive_power", "W")
            _append_metric_line(fault, "功率因数", "power_factor", "")
            _append_metric_line(fault, "频率", "frequency", "Hz")
            _append_metric_line(fault, "线温", "line_temp", "°C")
            _append_metric_line(fault, "芯片温度", "chip_temp", "°C")
        if alarm:
            lines.append("报警记录:")
            if "time" in alarm:
                lines.append(f"- 时间: {alarm['time']}")
            if "count" in alarm:
                lines.append(f"- 次数: {alarm['count']}")
            _append_metric_line(alarm, "电流", "current", "A")
            _append_metric_line(alarm, "漏电流", "leakage_current", "mA")
            _append_metric_line(alarm, "电压", "voltage", "V")
            _append_metric_line(alarm, "有功功率", "active_power", "W")
            _append_metric_line(alarm, "无功功率", "reactive_power", "W")
            _append_metric_line(alarm, "功率因数", "power_factor", "")
            _append_metric_line(alarm, "频率", "frequency", "Hz")
            _append_metric_line(alarm, "线温", "line_temp", "°C")
            _append_metric_line(alarm, "芯片温度", "chip_temp", "°C")
        return lines

    @staticmethod
    def _build_alarm_status_text(raw: dict) -> str:
        """Build merged status text for alarm notification."""
        wstate = int(raw.get("wstate", 0) or 0)
        wmode = int(raw.get("wmode", 0) or 0)
        alarm = WeiyuGatewayClient._filter_alarm_bits_for_notify(int(raw.get("alarm", 0) or 0))
        fault = WeiyuGatewayClient._filter_fault_bits_for_notify(int(raw.get("fault", 0) or 0))
        trip = WeiyuGatewayClient._filter_trip_bits_for_notify(int(raw.get("trip", 0) or 0))
        pretrip = WeiyuGatewayClient._filter_pretrip_bits_for_notify(int(raw.get("pretrip", 0) or 0))

        states: list[str] = []
        if wmode == 1:
            states.append("锁定")
        if fault > 0 or bool(wstate & (1 << 2)):
            states.append("故障")
        if trip > 0:
            states.append("脱扣")
        if pretrip > 0 or bool(wstate & (1 << 3)):
            states.append("预脱扣")
        if alarm > 0 or bool(wstate & (1 << 1)):
            states.append("告警")
        if bool(wstate & (1 << 4)):
            states.append("漏电测试中")
        if bool(wstate & (1 << 7)):
            states.append("漏电测试成功")
        if not states:
            states.append("电气异常")
        return "、".join(states)

    @staticmethod
    def _decode_bit_labels(value: int, labels: dict[int, str], prefix: str) -> list[str]:
        """Decode bitmap value to readable labels."""
        if value <= 0:
            return []
        out: list[str] = []
        bit = 0
        num = int(value)
        while num > 0:
            if num & 1:
                label = labels.get(bit)
                if label:
                    out.append(label)
            num >>= 1
            bit += 1
        return out

    @classmethod
    def _build_alarm_bits_text(cls, alarm: int) -> str:
        """Build decoded alarm text from alarm bitmap."""
        alarm = cls._filter_alarm_bits_for_notify(alarm)
        labels = {
            0: "过压告警",
            1: "欠压告警",
            2: "过流/过载告警",
            3: "漏电告警",
            4: "温度告警",
            5: "打火告警",
        }
        decoded = cls._decode_bit_labels(alarm, labels, "告警")
        return "无" if not decoded else "、".join(decoded)

    @staticmethod
    def _filter_alarm_bits_for_notify(alarm: int) -> int:
        """Keep only documented alarm bits and ignore reserved/undefined ones."""
        # Per current docs, keep alarm bits with clear semantics.
        actionable_bits = (0, 1, 2, 3, 4, 5)
        mask = 0
        for bit in actionable_bits:
            mask |= 1 << bit
        return int(alarm) & mask

    @classmethod
    def _build_fault_bits_text(cls, fault: int) -> str:
        """Build decoded fault text from fault bitmap."""
        fault = cls._filter_fault_bits_for_notify(fault)
        labels = {
            0: "过载故障",
            1: "瞬时故障",
            2: "漏电故障",
            3: "欠压故障",
            4: "过压故障",
            5: "高过压故障",
            6: "温度故障",
            7: "硬件漏电故障",
            10: "过功率故障",
            11: "漏电试验故障",
            12: "过用电量故障",
            13: "恶性负载故障",
            14: "打火故障",
        }
        decoded = cls._decode_bit_labels(fault, labels, "故障")
        return "无" if not decoded else "、".join(decoded)

    @staticmethod
    def _filter_fault_bits_for_notify(fault: int) -> int:
        """Keep only actionable fault bits and ignore reserved/undefined ones."""
        # Ignore reserved/undefined bits (bit8 self-test indicator, bit9 undefined, bit15 reserved).
        actionable_bits = (0, 1, 2, 3, 4, 5, 6, 7, 10, 11, 12, 13, 14)
        mask = 0
        for bit in actionable_bits:
            mask |= 1 << bit
        return int(fault) & mask

    @staticmethod
    def _filter_trip_bits_for_notify(trip: int) -> int:
        """Keep only documented trip bits and ignore reserved/undefined ones."""
        actionable_bits = (0, 1, 2, 3, 4, 5)
        mask = 0
        for bit in actionable_bits:
            mask |= 1 << bit
        return int(trip) & mask

    @staticmethod
    def _filter_pretrip_bits_for_notify(pretrip: int) -> int:
        """Keep only documented pretrip bits and ignore reserved/undefined ones."""
        actionable_bits = (0, 1, 2, 3, 4, 5)
        mask = 0
        for bit in actionable_bits:
            mask |= 1 << bit
        return int(pretrip) & mask

    @classmethod
    def _build_trip_bits_text(cls, trip: int) -> str:
        """Build decoded trip text from bitmap."""
        trip = cls._filter_trip_bits_for_notify(trip)
        labels = {
            0: "过压脱扣",
            1: "欠压脱扣",
            2: "过流/过载脱扣",
            3: "漏电脱扣",
            4: "温度脱扣",
            5: "打火脱扣",
        }
        decoded = cls._decode_bit_labels(trip, labels, "脱扣")
        return "无" if not decoded else "、".join(decoded)

    @classmethod
    def _build_pretrip_bits_text(cls, pretrip: int) -> str:
        """Build decoded pretrip text from bitmap."""
        pretrip = cls._filter_pretrip_bits_for_notify(pretrip)
        labels = {
            0: "过压预脱扣",
            1: "欠压预脱扣",
            2: "过流/过载预脱扣",
            3: "漏电预脱扣",
            4: "温度预脱扣",
            5: "打火预脱扣",
        }
        decoded = cls._decode_bit_labels(pretrip, labels, "预脱扣")
        return "无" if not decoded else "、".join(decoded)

    def get_operating_status_text(self, devno: str) -> str:
        """Return merged status including alarm/fault details."""
        data = self.get_device_data(devno)
        connected = int(data.get("connected", 0) or 0)
        state = int(data.get("state", 0) or 0)
        raw = data.get("raw", {})
        wstate = int(raw.get("wstate", 0) or 0)
        wmode = int(raw.get("wmode", 0) or 0)
        alarm = self._filter_alarm_bits_for_notify(int(raw.get("alarm", 0) or 0))
        fault = self._filter_fault_bits_for_notify(int(raw.get("fault", 0) or 0))
        trip = self._filter_trip_bits_for_notify(int(raw.get("trip", 0) or 0))
        pretrip = self._filter_pretrip_bits_for_notify(int(raw.get("pretrip", 0) or 0))
        parts: list[str] = []

        if connected == 0:
            return "离线"
        if state == 0:
            parts.append("断电")
        if wmode == 1 or bool(wstate & (1 << 5)):
            parts.append("锁定")
        if bool(wstate & (1 << 6)):
            parts.append("设置中")
        if alarm > 0:
            parts.append(f"告警({self._build_alarm_bits_text(alarm)})")
        if fault > 0:
            parts.append(f"故障({self._build_fault_bits_text(fault)})")
        if trip > 0:
            parts.append("脱扣")
        if pretrip > 0:
            parts.append("预脱扣")

        if not parts:
            return "正常"
        return "、".join(parts)
