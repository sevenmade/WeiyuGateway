"""Low-level gateway client for Weiyu protocol."""

from __future__ import annotations

import asyncio
import base64
import json
import logging
import socket
import uuid
from collections.abc import Callable
from collections import deque
from time import monotonic

from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.util import dt as dt_util

from .const import DOMAIN

_LOGGER = logging.getLogger(__name__)


class WeiyuGatewayClient:
    """Manage Weiyu gateway discovery registration and TCP session."""

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
        self._settings_refresh_task: asyncio.Task | None = None
        self._lock = asyncio.Lock()

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
        self._device_settings_cache: dict[str, dict[str, dict]] = {}
        self._target_report_cycle_seconds: int = 20
        self._fast_poll_until_monotonic: float = 0.0
        self._request_results: deque[tuple[float, bool]] = deque(maxlen=1024)
        self._pending_subdev_requests: deque[float] = deque(maxlen=256)
        self._last_heartbeat_monotonic: float | None = None
        self._last_heartbeat_iso: str | None = None
        self._gateway_setting_values: dict[str, float] = {
            "ov_fault": 275.0,
            "ov_alarm": 265.0,
            "ov_recover": 230.0,
            "ov_action_delay": 6.0,
            "ov_recover_delay": 20.0,
            "uv_fault": 160.0,
            "uv_alarm": 180.0,
            "uv_recover": 200.0,
            "uv_action_delay": 6.0,
            "uv_recover_delay": 20.0,
            "overload_fault": 120.0,
            "overload_alarm": 90.0,
            "overload_action_delay": 40.0,
            "leak_fault": 30.0,
            "leak_alarm": 20.0,
        }

    async def async_start(self) -> None:
        """Start TCP listener and register this HA endpoint to gateway."""
        self._server = await asyncio.start_server(
            self._handle_gateway_connected,
            host=self.listen_ip,
            port=self.listen_port,
        )
        _LOGGER.info("Weiyu server listening on %s:%s", self.listen_ip, self.listen_port)
        try:
            await self._register_service_ip()
        except Exception:
            if self._server:
                self._server.close()
                await self._server.wait_closed()
                self._server = None
            raise
        if self._register_task and not self._register_task.done():
            self._register_task.cancel()
        self._register_task = self.hass.async_create_task(self._register_keepalive_loop())

    async def async_stop(self) -> None:
        """Stop the client cleanly."""
        if self._read_task:
            self._read_task.cancel()
        if self._poll_task:
            self._poll_task.cancel()
        if self._register_task:
            self._register_task.cancel()
        if self._settings_refresh_task:
            self._settings_refresh_task.cancel()
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

    def get_gateway_setting_value(self, key: str) -> float | None:
        """Get in-memory gateway setting value."""
        return self._gateway_setting_values.get(key)

    def set_gateway_setting_value(self, key: str, value: float) -> None:
        """Set in-memory gateway setting value."""
        if key in self._gateway_setting_values:
            self._gateway_setting_values[key] = float(value)

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

    async def async_apply_gateway_settings_to_all(self) -> tuple[int, int]:
        """Apply configured protection settings to all discovered child devices."""
        success = 0
        fail = 0
        for devno in list(self.devices):
            try:
                await self.async_apply_gateway_settings_to_device(devno)
                success += 1
            except Exception as exc:
                _LOGGER.warning("Apply settings failed for %s: %s", devno, exc)
                fail += 1
        return success, fail

    async def async_apply_gateway_settings_to_device(self, devno: str) -> None:
        """Apply configured protection settings to one child device."""
        await self._send_setting_write(
            devno=devno,
            payload={
                "name": "overvoltage",
                "fault": int(self._gateway_setting_values["ov_fault"]),
                "alarm": int(self._gateway_setting_values["ov_alarm"]),
                "recover": int(self._gateway_setting_values["ov_recover"]),
                "actionDelay": int(self._gateway_setting_values["ov_action_delay"]),
                "recoverDelay": int(self._gateway_setting_values["ov_recover_delay"]),
                "bit": 0x1F,
            },
        )
        await self._send_setting_write(
            devno=devno,
            payload={
                "name": "undervoltage",
                "fault": int(self._gateway_setting_values["uv_fault"]),
                "alarm": int(self._gateway_setting_values["uv_alarm"]),
                "recover": int(self._gateway_setting_values["uv_recover"]),
                "actionDelay": int(self._gateway_setting_values["uv_action_delay"]),
                "recoverDelay": int(self._gateway_setting_values["uv_recover_delay"]),
                "bit": 0x1F,
            },
        )
        await self._send_setting_write(
            devno=devno,
            payload={
                "name": "overload",
                "fault": int(self._gateway_setting_values["overload_fault"]),
                "alarm": int(self._gateway_setting_values["overload_alarm"]),
                "actionDelay": int(self._gateway_setting_values["overload_action_delay"]),
                "bit": 0x0B,
            },
        )
        if self.is_leakage_protection_device(devno):
            await self._send_setting_write(
                devno=devno,
                payload={
                    "name": "leakcurrent",
                    "fault": int(self._gateway_setting_values["leak_fault"]),
                    "alarm": int(self._gateway_setting_values["leak_alarm"]),
                    "bit": 0x03,
                },
            )

    async def async_request_subdevices(self) -> None:
        """Actively request sub-device list/state snapshot."""
        try:
            await self._async_send_packet({"actionType": "report", "actionTarget": "subdev"})
            self._pending_subdev_requests.append(monotonic())
            self._prune_pending_subdev_requests()
        except Exception:
            self._record_request_result(False)
            raise

    async def _register_service_ip(self) -> None:
        """Register HA service address to gateway port 50500."""
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

        # Most models accept TCP on 50500. Some firmwares only respond via UDP.
        tcp_exc: Exception | None = None
        try:
            reader, writer = await asyncio.open_connection(self.gateway_host, 50500)
            writer.write(payload_bytes)
            await writer.drain()
            response = await asyncio.wait_for(reader.read(128), timeout=5)
            writer.close()
            await writer.wait_closed()
            text = response.decode(errors="ignore").strip()
            _LOGGER.debug("Weiyu register TCP response: %s", text)
            if "Set OK" not in text:
                raise RuntimeError(f"Unexpected TCP register response: {text or '<empty>'}")
            return
        except Exception as exc:
            tcp_exc = exc
            _LOGGER.debug("TCP register failed, try UDP fallback: %s", exc)

        udp_text = await asyncio.to_thread(self._register_service_ip_udp, payload_bytes)
        _LOGGER.debug("Weiyu register UDP response: %s", udp_text)
        if "Set OK" not in udp_text:
            raise RuntimeError(
                f"Gateway register failed. TCP error: {tcp_exc!r}; UDP response: {udp_text or '<empty>'}"
            )

    def _register_service_ip_udp(self, payload_bytes: bytes) -> str:
        """Fallback register over UDP for gateways without TCP 50500 listener."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.settimeout(5)
            sock.sendto(payload_bytes, (self.gateway_host, 50500))
            resp, _ = sock.recvfrom(256)
            return resp.decode("utf-8", errors="ignore").strip()
        finally:
            sock.close()

    async def _handle_gateway_connected(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        """Handle reverse TCP connection from gateway."""
        async with self._lock:
            if self._gateway_writer:
                self._gateway_writer.close()
                try:
                    await self._gateway_writer.wait_closed()
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
            self._gateway_reader = reader
            self._gateway_writer = writer
            self.gateway_info["connected"] = 1

        _LOGGER.info("Weiyu gateway connected from %s", writer.get_extra_info("peername"))
        self._enable_tcp_keepalive(writer)
        self._notify_listeners(set())
        await self._async_send_packet({"actionType": "report", "actionTarget": "subver"})
        await self.async_request_subdevices()
        self._fast_poll_until_monotonic = monotonic() + 60
        if self._settings_refresh_task and not self._settings_refresh_task.done():
            self._settings_refresh_task.cancel()
        self._settings_refresh_task = self.hass.async_create_task(self._delayed_refresh_all_settings())
        if self._poll_task and not self._poll_task.done():
            self._poll_task.cancel()
        self._poll_task = self.hass.async_create_task(self._watchdog_loop())
        self._read_task = self.hass.async_create_task(self._read_loop())

    async def _read_loop(self) -> None:
        """Read and parse framed protocol packets."""
        if self._gateway_reader is None:
            return

        buffer = bytearray()
        while True:
            chunk = await self._gateway_reader.read(4096)
            if not chunk:
                _LOGGER.warning("Gateway disconnected")
                self.gateway_info["connected"] = 0
                self._notify_listeners(set())
                self.hass.async_create_task(self._try_re_register())
                return
            buffer.extend(chunk)
            for payload in self._extract_packets(buffer):
                await self._handle_payload(payload)

    def _extract_packets(self, buffer: bytearray) -> list[dict]:
        """Extract as many valid packets as possible from stream buffer."""
        out: list[dict] = []
        while True:
            start = buffer.find(b"\xFA")
            if start < 0:
                buffer.clear()
                break
            if start > 0:
                del buffer[:start]
            if len(buffer) < 7:
                break

            msg_len = int.from_bytes(buffer[1:5], byteorder="big")
            total_len = 1 + 4 + msg_len + 1 + 1
            if len(buffer) < total_len:
                break

            packet = bytes(buffer[:total_len])
            del buffer[:total_len]

            if packet[-1] != 0xFB:
                continue
            payload_bytes = packet[5 : 5 + msg_len]
            check = packet[5 + msg_len]
            if (sum(payload_bytes) & 0xFF) != check:
                continue
            try:
                out.append(json.loads(payload_bytes.decode("utf-8")))
            except (UnicodeDecodeError, json.JSONDecodeError):
                _LOGGER.debug("Skip non-json payload")
        return out

    async def _handle_payload(self, payload: dict) -> None:
        """Handle decoded protocol payload."""
        heartbeat_key = "Heartbeat" if "Heartbeat" in payload else "heatbeat" if "heatbeat" in payload else None
        heartbeat = payload.get(heartbeat_key) if heartbeat_key else None
        if heartbeat and heartbeat_key:
            if not self.gateway_info.get("model") or self.gateway_info.get("model") == "Unknown":
                self.gateway_info["model"] = str(heartbeat)
            self._last_heartbeat_monotonic = monotonic()
            self._last_heartbeat_iso = dt_util.utcnow().isoformat()
            # Some firmwares use "heatbeat" key; echo original key for compatibility.
            await self._async_send_packet({heartbeat_key: heartbeat, "connected": 1})
            self._notify_listeners(set())
            return

        action_type = payload.get("actionType")
        if action_type == "version" and "data" in payload:
            self._update_gateway_version(payload["data"])
            return
        if action_type == "subclass" and "data" in payload:
            self._update_devices_from_subclass(payload["data"])
            return
        if action_type == "setting" and "data" in payload:
            self._handle_setting_payload(payload)
            return

        if action_type:
            _LOGGER.debug("Unhandled actionType payload: %s", action_type)

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
            value_blocks = [data.get("Value", {})]

        for value in value_blocks:
            if not isinstance(value, dict):
                continue
            base_class = value.get("class")
            child_items = value.get("child") or []
            if isinstance(child_items, dict):
                child_items = [child_items]
            if not isinstance(child_items, list):
                continue

            for child in child_items:
                if not isinstance(child, dict):
                    continue
                devno = child.get("class") or child.get("devno") or base_class
                if not devno:
                    continue

                # We only expose breaker subdevices (BK prefix) as requested.
                if not str(devno).startswith("BK"):
                    continue
                if "state" not in child:
                    continue

                old_state = self.devices.get(devno, {}).get("state")
                self.devices[devno] = {
                    "state": int(child.get("state", 0)),
                    "connected": int(data.get("connected", 0)),
                    "raw": child,
                    "meta": {
                        "category": data.get("category"),
                        "version": data.get("version"),
                        "bus_id": data.get("BusID"),
                        "devtag": data.get("devtag"),
                    },
                    "model": self._build_model_name(data),
                    "device_type": self._build_device_type(data),
                }
                self._handle_alarm_transition(devno)
                if old_state is None or old_state != self.devices[devno]["state"]:
                    changed.add(devno)
                if old_state is None:
                    self.hass.async_create_task(self._refresh_settings_for_device(devno))

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
        self.gateway_info["connected"] = 0
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

    async def _send_setting_write(self, devno: str, payload: dict) -> None:
        """Send setting/write command with base64 payload."""
        evt_id = str(uuid.uuid4())
        encoded = base64.b64encode(json.dumps(payload, ensure_ascii=False).encode("utf-8")).decode()
        await self._async_send_packet(
            {
                "actionType": "setting",
                "actionTarget": "write",
                "targetId": devno,
                "evtId": evt_id,
                "data": encoded,
            }
        )

    async def _send_setting_read(self, devno: str, name: str) -> None:
        """Send setting/read command with base64 payload."""
        evt_id = str(uuid.uuid4())
        encoded = base64.b64encode(json.dumps({"name": name}, ensure_ascii=False).encode("utf-8")).decode()
        await self._async_send_packet(
            {
                "actionType": "setting",
                "actionTarget": "read",
                "targetId": devno,
                "evtId": evt_id,
                "data": encoded,
            }
        )

    def _notify_listeners(self, changed_devnos: set[str]) -> None:
        """Notify entity listeners."""
        for listener in list(self._listeners):
            listener(changed_devnos)

    def _record_request_result(self, success: bool) -> None:
        """Store one request result sample for diagnostics."""
        self._request_results.append((monotonic(), success))

    def _prune_pending_subdev_requests(self, timeout_seconds: int = 30) -> None:
        """Expire timed-out subdev requests and mark as failed."""
        now = monotonic()
        while self._pending_subdev_requests and now - self._pending_subdev_requests[0] > timeout_seconds:
            self._pending_subdev_requests.popleft()
            self._record_request_result(False)

    def _resolve_pending_subdev_request(self, success: bool) -> None:
        """Resolve one pending subdev request."""
        self._prune_pending_subdev_requests()
        if self._pending_subdev_requests:
            self._pending_subdev_requests.popleft()
            self._record_request_result(success)

    def _get_request_stats(self, window_seconds: int = 600) -> tuple[int, int]:
        """Return total and success counts in recent window."""
        now = monotonic()
        while self._request_results and now - self._request_results[0][0] > window_seconds:
            self._request_results.popleft()
        total = len(self._request_results)
        success = sum(1 for _, ok in self._request_results if ok)
        return total, success

    def get_request_success_rate_10m(self) -> float | None:
        """Return request success rate in last 10 minutes."""
        total, success = self._get_request_stats(600)
        if total == 0:
            return None
        return round((success / total) * 100.0, 2)

    def get_gateway_health_score(self) -> int:
        """Return communication health score (0~100)."""
        rate = self.get_request_success_rate_10m()
        score = 100.0 if rate is None else float(rate)
        if int(self.gateway_info.get("connected", 0) or 0) == 0:
            score = min(score, 20.0)
        if self._last_heartbeat_monotonic is not None:
            heartbeat_silence = monotonic() - self._last_heartbeat_monotonic
            if heartbeat_silence > 90:
                score -= 20.0
            if heartbeat_silence > 180:
                score -= 30.0
        return int(max(0.0, min(100.0, score)))

    def get_last_heartbeat_time(self) -> str:
        """Return last heartbeat timestamp in ISO format."""
        return self._last_heartbeat_iso or ""

    async def _try_re_register(self) -> None:
        """Best-effort re-register service endpoint after disconnect."""
        try:
            await self._register_service_ip()
        except Exception as exc:
            _LOGGER.debug("Re-register service endpoint failed: %s", exc)

    async def _register_keepalive_loop(self) -> None:
        """Periodically ensure gateway keeps service endpoint registration."""
        while True:
            try:
                await asyncio.sleep(60)
                if int(self.gateway_info.get("connected", 0) or 0) == 0:
                    await self._try_re_register()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                _LOGGER.debug("Register keepalive loop error: %s", exc)

    async def _delayed_refresh_all_settings(self) -> None:
        """Delay a bit for subdevice list, then refresh settings."""
        try:
            await asyncio.sleep(3)
            await self.async_refresh_gateway_settings_from_devices()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            _LOGGER.debug("Initial settings refresh failed: %s", exc)

    async def async_refresh_gateway_settings_from_devices(self) -> None:
        """Read settings from all subdevices for averaging."""
        for devno in list(self.devices):
            await self._refresh_settings_for_device(devno)

    async def _refresh_settings_for_device(self, devno: str) -> None:
        """Read setting profiles from one subdevice."""
        profile_names = ["overvoltage", "undervoltage", "overload"]
        if self.is_leakage_protection_device(devno):
            profile_names.append("leakcurrent")
        for name in profile_names:
            try:
                await self._send_setting_read(devno, name)
            except Exception as exc:
                _LOGGER.debug("Setting read failed for %s/%s: %s", devno, name, exc)
            await asyncio.sleep(0.05)

    def _handle_setting_payload(self, payload: dict) -> None:
        """Parse setting/read response and refresh averaged gateway settings."""
        try:
            decoded = json.loads(base64.b64decode(payload["data"]).decode("utf-8"))
        except (ValueError, UnicodeDecodeError, json.JSONDecodeError, KeyError):
            _LOGGER.debug("Skip invalid setting payload")
            return

        profile = str(decoded.get("name") or "")
        if profile not in {"overvoltage", "undervoltage", "overload", "leakcurrent"}:
            return

        target_id = str(payload.get("targetId") or decoded.get("devno") or decoded.get("class") or "")
        if not target_id:
            return

        settings = self._device_settings_cache.setdefault(target_id, {})
        settings[profile] = decoded
        self._rebuild_gateway_average_settings()

    def _rebuild_gateway_average_settings(self) -> None:
        """Recompute gateway setting values using average of known subdevices."""
        numeric_samples: dict[str, list[float]] = {key: [] for key in self._gateway_setting_values}
        profile_map = {
            "overvoltage": {
                "fault": "ov_fault",
                "alarm": "ov_alarm",
                "recover": "ov_recover",
                "actionDelay": "ov_action_delay",
                "recoverDelay": "ov_recover_delay",
            },
            "undervoltage": {
                "fault": "uv_fault",
                "alarm": "uv_alarm",
                "recover": "uv_recover",
                "actionDelay": "uv_action_delay",
                "recoverDelay": "uv_recover_delay",
            },
            "overload": {
                "fault": "overload_fault",
                "alarm": "overload_alarm",
                "actionDelay": "overload_action_delay",
            },
            "leakcurrent": {
                "fault": "leak_fault",
                "alarm": "leak_alarm",
            },
        }

        for profile_payloads in self._device_settings_cache.values():
            for profile_name, field_map in profile_map.items():
                payload = profile_payloads.get(profile_name, {})
                for source_key, target_key in field_map.items():
                    raw = payload.get(source_key)
                    try:
                        numeric_samples[target_key].append(float(raw))
                    except (TypeError, ValueError):
                        continue

        changed = False
        for target_key, values in numeric_samples.items():
            if not values:
                continue
            avg_value = round(sum(values) / len(values), 2)
            if self._gateway_setting_values.get(target_key) != avg_value:
                self._gateway_setting_values[target_key] = avg_value
                changed = True
        if changed:
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
        self._notify_listeners(set())

    async def _watchdog_loop(self) -> None:
        """Actively poll subdevice data at configured interval."""
        while True:
            try:
                interval = max(5, int(self._target_report_cycle_seconds))
                if monotonic() < self._fast_poll_until_monotonic:
                    interval = 5
                await asyncio.sleep(interval)
                if not self._gateway_writer:
                    return
                self._prune_pending_subdev_requests()
                await self.async_request_subdevices()
            except asyncio.CancelledError:
                return
            except Exception as exc:
                _LOGGER.debug("Active polling failed: %s", exc)
                self.gateway_info["connected"] = 0
                self._notify_listeners(set())
                await self._try_re_register()

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
        has_issue = any(
            [
                int(raw.get("alarm", 0) or 0) > 0,
                int(raw.get("fault", 0) or 0) > 0,
                int(raw.get("pretrip", 0) or 0) > 0,
                int(raw.get("trip", 0) or 0) > 0,
                bool(wstate & ((1 << 1) | (1 << 2) | (1 << 3))),
            ]
        )

        previous = self._alarm_cache.get(devno, False)
        self._alarm_cache[devno] = has_issue
        if not has_issue or previous:
            return

        device_name = self.get_device_name(devno)
        status_text = self._build_alarm_status_text(raw)
        alarm_payload = {
            "devno": devno,
            "name": device_name,
            "status": status_text,
            "alarm": int(raw.get("alarm", 0) or 0),
            "fault": int(raw.get("fault", 0) or 0),
            "trip": int(raw.get("trip", 0) or 0),
            "pretrip": int(raw.get("pretrip", 0) or 0),
            "voltage": self._scale_metric(raw.get("voltage"), 100),
            "current": self._scale_metric(raw.get("electric"), 1000),
            "power": self._scale_metric(raw.get("powerrate"), 100),
            "line_temp": self._scale_metric(raw.get("ltemp"), 100),
            "chip_temp": self._scale_metric(raw.get("ctemp"), 100),
            "time": dt_util.utcnow().isoformat(),
        }
        self.hass.bus.async_fire(f"{DOMAIN}_alarm", alarm_payload)
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
                        f"告警位: {alarm_payload['alarm']}\n"
                        f"故障位: {alarm_payload['fault']}\n"
                        f"脱扣位: {alarm_payload['trip']}\n"
                        f"预脱扣位: {alarm_payload['pretrip']}\n"
                        f"电压: {self._fmt_metric(alarm_payload['voltage'], 'V')}\n"
                        f"电流: {self._fmt_metric(alarm_payload['current'], 'A')}\n"
                        f"有功功率: {self._fmt_metric(alarm_payload['power'], 'W')}\n"
                        f"火线温度: {self._fmt_metric(alarm_payload['line_temp'], '°C')}\n"
                        f"芯片温度: {self._fmt_metric(alarm_payload['chip_temp'], '°C')}"
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

    @staticmethod
    def _build_alarm_status_text(raw: dict) -> str:
        """Build merged status text for alarm notification."""
        wstate = int(raw.get("wstate", 0) or 0)
        alarm = int(raw.get("alarm", 0) or 0)
        fault = int(raw.get("fault", 0) or 0)
        trip = int(raw.get("trip", 0) or 0)
        pretrip = int(raw.get("pretrip", 0) or 0)

        states: list[str] = []
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
