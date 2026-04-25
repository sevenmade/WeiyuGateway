"""Weiyu Gateway integration."""

from __future__ import annotations

import logging

from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import ConfigEntryNotReady

from .const import CONF_GATEWAY_HOST, CONF_GATEWAY_MODEL, CONF_LISTEN_IP, CONF_LISTEN_PORT, DOMAIN, PLATFORMS
from .gateway import WeiyuGatewayClient

_LOGGER = logging.getLogger(__name__)


async def async_setup(hass: HomeAssistant, config: dict) -> bool:
    """Set up via YAML is not supported."""
    return True


async def async_setup_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Set up Weiyu Gateway from a config entry."""
    hass.data.setdefault(DOMAIN, {})
    client = WeiyuGatewayClient(
        hass=hass,
        gateway_host=entry.data[CONF_GATEWAY_HOST],
        gateway_model=entry.data.get(CONF_GATEWAY_MODEL, ""),
        listen_ip=entry.data[CONF_LISTEN_IP],
        listen_port=entry.data[CONF_LISTEN_PORT],
    )
    try:
        await client.async_start()
    except OSError as exc:
        raise ConfigEntryNotReady(
            f"无法通过UDP向网关 {entry.data[CONF_GATEWAY_HOST]}:50500 注册或监听端口不可用: {exc}"
        ) from exc
    except Exception as exc:
        _LOGGER.exception("Failed to start Weiyu client")
        raise ConfigEntryNotReady(f"Weiyu 初始化失败: {exc}") from exc
    hass.data[DOMAIN][entry.entry_id] = client
    await hass.config_entries.async_forward_entry_setups(entry, PLATFORMS)
    return True


async def async_unload_entry(hass: HomeAssistant, entry: ConfigEntry) -> bool:
    """Unload Weiyu Gateway entry."""
    unload_ok = await hass.config_entries.async_unload_platforms(entry, PLATFORMS)
    if unload_ok:
        client: WeiyuGatewayClient = hass.data[DOMAIN].pop(entry.entry_id)
        await client.async_stop()
    return unload_ok
