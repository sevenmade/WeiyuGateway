"""Config flow for Weiyu Gateway."""

from __future__ import annotations

from urllib.parse import urlparse

import voluptuous as vol

from homeassistant import config_entries
from homeassistant.helpers import selector

from .const import CONF_GATEWAY_HOST, CONF_GATEWAY_MODEL, CONF_LISTEN_IP, CONF_LISTEN_PORT, DEFAULT_LISTEN_PORT, DOMAIN
from .discovery import async_discover_gateways

CONF_DISCOVERED_GATEWAY = "discovered_gateway"
CONF_SETUP_MODE = "setup_mode"
SETUP_MODE_DISCOVER = "discover"
SETUP_MODE_MANUAL = "manual"


class WeiyuGatewayConfigFlow(config_entries.ConfigFlow, domain=DOMAIN):
    """Handle a config flow for Weiyu Gateway."""

    VERSION = 1
    _discovered_gateways: dict[str, dict[str, str]]

    async def async_step_user(self, user_input: dict | None = None):
        """Choose setup mode with explicit labels."""
        errors: dict[str, str] = {}
        if user_input is not None:
            mode = user_input[CONF_SETUP_MODE]
            if mode == SETUP_MODE_DISCOVER:
                return await self.async_step_discover()
            return await self.async_step_manual()

        schema = vol.Schema(
            {
                vol.Required(CONF_SETUP_MODE, default=SETUP_MODE_DISCOVER): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            selector.SelectOptionDict(value=SETUP_MODE_DISCOVER, label="自动搜索网关（推荐）"),
                            selector.SelectOptionDict(value=SETUP_MODE_MANUAL, label="手动输入网关地址"),
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                )
            }
        )
        return self.async_show_form(step_id="user", data_schema=schema, errors=errors)

    async def async_step_discover(self, user_input: dict | None = None):
        """Discover gateways and let user select one from dropdown."""
        errors: dict[str, str] = {}
        discovered = await async_discover_gateways()
        if not discovered:
            return self.async_show_form(step_id="discover_failed")
        self._discovered_gateways = {item["ip"]: item for item in discovered}

        discovered_map = {
            gateway["ip"]: f"{gateway['ip']} ({gateway.get('type', '-')}, {gateway.get('id', '-')})"
            for gateway in discovered
        }

        if user_input is not None:
            gateway_host = user_input[CONF_DISCOVERED_GATEWAY]
            gateway_model = self._discovered_gateways.get(gateway_host, {}).get("type", "")
            return await self._create_entry(
                gateway_host,
                user_input[CONF_LISTEN_IP],
                user_input[CONF_LISTEN_PORT],
                gateway_model,
            )

        default_gateway = discovered[0]["ip"]
        default_listen_ip = self._default_listen_ip()
        schema = vol.Schema(
            {
                vol.Required(
                    CONF_DISCOVERED_GATEWAY,
                    default=default_gateway,
                ): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            selector.SelectOptionDict(value=ip, label=label)
                            for ip, label in discovered_map.items()
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                ),
                vol.Required(CONF_LISTEN_IP, default=default_listen_ip): str,
                vol.Required(CONF_LISTEN_PORT, default=DEFAULT_LISTEN_PORT): int,
            }
        )
        return self.async_show_form(step_id="discover", data_schema=schema, errors=errors)

    async def async_step_discover_failed(self, user_input: dict | None = None):
        """Show discovery failed page with retry/manual choices."""
        errors: dict[str, str] = {}
        if user_input is not None:
            mode = user_input.get(CONF_SETUP_MODE)
            if mode not in (SETUP_MODE_DISCOVER, SETUP_MODE_MANUAL):
                errors["base"] = "invalid_setup_mode"
            elif mode == SETUP_MODE_DISCOVER:
                return await self.async_step_discover()
            else:
                return await self.async_step_manual()

        schema = vol.Schema(
            {
                vol.Required(CONF_SETUP_MODE, default=SETUP_MODE_DISCOVER): selector.SelectSelector(
                    selector.SelectSelectorConfig(
                        options=[
                            selector.SelectOptionDict(value=SETUP_MODE_DISCOVER, label="重新搜索网关"),
                            selector.SelectOptionDict(value=SETUP_MODE_MANUAL, label="手动输入网关地址"),
                        ],
                        mode=selector.SelectSelectorMode.DROPDOWN,
                    )
                )
            }
        )
        return self.async_show_form(step_id="discover_failed", data_schema=schema, errors=errors)

    async def async_step_manual(self, user_input: dict | None = None):
        """Manual setup when discovery is unavailable."""
        errors: dict[str, str] = {}
        if user_input is not None:
            return await self._create_entry(
                user_input[CONF_GATEWAY_HOST].strip(),
                user_input[CONF_LISTEN_IP],
                user_input[CONF_LISTEN_PORT],
                "",
            )

        schema = vol.Schema(
            {
                vol.Required(CONF_GATEWAY_HOST): str,
                vol.Required(CONF_LISTEN_IP, default=self._default_listen_ip()): str,
                vol.Required(CONF_LISTEN_PORT, default=DEFAULT_LISTEN_PORT): int,
            }
        )
        return self.async_show_form(
            step_id="manual",
            data_schema=schema,
            errors=errors,
        )

    async def _create_entry(self, gateway_host: str, listen_ip: str, listen_port: int, gateway_model: str):
        """Create config entry after unified validation."""
        await self.async_set_unique_id(gateway_host)
        self._abort_if_unique_id_configured()
        return self.async_create_entry(
            title=f"Weiyu {gateway_host}",
            data={
                CONF_GATEWAY_HOST: gateway_host,
                CONF_GATEWAY_MODEL: gateway_model,
                CONF_LISTEN_IP: listen_ip,
                CONF_LISTEN_PORT: listen_port,
            },
        )

    def _default_listen_ip(self) -> str:
        """Get default HA IP for listen_ip field."""
        candidates = [
            getattr(getattr(self.hass.config, "api", None), "local_ip", None),
            getattr(self.hass.config, "internal_url", None),
            getattr(self.hass.config, "external_url", None),
        ]
        for candidate in candidates:
            if not candidate:
                continue
            if isinstance(candidate, str) and candidate.startswith("http"):
                parsed = urlparse(candidate)
                if parsed.hostname:
                    return parsed.hostname
                continue
            return str(candidate)
        return ""
