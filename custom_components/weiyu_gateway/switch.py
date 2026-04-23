"""Switch platform for Weiyu Gateway."""

from __future__ import annotations

from homeassistant.components.switch import SwitchEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .gateway import WeiyuGatewayClient


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up switch entities from a config entry."""
    client: WeiyuGatewayClient = hass.data[DOMAIN][entry.entry_id]
    known_devnos: set[str] = set()
    entities: dict[str, WeiyuSwitch] = {}
    gateway_switch = WeiyuGatewayMasterSwitch(client, entry.entry_id)
    async_add_entities([gateway_switch])

    @callback
    def _sync_entities(_: set[str]) -> None:
        new_entities: list[WeiyuSwitch] = []
        for devno in client.devices:
            if devno in known_devnos:
                continue
            entity = WeiyuSwitch(client, devno)
            entities[devno] = entity
            known_devnos.add(devno)
            new_entities.append(entity)

        if new_entities:
            async_add_entities(new_entities)

        if gateway_switch.hass is not None:
            gateway_switch.async_write_ha_state()
        for entity in entities.values():
            if entity.hass is not None:
                entity.async_write_ha_state()

    unsub = client.add_listener(_sync_entities)
    entry.async_on_unload(unsub)
    _sync_entities(set())


class WeiyuSwitch(SwitchEntity):
    """Representation of one breaker switch."""

    _attr_has_entity_name = True
    _attr_icon = "mdi:toggle-switch-variant"

    def __init__(self, client: WeiyuGatewayClient, devno: str) -> None:
        self._client = client
        self._devno = devno
        self._attr_unique_id = f"weiyu_{devno}"
        self._attr_name = client.get_device_name(devno)

    @property
    def is_on(self) -> bool:
        """Return if switch is on (closed)."""
        return self._client.get_device_state(self._devno)

    @property
    def extra_state_attributes(self) -> dict:
        """Expose model and key metrics on switch entity."""
        data = self._client.get_device_data(self._devno)
        raw = data.get("raw", {})
        return {
            "设备编号": self._devno,
            "型号": data.get("model", "未知型号"),
            "设备分类": data.get("meta", {}).get("category"),
            "固件版本": data.get("meta", {}).get("version"),
            "电压(V)": _scale(raw.get("voltage"), 100),
            "电流(A)": _scale(raw.get("electric"), 1000),
            "有功功率(W)": _scale(raw.get("powerrate"), 100),
            "电能(kWh)": _scale(raw.get("power"), 100),
            "频率(Hz)": _scale(raw.get("frequency"), 100),
            "功率因数": _scale(raw.get("powerfactor"), 1000),
            "漏电流(mA)": _scale(raw.get("leakagecurrent"), 100),
        }

    @property
    def device_info(self) -> dict:
        """Bind all entities to one physical breaker device."""
        data = self._client.get_device_data(self._devno)
        return {
            "identifiers": {("weiyu_gateway", self._devno)},
            "name": self._client.get_device_name(self._devno),
            "manufacturer": "微羽智能",
            "model": data.get("model", "未知型号"),
            "sw_version": data.get("meta", {}).get("version"),
            "via_device": next(iter(self._client.get_gateway_identifiers())),
        }

    async def async_turn_on(self, **kwargs) -> None:
        """Turn switch on."""
        await self._client.async_set_device_state(self._devno, True)

    async def async_turn_off(self, **kwargs) -> None:
        """Turn switch off."""
        await self._client.async_set_device_state(self._devno, False)


class WeiyuGatewayMasterSwitch(SwitchEntity):
    """Gateway-level master switch for all subdevices."""

    _attr_has_entity_name = True
    _attr_name = "总开关"
    _attr_icon = "mdi:dip-switch"

    def __init__(self, client: WeiyuGatewayClient, entry_id: str) -> None:
        self._client = client
        self._attr_unique_id = f"weiyu_gateway_{entry_id}_master_switch"

    @property
    def is_on(self) -> bool:
        """Return True if all child devices are currently on."""
        return self._client.get_all_devices_state()

    @property
    def device_info(self) -> dict:
        """Bind this switch to gateway device."""
        return {
            "identifiers": self._client.get_gateway_identifiers(),
            "name": self._client.get_gateway_name(),
            "manufacturer": "Weiyu",
            "model": self._client.gateway_info.get("model", "Unknown"),
            "sw_version": self._client.gateway_info.get("version", ""),
        }

    async def async_turn_on(self, **kwargs) -> None:
        """Turn on all child devices."""
        success, fail = await self._client.async_set_all_devices_state(True)
        await self._client.hass.services.async_call(
            "persistent_notification",
            "create",
            {
                "title": "微羽总开关执行结果",
                "message": f"一键合闸完成，成功 {success} 台，失败 {fail} 台。",
                "notification_id": "weiyu_master_switch_result",
            },
            blocking=False,
        )

    async def async_turn_off(self, **kwargs) -> None:
        """Turn off all child devices."""
        success, fail = await self._client.async_set_all_devices_state(False)
        await self._client.hass.services.async_call(
            "persistent_notification",
            "create",
            {
                "title": "微羽总开关执行结果",
                "message": f"一键分闸完成，成功 {success} 台，失败 {fail} 台。",
                "notification_id": "weiyu_master_switch_result",
            },
            blocking=False,
        )


def _scale(value: object, divisor: int) -> float | None:
    """Scale integer telemetry value using protocol divisor."""
    if value is None:
        return None
    try:
        return round(float(value) / divisor, 3)
    except (TypeError, ValueError):
        return None
