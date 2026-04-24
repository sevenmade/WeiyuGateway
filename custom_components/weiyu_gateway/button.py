"""Button platform for Weiyu Gateway actions."""

from __future__ import annotations

from homeassistant.components.button import ButtonEntity
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .gateway import WeiyuGatewayClient


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up action buttons from a config entry."""
    client: WeiyuGatewayClient = hass.data[DOMAIN][entry.entry_id]
    known_devnos: set[str] = set()
    entities: list[ButtonEntity] = []

    @callback
    def _sync_entities(_: set[str]) -> None:
        new_entities: list[ButtonEntity] = []
        for devno in client.devices:
            if devno in known_devnos:
                continue
            if not client.is_leakage_protection_device(devno):
                continue

            entity = WeiyuLeakageTestButton(client, devno)
            known_devnos.add(devno)
            entities.append(entity)
            new_entities.append(entity)

        if new_entities:
            async_add_entities(new_entities)

        for entity in entities:
            if entity.hass is not None:
                entity.async_write_ha_state()

    unsub = client.add_listener(_sync_entities)
    entry.async_on_unload(unsub)
    _sync_entities(set())


class WeiyuLeakageTestButton(ButtonEntity):
    """One-shot leakage self-test trigger button."""

    _attr_has_entity_name = True
    _attr_name = "漏电自检"
    _attr_icon = "mdi:shield-check"

    def __init__(self, client: WeiyuGatewayClient, devno: str) -> None:
        self._client = client
        self._devno = devno
        self._attr_unique_id = f"weiyu_{devno}_leakage_test"

    @property
    def device_info(self) -> dict:
        """Bind this button to breaker device."""
        data = self._client.get_device_data(self._devno)
        return {
            "identifiers": {("weiyu_gateway", self._devno)},
            "name": self._client.get_device_name(self._devno),
            "manufacturer": "微羽智能",
            "model": data.get("model", "未知型号"),
            "sw_version": data.get("meta", {}).get("version"),
            "via_device": next(iter(self._client.get_gateway_identifiers())),
        }

    async def async_press(self) -> None:
        """Handle button press."""
        await self._client.async_trigger_leakage_test(self._devno)



