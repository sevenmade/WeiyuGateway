"""Binary sensor platform for Weiyu Gateway alarms and faults."""

from __future__ import annotations

from dataclasses import dataclass

from homeassistant.components.binary_sensor import BinarySensorEntity, BinarySensorEntityDescription
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .gateway import WeiyuGatewayClient


@dataclass(frozen=True, kw_only=True)
class WeiyuBinarySensorDescription(BinarySensorEntityDescription):
    """Description for Weiyu binary sensor."""

    source_key: str
    mode: str = "numeric"  # numeric / bit
    bit_index: int | None = None


BINARY_SENSOR_TYPES: tuple[WeiyuBinarySensorDescription, ...] = (
    WeiyuBinarySensorDescription(
        key="has_issue",
        name="电气异常",
        icon="mdi:alert",
        source_key="wstate",
        mode="combined",
    ),
    WeiyuBinarySensorDescription(
        key="alarm",
        name="告警",
        icon="mdi:alarm-light",
        source_key="alarm",
    ),
    WeiyuBinarySensorDescription(
        key="fault",
        name="故障",
        icon="mdi:alert-circle",
        source_key="fault",
    ),
    WeiyuBinarySensorDescription(
        key="trip",
        name="脱扣",
        icon="mdi:lightning-bolt-outline",
        source_key="trip",
    ),
    WeiyuBinarySensorDescription(
        key="pretrip",
        name="预脱扣",
        icon="mdi:alert-outline",
        source_key="pretrip",
    ),
    WeiyuBinarySensorDescription(
        key="leakage_test_running",
        name="漏电测试中",
        icon="mdi:shield-sync",
        source_key="wstate",
        mode="bit",
        bit_index=4,
    ),
    WeiyuBinarySensorDescription(
        key="leakage_test_success",
        name="漏电测试成功",
        icon="mdi:shield-check",
        source_key="wstate",
        mode="bit",
        bit_index=7,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up binary sensors from config entry."""
    client: WeiyuGatewayClient = hass.data[DOMAIN][entry.entry_id]
    known: set[tuple[str, str]] = set()
    entities: list[WeiyuBinarySensor] = []

    @callback
    def _sync_entities(_: set[str]) -> None:
        new_entities: list[WeiyuBinarySensor] = []
        for devno in client.devices:
            for desc in BINARY_SENSOR_TYPES:
                key = (devno, desc.key)
                if key in known:
                    continue
                known.add(key)
                entity = WeiyuBinarySensor(client, devno, desc)
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


class WeiyuBinarySensor(BinarySensorEntity):
    """Binary sensor for one breaker signal."""

    _attr_has_entity_name = True

    def __init__(self, client: WeiyuGatewayClient, devno: str, description: WeiyuBinarySensorDescription) -> None:
        self.entity_description = description
        self._client = client
        self._devno = devno
        self._attr_unique_id = f"weiyu_{devno}_{description.key}"
        self._attr_name = description.name

    @property
    def is_on(self) -> bool:
        """Return binary state."""
        data = self._client.get_device_data(self._devno)
        raw = data.get("raw", {})

        if self.entity_description.mode == "combined":
            wstate = int(raw.get("wstate", 0) or 0)
            return any(
                [
                    int(raw.get("alarm", 0) or 0) > 0,
                    int(raw.get("fault", 0) or 0) > 0,
                    int(raw.get("pretrip", 0) or 0) > 0,
                    int(raw.get("trip", 0) or 0) > 0,
                    bool(wstate & ((1 << 1) | (1 << 2) | (1 << 3))),
                ]
            )

        value = int(raw.get(self.entity_description.source_key, 0) or 0)
        if self.entity_description.mode == "bit":
            if self.entity_description.bit_index is None:
                return False
            return bool(value & (1 << self.entity_description.bit_index))

        return value > 0

    @property
    def device_info(self) -> dict:
        """Bind entities to breaker device."""
        data = self._client.get_device_data(self._devno)
        return {
            "identifiers": {("weiyu_gateway", self._devno)},
            "name": self._client.get_device_name(self._devno),
            "manufacturer": "微羽智能",
            "model": data.get("model", "未知型号"),
            "sw_version": data.get("meta", {}).get("version"),
            "via_device": next(iter(self._client.get_gateway_identifiers())),
        }
