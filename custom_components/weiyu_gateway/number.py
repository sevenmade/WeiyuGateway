"""Number platform for gateway-level protection settings."""

from __future__ import annotations

from dataclasses import dataclass

from homeassistant.components.number import NumberEntity, NumberEntityDescription, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfTime
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .gateway import WeiyuGatewayClient


@dataclass(frozen=True, kw_only=True)
class WeiyuSettingNumberDescription(NumberEntityDescription):
    """Description for one gateway setting number entity."""

    setting_key: str
    ui_mode: str = "raw"  # raw / percent
    source_min: float | None = None
    source_max: float | None = None


SETTING_NUMBERS: tuple[WeiyuSettingNumberDescription, ...] = (
    WeiyuSettingNumberDescription(
        key="ov_fault",
        name="过压故障阈值",
        icon="mdi:flash-alert",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="ov_fault",
        ui_mode="percent",
        source_min=200,
        source_max=320,
    ),
    WeiyuSettingNumberDescription(
        key="ov_alarm",
        name="过压告警阈值",
        icon="mdi:flash",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="ov_alarm",
        ui_mode="percent",
        source_min=200,
        source_max=320,
    ),
    WeiyuSettingNumberDescription(
        key="ov_recover",
        name="过压恢复阈值",
        icon="mdi:flash-outline",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="ov_recover",
        ui_mode="percent",
        source_min=180,
        source_max=300,
    ),
    WeiyuSettingNumberDescription(
        key="ov_action_delay",
        name="过压动作延时",
        icon="mdi:timer-outline",
        native_min_value=0,
        native_max_value=120,
        native_step=1,
        native_unit_of_measurement=UnitOfTime.SECONDS,
        mode=NumberMode.BOX,
        setting_key="ov_action_delay",
    ),
    WeiyuSettingNumberDescription(
        key="uv_fault",
        name="欠压故障阈值",
        icon="mdi:flash-alert",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="uv_fault",
        ui_mode="percent",
        source_min=80,
        source_max=240,
    ),
    WeiyuSettingNumberDescription(
        key="uv_alarm",
        name="欠压告警阈值",
        icon="mdi:flash",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="uv_alarm",
        ui_mode="percent",
        source_min=80,
        source_max=240,
    ),
    WeiyuSettingNumberDescription(
        key="uv_recover",
        name="欠压恢复阈值",
        icon="mdi:flash-outline",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="uv_recover",
        ui_mode="percent",
        source_min=80,
        source_max=260,
    ),
    WeiyuSettingNumberDescription(
        key="uv_action_delay",
        name="欠压动作延时",
        icon="mdi:timer-outline",
        native_min_value=0,
        native_max_value=120,
        native_step=1,
        native_unit_of_measurement=UnitOfTime.SECONDS,
        mode=NumberMode.BOX,
        setting_key="uv_action_delay",
    ),
    WeiyuSettingNumberDescription(
        key="overload_fault",
        name="过载故障阈值",
        icon="mdi:gauge",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="overload_fault",
        ui_mode="percent",
        source_min=50,
        source_max=200,
    ),
    WeiyuSettingNumberDescription(
        key="overload_alarm",
        name="过载告警阈值",
        icon="mdi:gauge-low",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="overload_alarm",
        ui_mode="percent",
        source_min=40,
        source_max=180,
    ),
    WeiyuSettingNumberDescription(
        key="overload_action_delay",
        name="过载动作延时",
        icon="mdi:timer-outline",
        native_min_value=0,
        native_max_value=600,
        native_step=1,
        native_unit_of_measurement=UnitOfTime.SECONDS,
        mode=NumberMode.BOX,
        setting_key="overload_action_delay",
    ),
    WeiyuSettingNumberDescription(
        key="leak_fault",
        name="漏电故障阈值",
        icon="mdi:current-dc",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="leak_fault",
        ui_mode="percent",
        source_min=1,
        source_max=500,
    ),
    WeiyuSettingNumberDescription(
        key="leak_alarm",
        name="漏电告警阈值",
        icon="mdi:current-dc",
        native_min_value=0,
        native_max_value=100,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="leak_alarm",
        ui_mode="percent",
        source_min=1,
        source_max=500,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up gateway parameter number entities."""
    client: WeiyuGatewayClient = hass.data[DOMAIN][entry.entry_id]
    entities = [WeiyuGatewaySettingNumber(client, entry.entry_id, desc) for desc in SETTING_NUMBERS]
    async_add_entities(entities)

    @callback
    def _sync_numbers(_: set[str]) -> None:
        for entity in entities:
            if entity.hass is not None:
                entity.async_write_ha_state()

    unsub = client.add_listener(_sync_numbers)
    entry.async_on_unload(unsub)


class WeiyuGatewaySettingNumber(NumberEntity):
    """Number entity bound to gateway-level parameter cache."""

    _attr_has_entity_name = True

    def __init__(
        self, client: WeiyuGatewayClient, entry_id: str, description: WeiyuSettingNumberDescription
    ) -> None:
        self.entity_description = description
        self._client = client
        self._attr_unique_id = f"weiyu_gateway_{entry_id}_setting_{description.key}"
        self._attr_name = description.name

    @property
    def native_value(self) -> float | None:
        """Return current in-memory setting value."""
        raw_value = self._client.get_gateway_setting_value(self.entity_description.setting_key)
        if raw_value is None:
            return None
        if self.entity_description.ui_mode != "percent":
            return raw_value
        source_min = self.entity_description.source_min
        source_max = self.entity_description.source_max
        if source_min is None or source_max is None or source_max <= source_min:
            return raw_value
        percent = ((raw_value - source_min) / (source_max - source_min)) * 100
        return round(min(100.0, max(0.0, percent)), 2)

    async def async_set_native_value(self, value: float) -> None:
        """Store value locally, waiting for apply button to push to devices."""
        save_value = value
        if self.entity_description.ui_mode == "percent":
            source_min = self.entity_description.source_min
            source_max = self.entity_description.source_max
            if source_min is not None and source_max is not None and source_max > source_min:
                save_value = source_min + (source_max - source_min) * (value / 100.0)
        self._client.set_gateway_setting_value(self.entity_description.setting_key, save_value)
        self.async_write_ha_state()

    @property
    def device_info(self) -> dict:
        """Bind settings to gateway device."""
        return {
            "identifiers": self._client.get_gateway_identifiers(),
            "name": self._client.get_gateway_name(),
            "manufacturer": "Weiyu",
            "model": self._client.gateway_info.get("model", "Unknown"),
            "sw_version": self._client.gateway_info.get("version", ""),
        }
