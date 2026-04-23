"""Number platform for gateway-level protection settings."""

from __future__ import annotations

from dataclasses import dataclass

from homeassistant.components.number import NumberEntity, NumberEntityDescription, NumberMode
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import UnitOfElectricPotential, UnitOfTime
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .gateway import WeiyuGatewayClient


@dataclass(frozen=True, kw_only=True)
class WeiyuSettingNumberDescription(NumberEntityDescription):
    """Description for one gateway setting number entity."""

    setting_key: str


SETTING_NUMBERS: tuple[WeiyuSettingNumberDescription, ...] = (
    WeiyuSettingNumberDescription(
        key="report_cycle",
        name="网关上报周期",
        icon="mdi:timer-sync",
        native_min_value=5,
        native_max_value=3600,
        native_step=1,
        native_unit_of_measurement=UnitOfTime.SECONDS,
        mode=NumberMode.BOX,
        setting_key="report_cycle",
    ),
    WeiyuSettingNumberDescription(
        key="ov_fault",
        name="过压故障阈值",
        icon="mdi:flash-alert",
        native_min_value=200,
        native_max_value=320,
        native_step=1,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        mode=NumberMode.BOX,
        setting_key="ov_fault",
    ),
    WeiyuSettingNumberDescription(
        key="ov_alarm",
        name="过压告警阈值",
        icon="mdi:flash",
        native_min_value=200,
        native_max_value=320,
        native_step=1,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        mode=NumberMode.BOX,
        setting_key="ov_alarm",
    ),
    WeiyuSettingNumberDescription(
        key="ov_recover",
        name="过压恢复阈值",
        icon="mdi:flash-outline",
        native_min_value=180,
        native_max_value=300,
        native_step=1,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        mode=NumberMode.BOX,
        setting_key="ov_recover",
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
        native_min_value=80,
        native_max_value=240,
        native_step=1,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        mode=NumberMode.BOX,
        setting_key="uv_fault",
    ),
    WeiyuSettingNumberDescription(
        key="uv_alarm",
        name="欠压告警阈值",
        icon="mdi:flash",
        native_min_value=80,
        native_max_value=240,
        native_step=1,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        mode=NumberMode.BOX,
        setting_key="uv_alarm",
    ),
    WeiyuSettingNumberDescription(
        key="uv_recover",
        name="欠压恢复阈值",
        icon="mdi:flash-outline",
        native_min_value=80,
        native_max_value=260,
        native_step=1,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        mode=NumberMode.BOX,
        setting_key="uv_recover",
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
        native_min_value=50,
        native_max_value=200,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="overload_fault",
    ),
    WeiyuSettingNumberDescription(
        key="overload_alarm",
        name="过载告警阈值",
        icon="mdi:gauge-low",
        native_min_value=40,
        native_max_value=180,
        native_step=1,
        native_unit_of_measurement="%",
        mode=NumberMode.BOX,
        setting_key="overload_alarm",
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
        native_min_value=1,
        native_max_value=500,
        native_step=1,
        native_unit_of_measurement="mA",
        mode=NumberMode.BOX,
        setting_key="leak_fault",
    ),
    WeiyuSettingNumberDescription(
        key="leak_alarm",
        name="漏电告警阈值",
        icon="mdi:current-dc",
        native_min_value=1,
        native_max_value=500,
        native_step=1,
        native_unit_of_measurement="mA",
        mode=NumberMode.BOX,
        setting_key="leak_alarm",
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
        return self._client.get_gateway_setting_value(self.entity_description.setting_key)

    async def async_set_native_value(self, value: float) -> None:
        """Store value locally, waiting for apply button to push to devices."""
        self._client.set_gateway_setting_value(self.entity_description.setting_key, value)
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
