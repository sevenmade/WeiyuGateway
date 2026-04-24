"""Sensor platform for Weiyu Gateway telemetry."""

from __future__ import annotations

from dataclasses import dataclass

from homeassistant.components.sensor import SensorDeviceClass, SensorEntity, SensorEntityDescription, SensorStateClass
from homeassistant.config_entries import ConfigEntry
from homeassistant.const import (
    PERCENTAGE,
    UnitOfElectricCurrent,
    UnitOfElectricPotential,
    UnitOfEnergy,
    UnitOfFrequency,
    UnitOfPower,
    UnitOfTemperature,
)
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.entity_platform import AddEntitiesCallback

from .const import DOMAIN
from .gateway import WeiyuGatewayClient


@dataclass(frozen=True, kw_only=True)
class WeiyuSensorDescription(SensorEntityDescription):
    """Description of a Weiyu sensor."""

    source_key: str
    divisor: int


SENSOR_TYPES: tuple[WeiyuSensorDescription, ...] = (
    WeiyuSensorDescription(
        key="voltage",
        name="电压",
        source_key="voltage",
        divisor=100,
        icon="mdi:flash",
        device_class=SensorDeviceClass.VOLTAGE,
        native_unit_of_measurement=UnitOfElectricPotential.VOLT,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="current",
        name="电流",
        source_key="electric",
        divisor=1000,
        icon="mdi:current-ac",
        device_class=SensorDeviceClass.CURRENT,
        native_unit_of_measurement=UnitOfElectricCurrent.AMPERE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="power",
        name="有功功率",
        source_key="powerrate",
        divisor=100,
        icon="mdi:lightning-bolt",
        device_class=SensorDeviceClass.POWER,
        native_unit_of_measurement=UnitOfPower.WATT,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="energy",
        name="电能",
        source_key="power",
        divisor=100,
        icon="mdi:meter-electric",
        device_class=SensorDeviceClass.ENERGY,
        native_unit_of_measurement=UnitOfEnergy.KILO_WATT_HOUR,
        state_class=SensorStateClass.TOTAL_INCREASING,
    ),
    WeiyuSensorDescription(
        key="frequency",
        name="频率",
        source_key="frequency",
        divisor=100,
        icon="mdi:sine-wave",
        device_class=SensorDeviceClass.FREQUENCY,
        native_unit_of_measurement=UnitOfFrequency.HERTZ,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="power_factor",
        name="功率因数",
        source_key="powerfactor",
        divisor=1000,
        icon="mdi:percent",
        native_unit_of_measurement=PERCENTAGE,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="line_temperature",
        name="火线温度",
        source_key="ltemp",
        divisor=100,
        icon="mdi:thermometer-lines",
        device_class=SensorDeviceClass.TEMPERATURE,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="neutral_temperature",
        name="零线温度",
        source_key="ntemp",
        divisor=100,
        icon="mdi:thermometer",
        device_class=SensorDeviceClass.TEMPERATURE,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="chip_temperature",
        name="芯片温度",
        source_key="ctemp",
        divisor=100,
        icon="mdi:memory",
        device_class=SensorDeviceClass.TEMPERATURE,
        native_unit_of_measurement=UnitOfTemperature.CELSIUS,
        state_class=SensorStateClass.MEASUREMENT,
    ),
    WeiyuSensorDescription(
        key="leakage_current",
        name="漏电流",
        source_key="leakagecurrent",
        divisor=100,
        icon="mdi:current-dc",
        native_unit_of_measurement="mA",
        state_class=SensorStateClass.MEASUREMENT,
    ),
)


async def async_setup_entry(
    hass: HomeAssistant, entry: ConfigEntry, async_add_entities: AddEntitiesCallback
) -> None:
    """Set up sensor entities from a config entry."""
    client: WeiyuGatewayClient = hass.data[DOMAIN][entry.entry_id]
    known: set[tuple[str, str]] = set()
    gateway_entity = WeiyuGatewayStatusSensor(client, entry.entry_id)
    gateway_activity = WeiyuGatewayActivitySensor(client, entry.entry_id)
    entities: list[SensorEntity] = [gateway_entity, gateway_activity]
    async_add_entities([gateway_entity, gateway_activity])

    @callback
    def _sync_entities(_: set[str]) -> None:
        new_entities: list[SensorEntity] = []
        for devno in client.devices:
            status_key = (devno, "operating_status")
            if status_key not in known:
                known.add(status_key)
                status_sensor = WeiyuOperatingStatusSensor(client, devno)
                entities.append(status_sensor)
                new_entities.append(status_sensor)

            mode_key = (devno, "work_mode")
            if mode_key not in known:
                known.add(mode_key)
                mode_sensor = WeiyuWorkModeSensor(client, devno)
                entities.append(mode_sensor)
                new_entities.append(mode_sensor)

            for desc in SENSOR_TYPES:
                if desc.key == "leakage_current" and not client.is_leakage_protection_device(devno):
                    continue
                if desc.key == "neutral_temperature" and not client.is_two_p_device(devno):
                    continue
                key = (devno, desc.key)
                if key in known:
                    continue
                known.add(key)
                sensor = WeiyuSensor(client, devno, desc)
                entities.append(sensor)
                new_entities.append(sensor)

        if new_entities:
            async_add_entities(new_entities)

        for entity in entities:
            if entity.hass is not None:
                entity.async_write_ha_state()

    unsub = client.add_listener(_sync_entities)
    entry.async_on_unload(unsub)
    _sync_entities(set())


class WeiyuSensor(SensorEntity):
    """One telemetry sensor for one breaker."""

    _attr_has_entity_name = True

    def __init__(self, client: WeiyuGatewayClient, devno: str, description: WeiyuSensorDescription) -> None:
        self.entity_description = description
        self._client = client
        self._devno = devno
        self._attr_unique_id = f"weiyu_{devno}_{description.key}"
        self._attr_name = description.name

    @property
    def native_value(self) -> float | None:
        """Return sensor state."""
        if self.entity_description.key == "leakage_current" and not self._client.is_leakage_protection_device(self._devno):
            return None
        if self.entity_description.key == "neutral_temperature" and not self._client.is_two_p_device(self._devno):
            return None
        device = self._client.get_device_data(self._devno)
        raw = device.get("raw", {})
        value = raw.get(self.entity_description.source_key)
        if value is None:
            return None
        try:
            numeric = float(value) / self.entity_description.divisor
        except (TypeError, ValueError):
            return None

        if self.entity_description.key == "power_factor":
            return round(numeric * 100, 2)
        return round(numeric, 3)

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


class WeiyuOperatingStatusSensor(SensorEntity):
    """Human-friendly operating status sensor for one breaker."""

    _attr_has_entity_name = True
    _attr_name = "运行状态"
    _attr_icon = "mdi:list-status"

    def __init__(self, client: WeiyuGatewayClient, devno: str) -> None:
        self._client = client
        self._devno = devno
        self._attr_unique_id = f"weiyu_{devno}_operating_status"

    @property
    def native_value(self) -> str:
        """Return merged operating status text."""
        return self._client.get_operating_status_text(self._devno)

    @property
    def device_info(self) -> dict:
        """Bind entity to breaker device."""
        data = self._client.get_device_data(self._devno)
        return {
            "identifiers": {("weiyu_gateway", self._devno)},
            "name": self._client.get_device_name(self._devno),
            "manufacturer": "微羽智能",
            "model": data.get("model", "未知型号"),
            "sw_version": data.get("meta", {}).get("version"),
            "via_device": next(iter(self._client.get_gateway_identifiers())),
        }


class WeiyuWorkModeSensor(SensorEntity):
    """Work mode sensor for one breaker."""

    _attr_has_entity_name = True
    _attr_name = "工作模式"
    _attr_icon = "mdi:cog-transfer"

    def __init__(self, client: WeiyuGatewayClient, devno: str) -> None:
        self._client = client
        self._devno = devno
        self._attr_unique_id = f"weiyu_{devno}_work_mode"

    @property
    def native_value(self) -> str:
        """Return work mode text from wmode field."""
        data = self._client.get_device_data(self._devno)
        raw = data.get("raw", {})
        wmode = int(raw.get("wmode", 0) or 0)
        return "手动" if wmode == 1 else "自动"

    @property
    def device_info(self) -> dict:
        """Bind entity to breaker device."""
        data = self._client.get_device_data(self._devno)
        return {
            "identifiers": {("weiyu_gateway", self._devno)},
            "name": self._client.get_device_name(self._devno),
            "manufacturer": "微羽智能",
            "model": data.get("model", "未知型号"),
            "sw_version": data.get("meta", {}).get("version"),
            "via_device": next(iter(self._client.get_gateway_identifiers())),
        }


class WeiyuGatewayStatusSensor(SensorEntity):
    """Gateway status entity to expose gateway device info."""

    _attr_has_entity_name = True
    _attr_name = "网关在线状态"
    _attr_icon = "mdi:router-wireless"

    def __init__(self, client: WeiyuGatewayClient, entry_id: str) -> None:
        self._client = client
        self._attr_unique_id = f"weiyu_gateway_{entry_id}_status"

    @property
    def native_value(self) -> str:
        """Return online/offline status."""
        connected = int(self._client.gateway_info.get("connected", 0))
        return "在线" if connected else "离线"

    @property
    def extra_state_attributes(self) -> dict:
        """Show gateway model/version/serial."""
        return {
            "型号": self._client.gateway_info.get("model", "Unknown"),
            "固件版本": self._client.gateway_info.get("version", ""),
            "网关编号": self._client.gateway_info.get("devno", ""),
            "网关IP": self._client.gateway_host,
        }

    @property
    def device_info(self) -> dict:
        """Create gateway device card with requested display name."""
        return {
            "identifiers": self._client.get_gateway_identifiers(),
            "name": self._client.get_gateway_name(),
            "manufacturer": "Weiyu",
            "model": self._client.gateway_info.get("model", "Unknown"),
            "sw_version": self._client.gateway_info.get("version", ""),
        }


class WeiyuGatewayActivitySensor(SensorEntity):
    """Gateway human-readable activity (e.g. scanning subdevices, reconnecting)."""

    _attr_has_entity_name = True
    _attr_name = "状态"
    _attr_icon = "mdi:state-machine"

    def __init__(self, client: WeiyuGatewayClient, entry_id: str) -> None:
        self._client = client
        self._attr_unique_id = f"weiyu_gateway_{entry_id}_activity"

    @property
    def native_value(self) -> str:
        """Return current gateway activity text."""
        return self._client.get_gateway_activity_text()

    @property
    def device_info(self) -> dict:
        """Bind entity to gateway device."""
        return {
            "identifiers": self._client.get_gateway_identifiers(),
            "name": self._client.get_gateway_name(),
            "manufacturer": "Weiyu",
            "model": self._client.gateway_info.get("model", "Unknown"),
            "sw_version": self._client.gateway_info.get("version", ""),
        }
