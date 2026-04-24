"""Constants for Weiyu Gateway integration."""

from homeassistant.const import Platform

DOMAIN = "weiyu_gateway"

CONF_GATEWAY_HOST = "gateway_host"
CONF_GATEWAY_MODEL = "gateway_model"
CONF_LISTEN_IP = "listen_ip"
CONF_LISTEN_PORT = "listen_port"

DEFAULT_LISTEN_PORT = 23456

PLATFORMS: list[Platform] = [Platform.SWITCH, Platform.SENSOR, Platform.BUTTON]
