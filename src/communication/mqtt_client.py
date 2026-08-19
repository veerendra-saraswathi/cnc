"""
Lightweight MQTT client wrapper for the Communication Layer.
Compatible with standard Mosquitto (MQTT v3.1.1).
"""

from typing import Callable, Optional
import json
from loguru import logger

try:
    import paho.mqtt.client as mqtt
except ImportError:
    mqtt = None
    logger.warning("paho-mqtt not installed. MQTT features disabled.")


class MQTTClient:
    def __init__(
        self,
        host: str = "localhost",
        port: int = 1883,
        client_id: str = "cnc-platform",
        username: Optional[str] = None,
        password: Optional[str] = None,
    ):
        self.host = host
        self.port = port
        self.client_id = client_id
        self._client = None
        self._on_message_callbacks = []

        if mqtt is None:
            return

        # Use MQTT v3.1.1 for maximum compatibility with simple Mosquitto
        self._client = mqtt.Client(
            client_id=client_id,
            protocol=mqtt.MQTTv311,
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2
        )

        if username and password:
            self._client.username_pw_set(username, password)

        self._client.on_connect = self._on_connect
        self._client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason_code, properties=None):
        logger.info(f"MQTT connected to {self.host}:{self.port} (rc={reason_code})")

    def _on_message(self, client, userdata, msg):
        try:
            payload = json.loads(msg.payload.decode())
        except Exception:
            payload = msg.payload.decode()
        for cb in self._on_message_callbacks:
            cb(msg.topic, payload)

    def connect(self):
        if self._client is None:
            logger.error("paho-mqtt not available")
            return
        self._client.connect(self.host, self.port, keepalive=60)
        self._client.loop_start()

    def disconnect(self):
        if self._client:
            self._client.loop_stop()
            self._client.disconnect()

    def publish(self, topic: str, payload: dict, qos: int = 0):
        if self._client is None:
            return
        result = self._client.publish(topic, json.dumps(payload), qos=qos)
        result.wait_for_publish(timeout=2)

    def subscribe(self, topic: str, callback: Callable):
        if self._client is None:
            return
        self._on_message_callbacks.append(callback)
        self._client.subscribe(topic)

    def subscribe_telemetry(self, callback: Callable):
        self.subscribe("assets/+/telemetry", callback)

    def publish_telemetry(self, asset_id: str, telemetry: dict):
        self.publish(f"assets/{asset_id}/telemetry", telemetry)

    def publish_command(self, asset_id: str, command: dict):
        self.publish(f"assets/{asset_id}/command", command, qos=1)
        