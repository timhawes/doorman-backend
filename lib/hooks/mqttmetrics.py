import logging
import queue
import threading
import time

import paho.mqtt.client as mqtt

from .base import BaseHook

IGNORE_METRICS = []
IGNORE_STATES = []


class MqttThread(threading.Thread):
    def run(self):
        while True:
            try:
                m = mqtt.Client()
                if self.mqtt_tls:
                    m.tls_set()
                if self.mqtt_username:
                    m.username_pw_set(self.mqtt_username, self.mqtt_password)
                m.connect(self.mqtt_host, self.mqtt_port)
                m.loop_start()
                while True:
                    topic, payload, retain = self.mqtt_queue.get()
                    if payload is None:
                        payload = ""
                    m.publish(
                        topic,
                        str(payload),
                        retain=retain,
                    )
            except Exception:
                logging.exception("Exception in MqttThread")
                time.sleep(1)


class MqttMetrics(BaseHook):
    def __init__(
        self, host, port=1883, tls=False, username=None, password=None, prefix="test/"
    ):
        self.mqtt_prefix = prefix
        self.mqtt_queue = queue.Queue()
        self.state_cache = {}

        self.mqtt_thread = MqttThread()
        self.mqtt_thread.mqtt_host = host
        self.mqtt_thread.mqtt_port = port
        self.mqtt_thread.mqtt_tls = tls
        self.mqtt_thread.mqtt_username = username
        self.mqtt_thread.mqtt_password = password
        self.mqtt_thread.mqtt_queue = self.mqtt_queue
        self.mqtt_thread.daemon = True
        self.mqtt_thread.start()

    def metric_topic(self, deviceid, devicename, key):
        return f"{self.mqtt_prefix}{devicename}/metrics/{key}"

    def state_topic(self, deviceid, devicename, key):
        return f"{self.mqtt_prefix}{devicename}/{key}"

    async def log_metrics(self, deviceid, devicename, metrics, *, timestamp=None):
        for key, value in metrics.items():
            if key in IGNORE_METRICS:
                continue
            topic = self.metric_topic(deviceid, devicename, key)
            self.mqtt_queue.put((topic, value, True))

    async def log_states(self, deviceid, devicename, states, *, timestamp=None):
        for key, value in states.items():
            if key in IGNORE_STATES:
                continue
            cache_key = f"{deviceid}:{key}"
            old_value = self.state_cache.get(cache_key)
            if old_value != value:
                topic = self.state_topic(deviceid, devicename, key)
                self.state_cache[cache_key] = value
                self.mqtt_queue.put((topic, value, True))
