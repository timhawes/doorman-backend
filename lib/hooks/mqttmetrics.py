import logging
import queue
import threading
import time

import paho.mqtt.client as mqtt

from .base import BaseHook


class MqttThread(threading.Thread):
    def on_connect(self, *args, **kwargs):
        pass

    def on_message(self, *args, **kwargs):
        pass

    def run(self):
        while True:
            try:
                m = mqtt.Client()
                m.on_connect = self.on_connect
                m.on_message = self.on_message
                m.connect(self.mqtt_host, self.mqtt_port)
                m.loop_start()
                while True:
                    topic, payload, retain = self.mqtt_queue.get()
                    m.publish(
                        topic,
                        payload,
                        retain=retain,
                    )
            except Exception as e:
                logging.exception("Exception in MqttThread")
                time.sleep(1)


class MqttMetrics(BaseHook):
    def __init__(self, host, port=1883, prefix=""):
        self.mqtt_prefix = prefix
        self.mqtt_queue = queue.Queue()
        self.state_cache = {}

        self.mqtt_thread = MqttThread()
        self.mqtt_thread.mqtt_host = host
        self.mqtt_thread.mqtt_port = port
        self.mqtt_thread.mqtt_queue = self.mqtt_queue
        self.mqtt_thread.daemon = True
        self.mqtt_thread.start()

    async def log_metric(self, deviceid, key, value):
        topic = f"{self.mqtt_prefix}{deviceid}/{key}"
        self.mqtt_queue.put((topic, value, True))

    async def log_state(self, deviceid, key, value):
        topic = f"{self.mqtt_prefix}{deviceid}/{key}"
        old_value = self.state_cache.get(topic)
        if old_value != value:
            self.state_cache[topic] = value
            self.mqtt_queue.put((topic, value, True))
