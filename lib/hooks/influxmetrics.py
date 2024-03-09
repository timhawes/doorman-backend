import logging
import queue
import threading
import time

import requests

from .base import BaseHook

MAX_SEND_INTERVAL = 5
MAX_SEND_METRICS = 100


logger = logging.getLogger()


class InfluxThread(threading.Thread):
    def run(self):
        while True:
            data = []
            last_send = time.time()
            while True:
                try:
                    m = self.queue.get(timeout=1)
                    data.append(m)
                except queue.Empty:
                    pass
                if (
                    len(data) > MAX_SEND_METRICS
                    or time.time() - last_send > MAX_SEND_INTERVAL
                ):
                    data_string = "\n".join(data)
                    for url in self.urls:
                        try:
                            r = requests.post(
                                url,
                                data=data_string,
                                headers={"Content-Type": "application/octet-stream"},
                            )
                            if not r.ok:
                                logging.error(
                                    "Error while sending {} to {}, response {}".format(
                                        data_string, url, r.content
                                    )
                                )
                        except Exception:
                            logging.exception(
                                "Exception while sending {} to {}".format(
                                    data_string, url
                                )
                            )
                    last_send = time.time()
                    data = []


def influx_line(measurement, tags={}, fields={}, timestamp=None):
    if timestamp is None:
        timestamp = time.time()
    t = []
    for k, v in tags.items():
        t.append(f"{k}={v}")
    f = []
    for k, v in fields.items():
        if isinstance(v, (bool, float)):
            f.append(f"{k}={v}")
        elif isinstance(v, int):
            f.append(f"{k}={v}i")
        elif isinstance(v, str):
            v2 = v.replace('"', '"')
            f.append(f'{k}="{v2}"')
        else:
            logger.debug(f"ignoring field {k} of unknown type {type(v)} value {v}")
    if t:
        return f"{measurement},{','.join(t)} {','.join(f)} {int(timestamp * 1_000_000_000)}"
    else:
        return f"{measurement} {','.join(f)} {int(timestamp * 1_000_000_000)}"


class InfluxMetrics(BaseHook):
    def __init__(
        self,
        urls,
        metrics_measurement=None,
        states_measurement=None,
        events_measurement=None,
    ):
        self.urls = urls
        self.metrics_measurement = metrics_measurement
        self.states_measurement = states_measurement
        self.events_measurement = events_measurement
        self.queue = queue.Queue()

        self.thread = InfluxThread()
        self.thread.urls = self.urls
        self.thread.queue = self.queue
        self.thread.daemon = True
        self.thread.start()

    async def log_event(self, deviceid, devicename, message, *, timestamp=None):
        if not self.events_measurement:
            return
        tags = {}
        if deviceid:
            tags["id"] = deviceid
        if devicename:
            tags["name"] = devicename
        line = influx_line(self.events_measurement, tags, message, timestamp)
        self.queue.put(line)

    async def log_metrics(self, deviceid, devicename, metrics, *, timestamp=None):
        if not self.metrics_measurement:
            return
        tags = {}
        if deviceid:
            tags["id"] = deviceid
        if devicename:
            tags["name"] = devicename
        line = influx_line(self.metrics_measurement, tags, metrics, timestamp)
        self.queue.put(line)

    async def log_states(self, deviceid, devicename, states, *, timestamp=None):
        if not self.states_measurement:
            return
        tags = {}
        if deviceid:
            tags["id"] = deviceid
        if devicename:
            tags["name"] = devicename
        line = influx_line(self.states_measurement, tags, states, timestamp)
        self.queue.put(line)
