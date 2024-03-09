import json
import logging
import queue
import threading
import time

import apprise

from .base import BaseHook


class AppriseThread(threading.Thread):
    def run(self):
        self.event_queue.put(
            ("*backend*", "*backend*", {"event": "backend_start"}, time.time())
        )
        apobj = apprise.Apprise()
        for url in self.apprise_urls:
            apobj.add(url)
        while True:
            deviceid, devicename, event, timestamp = self.event_queue.get()
            event2 = event.copy()
            for k in ["time", "millis", "clientid", "device", "event"]:
                if k in event2:
                    del event2[k]
            timestamp_text = time.strftime("%H:%M:%SZ", time.gmtime(timestamp))
            remaining = json.dumps(event2, sort_keys=True) if event2 else ""
            message = f"{timestamp_text} {devicename} {event['event']} {remaining}"
            if "all" in self.apprise_events or event["event"] in self.apprise_events:
                apobj.notify(body=message)


class AppriseEvents(BaseHook):
    def __init__(self, apprise_urls, apprise_events=["all"]):
        self.event_queue = queue.Queue()

        self.apprise_thread = AppriseThread()
        self.apprise_thread.apprise_urls = apprise_urls
        self.apprise_thread.apprise_events = apprise_events
        self.apprise_thread.event_queue = self.event_queue
        self.apprise_thread.daemon = True
        self.apprise_thread.start()

    async def log_event(self, deviceid, devicename, message, *, timestamp=None):
        logging.debug(f"AppriseEvents: queuing event {deviceid} {devicename} {message}")
        self.event_queue.put((deviceid, devicename, message, timestamp))
