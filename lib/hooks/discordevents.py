import json
import logging
import queue
import threading
import time

import disnake

from .base import BaseHook


class DiscordThread(threading.Thread):
    def run(self):
        self.event_queue.put(
            {"event": "backend_start", "device": "*backend*", "time": time.time()}
        )
        webhook = disnake.SyncWebhook.from_url(self.webhook_url)
        while True:
            event = self.event_queue.get()
            event2 = event.copy()
            for k in ["time", "millis", "clientid", "device", "event"]:
                if k in event2:
                    del event2[k]
            timestamp = time.strftime("%H:%M:%SZ", time.gmtime(event["time"]))
            remaining = json.dumps(event2, sort_keys=True) if event2 else ""
            message = f"{timestamp} {event['device']} {event['event']} {remaining}"
            if "all" in self.discord_events or event["event"] in self.discord_events:
                webhook.send(message)


class DiscordEvents(BaseHook):
    def __init__(self, webhook_url, discord_events=["all"]):
        self.event_queue = queue.Queue()

        self.discord_thread = DiscordThread()
        self.discord_thread.webhook_url = webhook_url
        self.discord_thread.discord_events = discord_events
        self.discord_thread.event_queue = self.event_queue
        self.discord_thread.daemon = True
        self.discord_thread.start()

    async def log_event(self, message):
        logging.debug(f"DiscordEvents: queuing event {message}")
        self.event_queue.put(message)
