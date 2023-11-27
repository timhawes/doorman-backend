import asyncio
import base64
import random

from clientbase import CommonConnection, CommonManager, is_uid

import settings


def encode_tune(tune):
    """Pack a tune into the client's internal format.

    The input tune must be a Python list:

    [ [frequency, milliseconds], [frequency2, milliseconds2], ...]
    """

    output = []
    for note in tune:
        hz = note[0] & 0xFFFF
        ms = note[1] & 0xFFFF
        output.append(hz & 0xFF)
        output.append(hz >> 8)
        output.append(ms & 0xFF)
        output.append(ms >> 8)
    return base64.b64encode(bytes(output)).decode()


class DoorConnection(CommonConnection):
    client_strip_prefix = "doorman-"

    async def handle_post_auth(self):
        await super().handle_post_auth()
        self.create_task(self.door_task())

    async def door_task(self):
        await self.send_message({"cmd": "state_query"})
        await asyncio.sleep(
            random.randint(0, int(settings.METRICS_QUERY_INTERVAL * 0.25))
        )
        while True:
            await self.send_message({"cmd": "state_query"})
            await self.send_message({"cmd": "metrics_query"})
            await asyncio.sleep(settings.METRICS_QUERY_INTERVAL)

    async def handle_cmd_state_info(self, message):
        lower_state_names = [
            "card_enable",
            "exit_enable",
            "snib_enable",
            "card_active",
            "exit_active",
            "snib_active",
            "remote_active",
            "unlock",
        ]
        state_names = ["door", "power"]
        metric_names = ["voltage"]

        states = {}
        metrics = {}

        for topic in message.keys():
            if topic in lower_state_names:
                if isinstance(message[topic], str):
                    states[topic] = message[topic].lower()
                else:
                    states[topic] = message[topic]
            elif topic in state_names:
                states[topic] = message[topic]
            elif topic in metric_names:
                metrics[topic] = message[topic]

        if "unlock" in message and "door" in message:
            if message["door"] == "closed" and message["unlock"] is False:
                states["secure_state"] = "secure"
            elif message["door"] == "closed":
                states["secure_state"] = "closed"
            else:
                states["secure_state"] = "open"

        if "user" in message:
            if message["user"] == "":
                states["user"] = None
            elif not is_uid(message["user"]):
                anon = await self.manager.tokendb.is_anonymous(message["user"])
                if anon:
                    states["user"] = "anonymous"
                else:
                    states["user"] = message["user"]
            else:
                states["user"] = "unknown"

        await self.set_states(states, timestamp=message.pop("time", None))
        await self.set_metrics(metrics, timestamp=message.pop("time", None))


class DoorManager(CommonManager):
    connection_class = DoorConnection
