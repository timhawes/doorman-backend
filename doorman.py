import asyncio
import base64
import logging
import random
import time

from clientbase import Client, ClientFactory, is_uid


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


class Door(Client):
    async def main_task(self):
        logging.debug("main_task() started")

        await self.set_states({"status": "online"})

        await self.send_message({"cmd": "state_query"})
        last_statistics = time.time() - random.randint(0, 45)

        while True:
            await self.loop()
            if time.time() - last_statistics > 60:
                await self.send_message({"cmd": "state_query"})
                await self.send_message({"cmd": "metrics_query"})
                last_statistics = time.time()
            await asyncio.sleep(5)

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
                anon = await self.factory.tokendb.is_anonymous(message["user"])
                if anon:
                    states["user"] = "anonymous"
                else:
                    states["user"] = message["user"]
            else:
                states["user"] = "unknown"

        await self.set_states(states, timestamp=message.pop("time", None))
        await self.set_metrics(metrics, timestamp=message.pop("time", None))


class DoorFactory(ClientFactory):
    def __init__(self, hooks, tokendb):
        self.hooks = hooks
        self.tokendb = tokendb
        super().__init__()

    async def client_from_auth(self, clientid, password, address=None):
        if clientid.startswith("doorman-"):
            clientid = clientid[8:]
        config = await self.hooks.auth_device(clientid, password)
        if config:
            client = Door(
                clientid, factory=self, config=config, hooks=self.hooks, address=address
            )
            self.clients_by_id[clientid] = client
            self.clients_by_slug[client.slug] = client
            return client
        else:
            logging.info(f"client {clientid} auth failed (address={address})")
        return None
