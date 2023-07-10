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

        await self.send_mqtt("status", "online", retain=True, dedup=False)

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
        mapping = {
            # field_name: [mqtt_topic, str_function, retain, dedup, is_state]
            "card_enable": ["card_enable", lambda x: str(x).lower(), True, True, True],
            "exit_enable": ["exit_enable", lambda x: str(x).lower(), True, True, True],
            "snib_enable": ["snib_enable", lambda x: str(x).lower(), True, True, True],
            "card_active": ["card_active", lambda x: str(x).lower(), True, True, True],
            "exit_active": ["exit_active", lambda x: str(x).lower(), True, True, True],
            "snib_active": ["snib_active", lambda x: str(x).lower(), True, True, True],
            "remote_active": [
                "remote_active",
                lambda x: str(x).lower(),
                True,
                True,
                True,
            ],
            "unlock": ["unlock", lambda x: str(x).lower(), True, True, True],
            "door": ["door", str, True, True, True],
            "power": ["power", str, True, True, True],
            "voltage": [
                "voltage",
                str,
                True,
                False,
                False,
            ],  # no dedup for numerical/graphable values
        }

        new_states = {}

        for attribute in mapping.keys():
            if attribute in message:
                topic = mapping[attribute][0]
                payload = mapping[attribute][1](message[attribute])
                retain = mapping[attribute][2]
                dedup = mapping[attribute][3]
                await self.send_mqtt(topic, payload, retain=retain, dedup=dedup)
                if mapping[attribute][4]:
                    new_states[attribute] = message[attribute]

        if "unlock" in message and "door" in message:
            if message["door"] == "closed" and message["unlock"] is False:
                await self.send_mqtt(
                    "sensor/{}/door".format(self.slug),
                    "secure",
                    retain=True,
                    dedup=True,
                    ignore_prefix=True,
                )
            elif message["door"] == "closed":
                await self.send_mqtt(
                    "sensor/{}/door".format(self.slug),
                    "closed",
                    retain=True,
                    dedup=True,
                    ignore_prefix=True,
                )
            else:
                await self.send_mqtt(
                    "sensor/{}/door".format(self.slug),
                    "open",
                    retain=True,
                    dedup=True,
                    ignore_prefix=True,
                )

        if "user" in message:
            if message["user"] == "":
                await self.send_mqtt("user", "", retain=True, dedup=True)
                new_states["user"] = None
            elif not is_uid(message["user"]):
                anon = await self.factory.tokendb.is_anonymous(message["user"])
                if anon:
                    await self.send_mqtt("user", "anonymous", retain=True, dedup=True)
                else:
                    await self.send_mqtt(
                        "user", message["user"], retain=True, dedup=True
                    )
                new_states["user"] = message["user"]
            else:
                await self.send_mqtt("user", "unknown", retain=True, dedup=True)
                new_states["user"] = message["user"]

        await self.set_state(new_states)


class DoorFactory(ClientFactory):
    def __init__(self, doordb, tokendb):
        self.clientdb = doordb
        self.tokendb = tokendb
        super(DoorFactory, self).__init__()

    def client_from_auth(self, clientid, password, address=None):
        if clientid.startswith("doorman-"):
            clientid = clientid[8:]
        if self.clientdb.authenticate(clientid, password):
            client = Door(
                clientid, factory=self, address=address, mqtt_prefix=self.mqtt_prefix
            )
            self.clients_by_id[clientid] = client
            self.clients_by_slug[client.slug] = client
            return client
        else:
            logging.info("client {} auth failed (address={})".format(clientid, address))
        return None
