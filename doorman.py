import asyncio
import base64
import json
import logging
import random
import time

from clientbase import Client, ClientFactory, SyncableStringFile


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


def friendly_age(t):
    if t > 86400:
        return '%dd ago' % (t/86400)
    elif t > 3600:
        return '%dh ago' % (t/3600)
    elif t > 60:
        return '%dm ago' % (t/60)
    else:
        return 'just now'


def is_uid(uid):
    if len(uid) not in [8, 14]:
        return False
    for c in uid.lower():
        if c not in '0123456789abcdef':
            return False
    return True


class Door(Client):

    def __init__(self, clientid, factory, address, mqtt_prefix='undefined/'):
        self.clientid = clientid
        self.factory = factory
        self.doordb = factory.doordb
        self.tokendb = factory.tokendb
        self.address = address

        # initial value for slug
        self.slug = self.doordb.get_value(self.clientid, 'slug', self.clientid)

        # asyncio network streams, will be populated by the factory
        self.reader = None
        self.writer = None

        # callback for sending an outbound message object
        # will be populated by the asyncio protocol handler
        self.write_callback = None

        # remote state for syncing
        self.remote_files = {}
        self.remote_firmware_active = None
        self.remote_firmware_pending = None

        # local specification for syncing
        self.files = {}
        self.firmware = None

        # current state of the sync
        self.firmware_complete = asyncio.Event()
        self.firmware_complete.clear()
        self.firmware_pending_reboot = False

        # mqtt
        self.mqtt_prefix = mqtt_prefix
        self.mqtt_cache = {}

        # metrics
        self.metrics = {}

    async def reload_settings(self, create=False):
        self.slug = self.doordb.get_value(self.clientid, 'slug', self.clientid)
        self.token_groups = self.doordb.get_value(self.clientid, 'groups')
        self.token_exclude_groups = self.doordb.get_value(self.clientid, 'exclude_groups')
        config_json = json.dumps(self.doordb.get_config(self.clientid)).encode()
        token_data = self.tokendb.token_database_v2(
            groups=self.token_groups,
            exclude_groups=self.token_exclude_groups,
            salt=self.doordb.get_value(self.clientid, 'token_salt').encode())
        if create:
            self.files['config.json'] = SyncableStringFile('config.json', config_json)
            self.files['tokens.dat'] = SyncableStringFile('tokens.dat', token_data)
        else:
            self.files['config.json'].update(config_json)
            self.files['tokens.dat'].update(token_data)
        firmware_filename = self.doordb.get_value(self.clientid, 'firmware')
        if firmware_filename:
            self.firmware = await self.get_firmware(firmware_filename)

    async def main_task(self):
        logging.debug("main_task() started")

        self.last_pong_received = time.time()
        last_ping_sent = time.time()
        
        await self.send_mqtt('status', 'online', retain=True, dedup=False)

        await self.send_message({'cmd': 'state_query'})
        last_statistics = time.time() - random.randint(15, 60)

        while True:
            if time.time() - last_ping_sent > 30:
                logging.debug('sending keepalive ping')
                await self.send_message({'cmd': 'ping', 'timestamp': str(time.time())})
                last_ping_sent = time.time()
            if time.time() - self.last_pong_received > 65:
                self.log('no pong received for >65 seconds')
                raise Exception('no pong received for >65 seconds')
            if time.time() - last_statistics > 60:
                await self.send_message({'cmd': 'state_query'})
                await self.send_message({'cmd': 'metrics_query'})
                await self.send_message({'cmd': 'net_metrics_query'})
                last_statistics = time.time()
            await asyncio.sleep(5)

    async def handle_cmd_state_info(self, message):
        mapping = {
           'card_enable': ['card_enable', lambda x: str(x).lower(), True, True],
           'exit_enable': ['exit_enable', lambda x: str(x).lower(), True, True],
           'snib_enable': ['snib_enable', lambda x: str(x).lower(), True, True],
           'card_active': ['card_active', lambda x: str(x).lower(), True, True],
           'exit_active': ['exit_active', lambda x: str(x).lower(), True, True],
           'snib_active': ['snib_active', lambda x: str(x).lower(), True, True],
           'remote_active': ['remote_active', lambda x: str(x).lower(), True, True],
           'unlock': ['unlock', lambda x: str(x).lower(), True, True],
           'door': ['door', str, True, True],
           'power': ['power', str, True, True],
           'voltage': ['voltage', str, True, False], # no dedup for numerical/graphable values
        }
        
        for attribute in mapping.keys():
            if attribute in message:
                topic = mapping[attribute][0]
                payload = mapping[attribute][1](message[attribute])
                retain = mapping[attribute][2]
                dedup = mapping[attribute][3]
                await self.send_mqtt(topic, payload, retain=retain, dedup=dedup)

        if 'unlock' in message and 'door' in message:
            if message['door'] == 'closed' and message['unlock'] is False:
                await self.send_mqtt('sensor/{}/door'.format(self.slug), 'secure', retain=True, dedup=True, ignore_prefix=True)
            elif message['door'] == 'closed':
                await self.send_mqtt('sensor/{}/door'.format(self.slug), 'closed', retain=True, dedup=True, ignore_prefix=True)
            else:
                await self.send_mqtt('sensor/{}/door'.format(self.slug), 'open', retain=True, dedup=True, ignore_prefix=True)

        if 'user' in message:
            if message['user'] == '':
                await self.send_mqtt('user', '', retain=True, dedup=True)
            elif not is_uid(message['user']):
                anon = await self.factory.tokendb.is_anonymous(message['user'])
                if anon:
                    await self.send_mqtt('user', 'anonymous', retain=True, dedup=True)
                else:
                    await self.send_mqtt('user', message['user'], retain=True, dedup=True)
            else:
                await self.send_mqtt('user', 'unknown', retain=True, dedup=True)

    def status_json(self):
        return {
            'clientid': self.clientid,
            'address': self.address,
            'slug': self.slug,
        }


class DoorFactory(ClientFactory):
    
    def __init__(self, doordb, tokendb):
        self.doordb = doordb
        self.tokendb = tokendb
        super(DoorFactory, self).__init__()

    def client_from_auth(self, clientid, password, address=None):
        if self.doordb.authenticate(clientid, password):
            client = Door(clientid, factory=self, address=address, mqtt_prefix=self.mqtt_prefix)
            self.clients_by_id[clientid] = client
            self.clients_by_slug[client.slug] = client
            return client
        else:
            logging.info('client {} auth failed (address={})'.format(clientid, address))
        return None
