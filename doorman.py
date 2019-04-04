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


class Door(Client):

    def __init__(self, clientid, factory, address):
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

        # mqtt cache
        self.mqtt_cache = {}

    async def reload_settings(self, create=False):
        self.slug = self.doordb.get_value(self.clientid, 'slug', self.clientid)
        self.token_groups = self.doordb.get_value(self.clientid, 'groups')
        config_json = json.dumps(self.doordb.get_config(self.clientid)).encode()
        token_data = self.tokendb.token_database_v2(
            groups=self.token_groups,
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

        last_keepalive = time.time()
        last_file_check = 0

        await self.send_message({'cmd': 'state_query'})
        last_statistics = time.time() - random.randint(15, 60)

        while True:
            if time.time() - last_keepalive > 30:
                logging.debug('sending keepalive ping')
                await self.send_message({'cmd': 'ping', 'timestamp': time.time()})
                last_keepalive = time.time()
            if time.time() - last_statistics > 60:
                await self.send_message({'cmd': 'state_query'})
                await self.send_message({'cmd': 'metrics_query'})
                last_statistics = time.time()
            await asyncio.sleep(5)

    async def handle_cmd_state_info(self, message):
        mapping = {
           'card_enable': ['{}/var/cardEnabled', lambda x: str(x).lower(), True],
           'exit_enable': ['{}/var/exitEnabled', lambda x: str(x).lower(), True],
           'snib_enable': ['{}/var/snibEnabled', lambda x: str(x).lower(), True],
           'card_active': ['{}/var/cardUnlockActive', lambda x: str(x).lower(), True],
           'exit_active': ['{}/var/exitUnlockActive', lambda x: str(x).lower(), True],
           'snib_active': ['{}/var/snibUnlockActive', lambda x: str(x).lower(), True],
           'remote_active': ['{}/var/remoteUnlockActive', lambda x: str(x).lower(), True],
           'unlock': ['{}/var/unlocked', lambda x: str(x).lower(), True],
           'door': ['{}/var/doorState', str, True],
           'power': ['{}/var/powerState', str, True],
           'voltage': ['{}/var/batteryVoltage', str, True],
        }
        
        for attribute in mapping.keys():
            if attribute in message:
                topic = mapping[attribute][0].format(self.slug)
                payload = mapping[attribute][1](message[attribute])
                retain = mapping[attribute][2]
                await self.send_mqtt(topic, payload, retain)


class DoorFactory(ClientFactory):
    
    def __init__(self, doordb, tokendb):
        self.doordb = doordb
        self.tokendb = tokendb
        super(DoorFactory, self).__init__()

    def client_from_auth(self, clientid, password, address=None):
        logging.debug('authing client {} with password {}'.format(clientid, password))
        if self.doordb.authenticate(clientid, password):
            client = Door(clientid, factory=self, address=address)
            self.clients_by_id[clientid] = client
            self.clients_by_slug[client.slug] = client
            return client
        else:
            logging.info('client {} auth failed (address={})'.format(clientid, address))
        return None
