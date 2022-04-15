import asyncio
import base64
import binascii
import hashlib
import io
import json
import logging
import os
import queue
import random
import time


class BaseSyncableFile:
    def __init__(self):
        self.remote_md5 = None
        self.remote_size = None
        self.remote_time = 0
        self.metadata_ready = asyncio.Event()
        self.sync_complete = asyncio.Event()
    def get_handle(self):
        raise NotImplementedError
    def get_md5(self):
        raise NotImplementedError
    def get_size(self):
        raise NotImplementedError
    def needSync(self):
        #logging.debug('needSync remote={}/{} vs local={}/{}'.format(self.remote_md5, self.remote_size, self.md5, self.size))
        if self.remote_md5 == self.md5 and self.remote_size == self.size:
            self.sync_complete.set()
            return False
        else:
            self.sync_complete.clear()
            return True
    async def waitForSync(self):
        await self.sync_complete.wait()
    def needMetadata(self):
        if self.remote_time == 0 or time.time() - self.remote_time > 300:
            self.metadata_ready.clear()
            return True
        else:
            self.metadata_ready.set()
            return False
    async def waitForMetadata(self):
        await self.metadata_ready.wait()
    def remote(self, md5, size):
        logging.debug('recording remote metadata md5={} size={}'.format(md5, size))
        self.remote_md5 = md5
        self.remote_size = size
        self.remote_time = time.time()
        self.metadata_ready.set()
        self.needSync()
    def syncDone(self):
        self.sync_complete.set()


class SyncableStringFile(BaseSyncableFile):
    def __init__(self, filename, data):
        self.filename = filename
        self.data = None
        self.update(data)
        super(SyncableStringFile, self).__init__()
    def update(self, data=None):
        if data is not None:
            if data != self.data:
                self.data = data
                self.md5 = hashlib.md5(data).hexdigest()
                self.size = len(data)
                self.handle = None
                logging.debug('{} updated'.format(self.filename))
            else:
                logging.debug('{} unchanged'.format(self.filename))
    def get_handle(self):
        if self.handle is None:
            self.handle = io.BytesIO(self.data)
        return self.handle
    def check(self):
        pass
    def get_size(self):
        return self.size
    def get_md5(self):
        return self.md5


class SyncableDiskFile(BaseSyncableFile):
    def __init__(self, filename):
        self.filename = filename
        self.size = None
        self.md5 = None
        self.update(filename)
        super(SyncableDiskFile, self).__init__()
    def update(self, filename=None):
        if filename:
            logging.debug('{} replaced with {}'.format(self.filename, filename))
            self.filename = filename
        new_size = os.path.getsize(filename)
        new_md5 = hashlib.md5()
        for chunk in open(filename, 'rb'):
            new_md5.update(chunk)
        if new_size != self.size or new_md5.hexdigest() != self.md5:
            self.size = new_size
            self.md5 = new_md5.hexdigest()
            self.handle = None
            self.get_handle()
            logging.debug('{} updated'.format(self.filename))
        else:
            logging.debug('{} unchanged'.format(self.filename))
    def get_handle(self):
        if self.handle is None:
            self.handle = open(self.filename, 'rb')
        return self.handle
    def get_size(self):
        return self.size
    def get_md5(self):
        return self.md5


class Client:
    
    def __init__(self):
        pass

    def __str__(self):
        if self.writer:
            tags = [
                'slug={}'.format(self.slug),
                'clientid={}'.format(self.clientid),
                'peername={}:{}'.format(*self.writer.get_extra_info('peername')),
            ]
            cipher = self.writer.get_extra_info('cipher')
            if cipher:
                tags.append('cipher={}:{}:{}'.format(*cipher))
            return "<{} {}>".format(self.__class__.__name__, ' '.join(tags))
        else:
            return "<{} clientid={}>".format(self.__class__.__name__, self.clientid)

    def loggable_message(self, message):
        output = message.copy()
        if output.get('cmd') in ['file_data', 'firmware_data']:
            output['data'] = '...'
        return output

    def log(self, message):
        logging.info('{}:{} {}/{} {}'.format(self.address[0], self.address[1], self.clientid, self.slug, message))

    async def get_firmware(self, filename):
        """Prepare file handle and metadata for the specified firmware file."""
        
        filename = os.path.join(os.environ.get('FIRMWARE_PATH', 'firmware'), filename)

        try:
            data = {
                'filename': filename,
                'handle': open(filename, 'rb'),
                'md5': None,
                'size': os.path.getsize(filename),
            }
            md5 = hashlib.md5()
            for chunk in data['handle']:
                md5.update(chunk)
            data['md5'] = md5.hexdigest()
            return data
        except FileNotFoundError:
            logging.error("firmware {} not found".format(filename))
            return None

    async def send_message(self, message):
        self.log('send {}'.format(self.loggable_message(message)))
        await self.write_callback(message)

    async def queue_message(self, message):
        self.log('send {}'.format(self.loggable_message(message)))
        await self.send_message(message)

    async def send_mqtt(self, topic, payload, retain=False, dedup=False, ignore_prefix=False):
        if ignore_prefix:
            t = topic
        else:
            t = self.mqtt_prefix + self.slug + '/' + topic
        logging.debug('mqtt {} {} {}'.format(topic, payload, retain))
        if self.factory.mqtt_queue:
            if retain:
                if dedup is False or payload != self.mqtt_cache.get(t):
                    self.factory.mqtt_queue.put((t, payload, retain), block=False)
                    self.mqtt_cache[t] = payload
            else:
                self.factory.mqtt_queue.put((t, payload, retain), block=False)

    async def sync_task(self):
        logging.debug('sync_task: starting')
        while True:
            logging.debug('sync_task: loop begins')

            await self.reload_settings()

            for filename in self.files.keys():

                if self.files[filename].needMetadata():
                    await self.send_message({
                        'cmd': 'file_query',
                        'filename': filename,
                    })
                    logging.debug('waiting for metadata to arrive')
                    await self.files[filename].waitForMetadata()
                    logging.debug('metadata is ready')

                if self.files[filename].needSync():
                    self.log('{} sync started'.format(filename))
                    await self.send_message({
                        'cmd': 'file_write',
                        'filename': filename,
                        'md5': self.files[filename].get_md5(),
                        'size': self.files[filename].get_size(),
                    })
                    logging.debug('waiting for sync to complete')
                    await self.files[filename].waitForSync()
                    self.log('sync complete')
                else:
                    self.log('{} up to date'.format(filename))

            if self.firmware and self.firmware_pending_reboot == False:
                if self.firmware['md5'] == self.remote_firmware_active:
                    logging.debug('firmware is up to date (active)')
                elif self.firmware['md5'] == self.remote_firmware_pending:
                    logging.debug('firmware is up to date (pending reboot)')
                    self.log('firmware up to date (pending reboot)')
                else:
                    self.log('firmware sync started')
                    await self.send_message({
                        'cmd': 'firmware_write',
                        'md5': self.firmware['md5'],
                        'size': self.firmware['size'],
                    })
                    logging.debug('waiting for firmware sync to complete')
                    await self.firmware_complete.wait()
                    self.log('firmware sync complete')

            logging.debug('sync_task: loop ends')
            await asyncio.sleep(180)

    async def handle_connect(self):
        self.log('connect {}'.format(self))
        await self.reload_settings(True)
        await self.send_mqtt('id', self.clientid, retain=True, dedup=False)
        await self.send_mqtt('address', self.writer.get_extra_info('peername')[0], retain=True, dedup=False)
        await self.send_mqtt('firmware_progress', '', retain=True, dedup=True)
        await self.send_message({'cmd': 'ready'})
        await self.send_message({'cmd': 'system_query'})

    async def handle_disconnect(self, reason=None):
        if self.factory.client_from_id(self.clientid) is self:
            self.log('disconnect: {} (final)'.format(reason))
            await self.send_mqtt('status', 'offline', retain=True, dedup=False)
            await self.send_mqtt('address', '', retain=True, dedup=False)
        else:
            self.log('disconnect: {} (replaced)'.format(reason))

    async def handle_message(self, message):
        """Handle and dispatch an incoming message from the client."""

        self.log('recv {}'.format(self.loggable_message(message)))

        commands = {
            'file_continue': self.handle_cmd_file_continue,
            'file_info': self.handle_cmd_file_info,
            'file_write_error': self.handle_cmd_file_write_error,
            'file_write_ok': self.handle_cmd_file_write_ok,
            'firmware_continue': self.handle_cmd_firmware_continue,
            'firmware_write_error': self.handle_cmd_firmware_write_error,
            'firmware_write_ok': self.handle_cmd_firmware_write_ok,
            'metrics_info': self.handle_cmd_metrics_info,
            'net_metrics_info': self.handle_cmd_net_metrics_info,
            'ping': self.handle_cmd_ping,
            'pong': self.handle_cmd_pong,
            'state_info': self.handle_cmd_state_info,
            'system_info': self.handle_cmd_system_info,
            'token_auth': self.handle_cmd_token_auth,
        }

        if message['cmd'] in commands:
            await commands[message['cmd']](message)

    async def handle_cmd_file_continue(self, message):
        chunk_size = 256
        filename = message['filename']
        position = message['position']
        handle = self.files[filename].get_handle()
        handle.seek(position)
        chunk = handle.read(chunk_size)
        reply = {
            'cmd': 'file_data',
            'filename': filename,
            'position': position,
            'data': base64.b64encode(chunk).decode(),
        }
        if position + len(chunk) >= self.files[filename].get_size():
            reply['eof'] = True
        await self.send_message(reply)

    async def handle_cmd_file_info(self, message):
        filename = message['filename']
        if filename not in self.files:
            # we don't know about this file
            logging.debug('file_info: {} is not known'.format(filename))
            return
        self.files[filename].remote(message["md5"], message["size"])

    async def handle_cmd_file_write_error(self, message):
        filename = message['filename']
        self.files[filename].syncDone()

    async def handle_cmd_file_write_ok(self, message):
        filename = message['filename']
        self.files[filename].syncDone()

    async def handle_cmd_firmware_continue(self, message):
        if not self.firmware:
            return
        chunk_size = 256
        position = message['position']
        handle = self.firmware['handle']
        handle.seek(position)
        chunk = handle.read(chunk_size)
        reply = {
            'cmd': 'firmware_data',
            'position': position,
            'data': base64.b64encode(chunk).decode(),
        }
        if position + len(chunk) >= self.firmware['size']:
            reply['eof'] = True
        progress = int(100 * (position + len(chunk)) / self.firmware['size'])
        await self.send_mqtt('firmware_progress', str(progress), retain=True, dedup=True)
        await self.send_message(reply)

    async def handle_cmd_firmware_write_error(self, message):
        self.firmware_complete.set()

    async def handle_cmd_firmware_write_ok(self, message):
        self.firmware_complete.set()
        self.firmware_pending_reboot = True

    async def handle_cmd_metrics_info(self, message):
        # all metadata will be sent to MQTT
        for k, v in message.items():
            if k not in ['cmd']:
                await self.send_mqtt('metrics/{}'.format(k), v, retain=True, dedup=False)

    async def handle_cmd_net_metrics_info(self, message):
        # all metadata will be sent to MQTT
        for k, v in message.items():
            if k not in ['cmd']:
                await self.send_mqtt('metrics/{}'.format(k), v, retain=True, dedup=False)

    async def handle_cmd_ping(self, message):
        """Reply to ping requests from the client."""

        reply = {'cmd': 'pong'}
        if 'seq' in message:
            reply['seq'] = message['seq']
        if 'timestamp' in message:
            reply['timestamp'] = message['timestamp']
        if 'millis' in message:
            reply['millis'] = message['millis']
        await self.send_message(reply)

    async def handle_cmd_pong(self, message):
        """Receive response to ping."""

        self.last_pong_received = time.time()

        if 'timestamp' in message:
            rtt = time.time() - float(message['timestamp'])
            await self.send_mqtt('rtt', str(int(rtt*1000)), retain=True, dedup=False)
    
    async def handle_cmd_system_info(self, message):
        """Receive system-level metadata from the client."""

        if 'esp_sketch_md5' in message:
            # firmware versions will be saved for managing firmware sync later
            self.remote_firmware_active = message['esp_sketch_md5']

            # send legacy MQTT topic
            await self.send_mqtt('sketch_md5', message['esp_sketch_md5'], retain=True, dedup=False)

        # all metadata will be sent to MQTT
        for k, v in message.items():
            if k not in ['cmd']:
                await self.send_mqtt('system/{}'.format(k), v, retain=True, dedup=False)

    async def handle_cmd_token_auth(self, message):
        """Look-up a token ID and return authentication data to the client."""

        result = await self.tokendb.auth_token(
            message['uid'], 
            groups=self.token_groups,
            exclude_groups=self.token_exclude_groups,
            counter=message.get('ntag_counter', None),
            location='{}:{}'.format(self.__class__.__name__.lower(), self.slug),
        )
        if result:
            await self.send_message({
                'cmd': 'token_info',
                'uid': result['uid'],
                'name': result['name'],
                'access': result['access'],
                'found': True,
            })
        else:
            await self.send_message({
                'cmd': 'token_info',
                'uid': message['uid'],
                'found': False,
            })

    def status_json(self):
        return {}


class ClientFactory:
    
    def __init__(self):
        self.clients_by_id = {}
        self.clients_by_slug = {}
        self.mqtt_queue = None        

    def client_from_id(self, clientid):
        try:
            return self.clients_by_id[clientid]
        except KeyError:
            return None    

    def client_from_slug(self, slug):
        try:
            return self.clients_by_slug[slug]
        except KeyError:
            return None

    async def client_from_hello(self, msg, reader, writer, address):
        if msg['cmd'] == 'hello':
            client = self.client_from_auth(msg['clientid'], msg['password'], address=address)
            if client:
                client.reader = reader
                client.writer = writer
                logging.debug("client_from_hello -> {}".format(client))
                return client
            else:
                logging.debug("client_from_hello -> auth failed")

    async def command(self, message):
        logging.info('command received: {}'.format(message))

        if message.get('cmd') == 'status':
            output = {}
            for clientid in self.clients_by_id.keys():
                output[clientid] = self.clients_by_id[clientid].status_json()
            return json.dumps(output)

        client = None
        if 'id' in message:
            client = self.client_from_id(message['id']) or self.client_from_slug(message['id'])
        if 'slug' in message:
            client = self.client_from_slug(message['slug'])
        if 'clientid' in message:
            client = self.client_from_id(message['clientid'])
        if not client:
            return 'Client not found'

        if message.get('cmd') == 'send' and 'message' in message:
            await client.send_message(message['message'])
            return 'OK'

        if message.get('cmd') == 'disconnect':
            client.writer.close()
            return 'OK'

        return 'Bad command'
