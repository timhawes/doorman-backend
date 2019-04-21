#!/usr/bin/env python3

import asyncio
import dbm
import json
import logging
import os
import ssl
import sys
import time
import queue
import threading

import paho.mqtt.client as mqtt

sys.path.insert(0, 'lib')

import doorman
from clientdb import ClientDB
from ehl_tokendb_crm_async import TokenAuthDatabase


class settings:
    mqtt_host = os.environ.get('MQTT_HOST')
    mqtt_port = int(os.environ.get('MQTT_PORT', '1883'))
    mqtt_global_prefix = os.environ.get('MQTT_PREFIX', 'test/')
    mqtt_prefix = os.environ.get('MQTT_PREFIX', 'doorman/')
    server_cert_file = os.environ.get('SERVER_CERT_FILE')
    server_key_file = os.environ.get('SERVER_KEY_FILE')
    listen_host = os.environ.get('LISTEN_HOST', '0.0.0.0')
    listen_port = int(os.environ.get('LISTEN_PORT', 14260))
    listen_ssl_port = int(os.environ.get('LISTEN_SSL_PORT', 14261))
    firmware_path = os.environ.get('FIRMWARE_PATH', 'firmware')
    config_yaml = os.environ.get('CONFIG_YAML', 'config/config.yaml')
    api_download_url = os.environ.get('API_DOWNLOAD_URL')
    api_auth_url = os.environ.get('API_AUTH_URL')
    api_query_url = os.environ.get('API_QUERY_URL')
    api_token = os.environ.get('API_TOKEN')
    command_socket = os.environ.get('COMMAND_SOCKET')
    if os.environ.get('DEBUG_MODE'):
        debug = True
    else:
        debug = False


async def read_packet(stream, len_bytes=1):
    header = await stream.readexactly(len_bytes)
    if len_bytes == 1:
        length = header[0]
    elif len_bytes == 2:
        length = header[0] << 8 | header[1]
    else:
        raise RuntimeError("Packet length header must be 1-2 bytes")
    return await stream.readexactly(length)


async def write_packet(stream, data, len_bytes=1):
    if len_bytes == 1:
        if len(data) <= 255:
            stream.write(bytes([len(data)]))
            await stream.drain()
        else:
            raise ValueError("Maximum packet size is 255")
    elif len_bytes == 2:
        if len(data) <= 65535:
            msb = len(data) >> 8
            lsb = len(data) & 255
            stream.write(bytes([msb, lsb]))
            await stream.drain()
        else:
            raise ValueError("Maximum packet size is 65535")
    else:
        raise RuntimeError("Packet length header must be 1-2 bytes")
    stream.write(data)
    await stream.drain()


async def create_client(reader, writer):
    data = await read_packet(reader, len_bytes=2)
    msg = json.loads(data)
    #logging.debug("create_client < {}".format(msg))
    client = await clientfactory.client_from_hello(msg, reader, writer, writer.get_extra_info('peername'))
    if client:
        return client

async def ss_reader(reader, callback, timeout=180):
    while True:
        data = await asyncio.wait_for(read_packet(reader, len_bytes=2), timeout=timeout)
        if data:
            #logging.debug("ss_reader < {}".format(data))
            msg = json.loads(data)
            await callback(msg)
        else:
            return


async def ss_write_callback(writer, lock, msg):
    #logging.debug("ss_writer > {}".format(msg))
    data = json.dumps(msg).encode()
    async with lock:
        await write_packet(writer, data, len_bytes=2)


async def gather_group(*tasks):
    gathering = asyncio.gather(*tasks)
    try:
        return await gathering
    except Exception as e:
        [task.cancel() for task in gathering._children]
        raise


async def ss_handler(reader, writer):
    address = writer.get_extra_info('peername')
    logging.debug('peername: {}'.format(address))
    for key in ['compression', 'cipher', 'peercert', 'sslcontext', 'ssl_object']:
        data = writer.get_extra_info(key)
        if data:
            logging.debug('{}: {}'.format(key, data))
            if key == 'ssl_object':
                logging.debug('version {}'.format(data.version()))

    write_lock = asyncio.Lock()
    async def client_write_callback(msg):
        await ss_write_callback(writer, write_lock, msg)

    client = await create_client(reader, writer)
    if client:
        client.write_callback = client_write_callback
    else:
        writer.close()
        return

    try:
        await client.handle_connect()
        await gather_group(
            ss_reader(reader, client.handle_message),
            client.main_task(),
            client.sync_task(),
        )
    except ConnectionResetError as e:
        await client.handle_disconnect(reason='connection reset')
    except asyncio.streams.IncompleteReadError as e:
        await client.handle_disconnect(reason='incomplete read')
    except asyncio.TimeoutError as e:
        await client.handle_disconnect(reason='receive timeout')
    except Exception as e:
        logging.exception("gather exception")
    finally:
        logging.debug('closing main_loop')
        writer.close()


async def command_handler(reader, writer):
    print("command handler connection in progress...")
    try:
        data = await reader.read()
        if len(data) > 0:
            message = json.loads(data)
            response = await clientfactory.command(message)
            if isinstance(response, dict):
                writer.write(json.dumps(response).encode())
                await writer.drain()
            elif isinstance(response, str):
                writer.write(response.encode())
                if not response.endswith('\n'):
                    writer.write(b'\n')
                await writer.drain()
    except Exception as e:
        writer.write('Exception: {}\n'.format(e).encode())
        await writer.drain()
    writer.close()


async def command_server():

    if settings.command_socket is None:
        return

    server = await asyncio.start_unix_server(
        command_handler,
        settings.command_socket,
    )

    addr = server.sockets[0].getsockname()
    print('Serving on {}'.format(addr))

    async with server:
        await server.serve_forever()


async def standard_server():

    server = await asyncio.start_server(
        ss_handler, '0.0.0.0',
        settings.listen_port,
    )

    addr = server.sockets[0].getsockname()
    print('Serving on {}'.format(addr))

    async with server:
        await server.serve_forever()


async def ssl_server():

    sslctx = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS)
    sslctx.load_cert_chain(settings.server_cert_file, settings.server_key_file)

    server = await asyncio.start_server(
        ss_handler, '0.0.0.0',
        settings.listen_ssl_port,
        ssl=sslctx,
    )

    addr = server.sockets[0].getsockname()
    print('Serving on {}'.format(addr))

    async with server:
        await server.serve_forever()


async def reloader():

    while True:
        await asyncio.sleep(300)
        await tokendb.load()


async def main():
    await tokendb.load()

    try:
        await gather_group(
            command_server(),
            standard_server(),
            ssl_server(),
            reloader(),
        )
    except Exception as e:
        logging.exception("gather exception")


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
                m.connect(settings.mqtt_host, settings.mqtt_port)
                m.loop_start()
                while True:
                    topic, payload, retain = mqtt_queue.get()
                    m.publish('{}{}'.format(settings.mqtt_global_prefix, topic), payload, retain=retain)
            except Exception as e:
                logging.exception('Exception in MqttThread')
                time.sleep(1)


if settings.mqtt_host:
    mqtt_queue = queue.Queue()
    mqtt_thread = MqttThread()
    mqtt_thread.daemon = True
    mqtt_thread.start()
else:
    mqtt_queue = None

clientdb = ClientDB(settings.config_yaml)
tokendb = TokenAuthDatabase(settings.api_download_url,
                            settings.api_query_url,
                            settings.api_auth_url,
                            settings.api_token)
clientfactory = doorman.DoorFactory(clientdb, tokendb)
clientfactory.mqtt_queue = mqtt_queue
clientfactory.mqtt_prefix = settings.mqtt_prefix

if settings.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)
asyncio.run(main())