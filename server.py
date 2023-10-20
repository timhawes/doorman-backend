#!/usr/bin/env python3

import asyncio
import json
import logging
import os
import ssl
import sys

sys.path.insert(0, os.path.join(os.path.dirname(sys.argv[0]), "lib"))

from hooks.dispatcher import HookDispatcher
from hooks.localdeviceconfig import LocalDeviceConfig
from hooks.hacklabtokens import HacklabTokens
from hooks.mqttmetrics import MqttMetrics
from hooks.appriseevents import AppriseEvents
from hooks.discordevents import DiscordEvents
from hooks.localtokens import LocalTokens
from hooks.logdebug import LogDebug
import doorman
import tokendb


DEFAULT_NOTIFY_EVENTS = "backend_start connect disconnect restarted power_mains power_battery file_sync_start file_sync_complete firmware_sync_start firmware_sync_complete exit_request_ignored"


class settings:
    mqtt_host = os.environ.get("MQTT_HOST")
    mqtt_port = int(os.environ.get("MQTT_PORT", "1883"))
    mqtt_prefix = os.environ.get("MQTT_PREFIX", "doorman/")
    server_cert_file = os.environ.get("SERVER_CERT_FILE")
    server_key_file = os.environ.get("SERVER_KEY_FILE")
    listen_host = os.environ.get("LISTEN_HOST", "0.0.0.0")
    listen_port = int(os.environ.get("LISTEN_PORT", 14260))
    listen_ssl_port = int(os.environ.get("LISTEN_SSL_PORT", 14261))
    firmware_path = os.environ.get("FIRMWARE_PATH", "firmware")
    config_yaml = os.environ.get("CONFIG_YAML", "config/config.yaml")
    local_tokens_file = os.environ.get("LOCAL_TOKENS_FILE")
    api_download_url = os.environ.get("API_DOWNLOAD_URL")
    api_auth_url = os.environ.get("API_AUTH_URL")
    api_token = os.environ.get("API_TOKEN")
    command_socket = os.environ.get("COMMAND_SOCKET")
    apprise_urls = os.environ.get("APPRISE_URLS", "").strip().split()
    apprise_events = (
        os.environ.get("APPRISE_EVENTS", DEFAULT_NOTIFY_EVENTS).strip().split()
    )
    discord_webhook = os.environ.get("DISCORD_WEBHOOK")
    discord_events = (
        os.environ.get("DISCORD_EVENTS", DEFAULT_NOTIFY_EVENTS).strip().split()
    )
    if os.environ.get("DEBUG_MODE"):
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
            stream.write(bytes([len(data)]) + data)
            await stream.drain()
        else:
            raise ValueError("Maximum packet size is 255")
    elif len_bytes == 2:
        if len(data) <= 65535:
            msb = len(data) >> 8
            lsb = len(data) & 255
            stream.write(bytes([msb, lsb]) + data)
            await stream.drain()
        else:
            raise ValueError("Maximum packet size is 65535")
    else:
        raise RuntimeError("Packet length header must be 1-2 bytes")


async def create_client(reader, writer):
    data = await read_packet(reader, len_bytes=2)
    msg = json.loads(data)
    # logging.debug("create_client < {}".format(msg))
    client = await clientfactory.client_from_hello(
        msg, reader, writer, writer.get_extra_info("peername")
    )
    if client:
        return client


async def ss_reader(reader, callback, timeout=180):
    while True:
        data = await asyncio.wait_for(read_packet(reader, len_bytes=2), timeout=timeout)
        if data:
            # logging.debug("ss_reader < {}".format(data))
            try:
                msg = json.loads(data)
            except UnicodeDecodeError:
                logging.exception(f"Error processing received packet {data}")
                return
            except json.JSONDecodeError:
                logging.exception(f"Error processing received packet {data}")
                return
            await callback(msg)
        else:
            return


async def ss_write_callback(writer, lock, msg):
    # logging.debug("ss_writer > {}".format(msg))
    data = json.dumps(msg, separators=(",", ":")).encode()
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
    address = writer.get_extra_info("peername")
    logging.debug(f"peername: {address}")
    for key in ["compression", "cipher", "peercert", "sslcontext", "ssl_object"]:
        data = writer.get_extra_info(key)
        if data:
            logging.debug(f"{key}: {data}")
            if key == "ssl_object":
                logging.debug(f"version {data.version()}")

    write_lock = asyncio.Lock()

    async def client_write_callback(msg):
        await ss_write_callback(writer, write_lock, msg)

    try:
        client = await create_client(reader, writer)
        if client:
            client.write_callback = client_write_callback
        else:
            writer.close()
            return
    except Exception as e:
        logging.exception(f"Exception creating client for {address}")
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
        await client.handle_disconnect(reason="connection reset")
    except asyncio.exceptions.IncompleteReadError as e:
        await client.handle_disconnect(reason="incomplete read")
    except TimeoutError as e:
        await client.handle_disconnect(reason="receive timeout")
    except Exception as e:
        logging.exception("gather exception")
    finally:
        logging.debug("closing main_loop")
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
                if response.endswith("\n"):
                    writer.write(response.encode())
                else:
                    writer.write(response.encode() + b"\n")
                await writer.drain()
    except Exception as e:
        writer.write(f"Exception: {e}\n".encode())
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
    print(f"Serving on {addr}")

    async with server:
        await server.serve_forever()


async def standard_server():
    server = await asyncio.start_server(
        ss_handler,
        "0.0.0.0",
        settings.listen_port,
    )

    addr = server.sockets[0].getsockname()
    print(f"Serving on {addr}")

    async with server:
        await server.serve_forever()


async def ssl_server():
    sslctx = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_SERVER)
    sslctx.set_ciphers(
        "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:AES256-SHA256"
    )
    sslctx.load_cert_chain(settings.server_cert_file, settings.server_key_file)

    server = await asyncio.start_server(
        ss_handler,
        "0.0.0.0",
        settings.listen_ssl_port,
        ssl=sslctx,
    )

    addr = server.sockets[0].getsockname()
    print(f"Serving on {addr}")

    async with server:
        await server.serve_forever()


async def main():
    try:
        await gather_group(
            command_server(),
            standard_server(),
            ssl_server(),
        )
    except Exception as e:
        logging.exception("gather exception")


if settings.debug:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

hooks = HookDispatcher()
hooks.add_hook(LocalDeviceConfig(settings.config_yaml))
hooks.add_hook(LogDebug())
if settings.local_tokens_file:
    hooks.add_hook(LocalTokens(settings.local_tokens_file))
if settings.api_download_url and settings.api_auth_url and settings.api_token:
    hooks.add_hook(
        HacklabTokens(
            settings.api_download_url, settings.api_auth_url, settings.api_token
        )
    )
if settings.mqtt_host:
    hooks.add_hook(
        MqttMetrics(
            settings.mqtt_host, port=settings.mqtt_port, prefix=settings.mqtt_prefix
        )
    )
if settings.apprise_urls:
    hooks.add_hook(
        AppriseEvents(settings.apprise_urls, apprise_events=settings.apprise_events)
    )
if settings.discord_webhook:
    hooks.add_hook(
        DiscordEvents(settings.discord_webhook, discord_events=settings.discord_events)
    )

tokendb = tokendb.TokenAuthDatabase(hooks)
clientfactory = doorman.DoorFactory(hooks, tokendb)

asyncio.run(main())
