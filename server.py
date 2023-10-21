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
import settings


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
    except Exception:
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
    except Exception:
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
    except ConnectionResetError:
        await client.handle_disconnect(reason="connection reset")
    except asyncio.exceptions.IncompleteReadError:
        await client.handle_disconnect(reason="incomplete read")
    except TimeoutError:
        await client.handle_disconnect(reason="receive timeout")
    except Exception:
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
    if settings.COMMAND_SOCKET is None:
        return

    server = await asyncio.start_unix_server(
        command_handler,
        settings.COMMAND_SOCKET,
    )

    addr = server.sockets[0].getsockname()
    print(f"Serving commands on {addr}")

    async with server:
        await server.serve_forever()


async def standard_server():
    if not settings.INSECURE_PORT:
        logging.warning("Insecure server not configured")
        return

    server = await asyncio.start_server(
        ss_handler,
        "0.0.0.0",
        settings.INSECURE_PORT,
    )

    addr = server.sockets[0].getsockname()
    print(f"Serving insecure on {addr}")

    async with server:
        await server.serve_forever()


async def ssl_server():
    if not (settings.TLS_PORT and settings.TLS_CERT_FILE and settings.TLS_KEY_FILE):
        logging.warning("TLS server not configured")
        return

    sslctx = ssl.SSLContext(protocol=ssl.PROTOCOL_TLS_SERVER)
    sslctx.set_ciphers(settings.TLS_CIPHERS)
    sslctx.load_cert_chain(settings.TLS_CERT_FILE, settings.TLS_KEY_FILE)

    server = await asyncio.start_server(
        ss_handler,
        "0.0.0.0",
        settings.TLS_PORT,
        ssl=sslctx,
    )

    addr = server.sockets[0].getsockname()
    print(f"Serving TLS on {addr}")

    async with server:
        await server.serve_forever()


async def main():
    try:
        await gather_group(
            command_server(),
            standard_server(),
            ssl_server(),
        )
    except Exception:
        logging.exception("gather exception")


if settings.DEBUG:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

hooks = HookDispatcher()
hooks.add_hook(LocalDeviceConfig(settings.DEVICE_FILE))
hooks.add_hook(LogDebug())
if settings.TOKENS_FILE:
    hooks.add_hook(LocalTokens(settings.TOKENS_FILE))
if settings.REMOTE_TOKENS_URL and settings.REMOTE_AUTH_URL and settings.REMOTE_SECRET:
    hooks.add_hook(
        HacklabTokens(
            settings.REMOTE_TOKENS_URL, settings.REMOTE_AUTH_URL, settings.REMOTE_SECRET
        )
    )
if settings.MQTT_HOST:
    hooks.add_hook(
        MqttMetrics(
            settings.MQTT_HOST, port=settings.MQTT_PORT, prefix=settings.MQTT_PREFIX
        )
    )
if settings.APPRISE_URLS:
    hooks.add_hook(
        AppriseEvents(settings.APPRISE_URLS, apprise_events=settings.APPRISE_EVENTS)
    )
if settings.DISCORD_WEBHOOK:
    hooks.add_hook(
        DiscordEvents(settings.DISCORD_WEBHOOK, discord_events=settings.DISCORD_EVENTS)
    )

tokendb = tokendb.TokenAuthDatabase(hooks)
clientfactory = doorman.DoorFactory(hooks, tokendb)

asyncio.run(main())
