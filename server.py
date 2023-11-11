#!/usr/bin/env python3

import asyncio
import logging
import os
import ssl
import sys

sys.path.insert(0, os.path.join(os.path.dirname(sys.argv[0]), "lib"))

import fileloader
import tokendb
from hooks.appriseevents import AppriseEvents
from hooks.discordevents import DiscordEvents
from hooks.dispatcher import HookDispatcher
from hooks.hacklabtokens import HacklabTokens
from hooks.localconfig import LocalConfig
from hooks.logdebug import LogDebug
from hooks.mqttmetrics import MqttMetrics

import doorman
import settings


async def command_server():
    if settings.COMMAND_SOCKET is None:
        return

    server = await asyncio.start_unix_server(
        manager.command_handler,
        settings.COMMAND_SOCKET,
    )

    addr = server.sockets[0].getsockname()
    logging.info(f"Serving commands on {addr}")

    async with server:
        await server.serve_forever()


async def standard_server():
    if not settings.INSECURE_PORT:
        logging.warning("Insecure server not configured")
        return

    server = await asyncio.start_server(
        manager.stream_handler,
        None,
        settings.INSECURE_PORT,
    )

    for s in server.sockets:
        logging.info(f"Serving insecure on {s.getsockname()}")

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
        manager.stream_handler,
        None,
        settings.TLS_PORT,
        ssl=sslctx,
    )

    for s in server.sockets:
        logging.info(f"Serving TLS on {s.getsockname()}")

    async with server:
        await server.serve_forever()


async def main():
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(command_server())
            tg.create_task(standard_server())
            tg.create_task(ssl_server())
    except Exception:
        logging.exception("exception in main task group")


if settings.DEBUG:
    logging.basicConfig(level=logging.DEBUG)
else:
    logging.basicConfig(level=logging.INFO)

if settings.CACHE_PATH:
    fileloader.get_loader().set_cache_dir(settings.CACHE_PATH)

hooks = HookDispatcher()
hooks.add_hook(
    LocalConfig(
        devices=settings.DEVICES_FILE,
        profiles=settings.PROFILES_FILE,
        tokens=settings.TOKENS_FILE,
    )
)
hooks.add_hook(LogDebug())
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
manager = doorman.DoorManager(hooks, tokendb)

asyncio.run(main())
