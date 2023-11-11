import asyncio
import base64
import datetime
import json
import logging
import os
import random
import time
from dataclasses import dataclass

import fileloader
import settings
import packetprotocol


logger = logging.getLogger(__name__)


class AuthenticationError(RuntimeError):
    pass


def is_uid(uid):
    if len(uid) not in [8, 14]:
        return False
    for c in uid.lower():
        if c not in "0123456789abcdef":
            return False
    return True


def render_file(filename, data):
    if type(data) is bytes:
        return data
    if type(data) is str:
        return data.encode()
    if filename.endswith(".json") and type(data) in [list, dict]:
        return json.dumps(data, sort_keys=True).encode()
    raise ValueError(f"Invalid data for {filename}")


def legacy_config(files):
    net_map = {
        "host": "server_host",
        "port": "server_port",
        "password": "server_password",
        "tls": "server_tls_enabled",
        "tls_verify": "server_tls_verify",
        "tls_fingerprint1": "server_fingerprint1",
        "tls_fingerprint2": "server_fingerprint2",
        "watchdog_time": "network_watchdog_time",
    }
    wifi_map = {
        "ssid": "ssid",
        "password": "wpa_password",
    }
    data = {}
    for k, v in files["app.json"].items():
        data[k] = v
    for k, v in net_map.items():
        if k in files["net.json"]:
            data[net_map[k]] = files["net.json"][k]
    for k, v in wifi_map.items():
        if k in files["wifi.json"]:
            data[wifi_map[k]] = files["wifi.json"][k]
    return data


class ConnectionLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return (
            f"{self.extra.get('peername', '-')} {self.extra.get('clientid', '-')} {self.extra.get('name', '-')} {msg}",
            kwargs,
        )


@dataclass
class MessageCallback:
    filters: list
    response: dict | None = None
    event = None

    def __post_init__(self):
        if self.event is None:
            self.event = asyncio.Event()


class CommonConnection(packetprotocol.JsonConnection):
    client_strip_prefix = ""

    def __init__(self, manager):
        self.manager = manager
        self.authenticated = False

        # intervals
        self.time_send_interval = 3600
        self.ping_interval = 30
        self.net_metrics_query_interval = 60

    def __str__(self):
        tags = []
        peername = self.get_extra_info("peername")
        if peername:
            tags.append("peername={}:{}".format(*peername))
        cipher = self.get_extra_info("cipher")
        if cipher:
            tags.append("cipher={}:{}:{}".format(*cipher))
        if self.authenticated:
            tags.append(f"clientid={self.clientid}")
            tags.append(f"name={self.name}")
        return "<{} {}>".format(self.__class__.__name__, " ".join(tags))

    async def reload_settings(self):
        self.config = await self.manager.hooks.get_device(self.clientid)

        self.token_groups = self.config.get("groups")
        self.token_exclude_groups = self.config.get("exclude_groups")
        token_data = await self.manager.tokendb.token_database_v2(
            groups=self.token_groups,
            exclude_groups=self.token_exclude_groups,
            salt=self.config.get("token_salt").encode(),
        )
        if "tokens.dat" in self.files:
            self.files["tokens.dat"].update(token_data)
        else:
            self.files["tokens.dat"] = self.manager.loader.memory_file(
                token_data, filename="tokens.dat"
            )

        for filename, filedata in self.config.get("files").items():
            if filedata is None:
                self.files[filename] = None
            else:
                content = render_file(filename, filedata)
                if filename in self.files:
                    self.files[filename].update(content)
                else:
                    self.files[filename] = self.manager.loader.memory_file(
                        content, filename=filename
                    )

        firmware_filename = self.config.get("firmware")
        if firmware_filename:
            if firmware_filename.startswith("https://") or firmware_filename.startswith(
                "http://"
            ):
                self.firmware = self.manager.loader.remote_file(
                    firmware_filename, default_ttl=28800
                )
            elif firmware_filename.startswith("/"):
                self.firmware = self.manager.loader.local_file(firmware_filename)
            else:
                self.firmware = self.manager.loader.local_file(
                    os.path.join(settings.FIRMWARE_PATH, firmware_filename)
                )

        try:
            legacy_config_json = render_file(
                "config.json",
                legacy_config(self.config.get("files")),
            )
            if "config.json" in self.files:
                self.files["config.json"].update(legacy_config_json)
            else:
                self.files["config.json"] = self.manager.loader.memory_file(
                    legacy_config_json, filename="config.json"
                )
        except Exception as e:
            logging.exception("Skipping legacy config.json", e)

    def status_json(self):
        status = {
            "clientid": self.clientid,
            "address": "{}:{}".format(*self.get_extra_info("peername")),
            "connected": self.connected,
            "name": self.name,
            "metrics": self.metrics,
            "states": self.states,
            "files": self.remote_files,
        }
        if self.connect_start:
            status["connect_start"] = datetime.datetime.fromtimestamp(
                self.connect_start, datetime.UTC
            ).isoformat()
        if self.connect_finish:
            status["connect_finish"] = datetime.datetime.fromtimestamp(
                self.connect_finish, datetime.UTC
            ).isoformat()
        if self.connected:
            status["connect_uptime"] = int(time.time() - self.connect_start)
        return status

    def loggable_message(self, message):
        output = message.copy()
        if output.get("cmd") in ["file_data", "firmware_data"]:
            output["data"] = "..."
        if "password" in output:
            output["password"] = "***"
        return output

    def log(self, message):
        self.logger.info(message)

    async def log_event(self, event):
        if event.get("time") is None:
            event["time"] = time.time()
        event["clientid"] = self.clientid
        event["device"] = self.name
        self.logger.info(f"event {event}")
        await self.manager.hooks.log_event(event)

    async def send_message(self, message):
        self.logger.info(f"send {self.loggable_message(message)}")
        await super().send_message(message)

    async def send_and_get_response(self, message, filters, timeout=5):
        cb = MessageCallback(filters=filters)
        self.callbacks.append(cb)
        await self.send_message(message)
        try:
            await asyncio.wait_for(cb.event.wait(), timeout=timeout)
            return cb.response
        except TimeoutError:
            self.logger.warn(f"callback timeout waiting for {filters}")
        finally:
            self.callbacks.remove(cb)

    async def set_metrics(self, metrics, timestamp=None):
        self.metrics.update(metrics)
        await self.manager.hooks.log_metrics(
            self.clientid, self.name, metrics, timestamp=timestamp
        )

    async def set_states(self, states, timestamp=None):
        self.states.update(states)
        await self.manager.hooks.log_states(
            self.clientid, self.name, states, timestamp=timestamp
        )

    async def _sync_file(self, filename, size, md5, data, dry_run=False):
        remote = await self.send_and_get_response(
            {"cmd": "file_query", "filename": filename},
            [{"cmd": "file_info", "filename": filename}],
        )
        if not remote:
            self.logger.warn(f"sync: {filename} no response to file_query")
            return

        if remote.get("md5") == md5 and remote.get("size") == size:
            self.logger.info(f"sync: {filename} is up to date")
            return

        if dry_run:
            self.logger.info(f"sync: {filename} requires sync (dry-run mode)")
            return

        self.logger.info(f"sync: {filename} started")
        await self.log_event(
            {
                "event": "file_sync_start",
                "filename": filename,
                "size": size,
                "md5": md5,
            }
        )

        filters = [
            {"cmd": "file_continue", "filename": filename},
            {"cmd": "file_write_error", "filename": filename},
            {"cmd": "file_write_ok", "filename": filename},
        ]

        response = await self.send_and_get_response(
            {
                "cmd": "file_write",
                "filename": filename,
                "md5": md5,
                "size": size,
            },
            filters,
        )

        while response:
            if response["cmd"] == "file_write_error":
                error = response.get("error")
                self.logger.error(f"sync: {filename} error {error}")
                return
            if response["cmd"] == "file_write_ok":
                self.logger.info(f"sync: {filename} complete")
                await self.log_event(
                    {
                        "event": "file_sync_complete",
                        "filename": filename,
                        "size": size,
                        "md5": md5,
                    }
                )
                return
            if response["cmd"] == "file_continue":
                position = response["position"]
                data.seek(position)
                chunk = data.read(self.chunk_size)
                file_data = {
                    "cmd": "file_data",
                    "filename": filename,
                    "position": position,
                    "data": base64.b64encode(chunk).decode(),
                }
                if position + len(chunk) >= size:
                    file_data["eof"] = True
                response = await self.send_and_get_response(file_data, filters)

        self.logger.error(f"sync: {filename} no response")

    async def _sync_delete_file(self, filename, dry_run=False):
        remote = await self.send_and_get_response(
            {"cmd": "file_query", "filename": filename},
            [{"cmd": "file_info", "filename": filename}],
        )
        if not remote:
            self.logger.warn(f"sync: {filename} no response to file_query")
            return

        if remote.get("md5") is None and remote.get("size") is None:
            self.logger.info(f"sync: {filename} is up to date (deleted)")
            return

        if dry_run:
            self.logger.info(f"sync: {filename} requires deletion (dry-run mode)")
            return

        self.logger.info(f"sync: {filename} deleting")
        await self.log_event(
            {
                "event": "file_delete",
                "filename": filename,
            }
        )

        await self.send_message(
            {
                "cmd": "file_delete",
                "filename": filename,
            }
        )

    async def _sync_firmware(self, size, md5, data, dry_run=False):
        if self.remote_firmware_active is None and self.remote_firmware_pending is None:
            self.logger.info("sync: firmware remote state is unknown")
            return

        if self.remote_firmware_active == md5:
            self.logger.info("sync: firmware is up to date (active)")
            return

        if self.remote_firmware_pending == md5:
            self.logger.info("sync: firmware is up to date (pending reboot)")
            return

        if dry_run:
            self.logger.info("sync: firmware requires sync (dry-run mode)")
            return

        self.logger.info("sync: firmware started")
        await self.log_event(
            {
                "event": "firmware_sync_start",
                "size": size,
                "md5": md5,
            }
        )
        filters = [
            {"cmd": "firmware_continue"},
            {"cmd": "firmware_write_error"},
            {"cmd": "firmware_write_ok"},
        ]

        response = await self.send_and_get_response(
            {
                "cmd": "firmware_write",
                "md5": md5,
                "size": size,
            },
            filters,
        )

        while response:
            if response["cmd"] == "firmware_write_error":
                error = response.get("error")
                self.logger.error(f"sync: firmware error {error}")
                return
            if response["cmd"] == "firmware_write_ok":
                self.remote_firmware_pending = md5
                self.logger.info("sync: firmware complete")
                await self.log_event(
                    {
                        "event": "firmware_sync_complete",
                        "size": size,
                        "md5": md5,
                    }
                )
                return
            if response["cmd"] == "firmware_continue":
                position = response["position"]
                data.seek(position)
                chunk = data.read(self.chunk_size)
                file_data = {
                    "cmd": "firmware_data",
                    "position": position,
                    "data": base64.b64encode(chunk).decode(),
                }
                if position + len(chunk) >= size:
                    file_data["eof"] = True
                progress = int(100 * (position + len(chunk)) / size)
                await self.set_states({"firmware_progress": progress})
                response = await self.send_and_get_response(file_data, filters)

        self.logger.error("sync: firmware no response")

    async def _sync_loop(self):
        self.logger.debug("sync_task: loop begins")

        await self.reload_settings()

        if settings.SYNC_FILES:
            for filename in self.files.keys():
                if self.files[filename] is None:
                    await self._sync_delete_file(filename)
                else:
                    async with self.files[filename] as f:
                        await self._sync_file(
                            filename,
                            f.size,
                            f.md5,
                            f.handle,
                            dry_run=self.config.get("sync_dryrun", False),
                        )

        if settings.SYNC_FIRMWARE and self.firmware:
            try:
                self.logger.debug(f"sync_task: firmware syncing {self.firmware}")
                async with self.firmware as f:
                    await self._sync_firmware(
                        f.size,
                        f.md5,
                        f.handle,
                        dry_run=self.config.get("sync_dryrun", False),
                    )
            except FileNotFoundError:
                self.logger.warn("fsync_task: firmware file not found")
            except Exception:
                self.logger.exception("sync_task: exception")

        self.logger.debug("sync_task: loop ends")

    async def sync_task(self):
        self.logger.debug("sync_task: starting")
        while True:
            await self._sync_loop()
            await asyncio.sleep(180)

    async def handle_connect(self):
        self.connected = True
        self.connect_start = time.time()
        self.connect_finish = None

        # set timeout for authentication
        self.set_timeout(60)

    async def handle_disconnect(self, reason=None):
        self.connect_finish = time.time()
        self.connected = False
        if self.authenticated:
            if self.manager.clients_by_id.get(self.clientid) is self:
                self.logger.info(f"disconnect: {reason} (final)")
                await self.log_event({"event": "disconnect", "reason": reason})
                await self.set_states({"status": "offline", "address": None})
            else:
                self.logger.info(f"disconnect: {reason} (replaced)")
        else:
            self.logger.info(f"disconnect: {reason}")

    async def handle_unauthenticated_message(self, message):
        if message.get("cmd") != "hello":
            raise AuthenticationError(
                "Invalid command type from {}:{}".format(
                    *self.get_extra_info("peername")
                )
            )

        # authenticate
        clientid = message["clientid"]
        if clientid.startswith(self.client_strip_prefix):
            clientid = clientid[len(self.client_strip_prefix) :]
        config = await self.manager.hooks.auth_device(clientid, message["password"])
        if not config:
            raise AuthenticationError(
                "Auth failed from {}:{}".format(*self.get_extra_info("peername"))
            )

        # increase timeout after the successful authentication
        self.set_timeout(300)

        # setup
        self.clientid = clientid
        self.config = config
        self.name = self.config.get("name") or self.config.get("slug") or self.clientid

        # chunk size for syncs
        self.chunk_size = self.config.get("sync_chunk_size", 256)

        # configure logging
        self.logger = ConnectionLoggerAdapter(
            logger,
            {
                "peername": "{}:{}".format(*self.get_extra_info("peername")),
                "clientid": self.clientid,
                "name": self.name,
            },
        )

        # timestamps
        self.last_time_sent = time.time() - random.randint(0, 1800)
        self.last_ping_sent = time.time() - random.randint(
            0, int(self.ping_interval * 0.75)
        )
        self.last_pong_received = time.time()
        self.last_net_metrics_query = time.time() - random.randint(
            0, int(self.net_metrics_query_interval * 0.75)
        )

        # remote state for syncing
        self.remote_files = {}
        self.remote_firmware_active = None
        self.remote_firmware_pending = None

        # local specification for syncing
        self.files = {}
        self.firmware = None

        # metrics
        self.metrics = {}
        self.states = {}

        # message callbacks
        self.callbacks = []

        # mark connection as authenticated
        self.authenticated = True

        # register connection with the manager
        self.manager.clients_by_id[self.clientid] = self
        self.manager.clients_by_name[self.name] = self

        self.logger.info(f"connect {self}")
        await self.reload_settings()
        event = {
            "event": "connect",
            "address": "{}:{}".format(*self.get_extra_info("peername")),
        }
        cipher = self.get_extra_info("cipher")
        if cipher:
            event["tls_cipher"] = "{}:{}:{}".format(*cipher)
        await self.log_event(event)
        await self.set_states(
            {
                "id": self.clientid,
                "address": self.get_extra_info("peername")[0],
                "firmware_progress": None,
            }
        )
        await self.send_message({"cmd": "ready"})
        await self.send_message({"cmd": "time", "time": int(time.time())})
        await self.send_message({"cmd": "system_query"})

        # start main and sync tasks
        self.create_task(self.main_task())
        self.create_task(self.sync_task())

    async def handle_message(self, message):
        """Handle and dispatch an incoming message from the client."""

        self.logger.info(f"recv {self.loggable_message(message)}")

        if not self.authenticated:
            return await self.handle_unauthenticated_message(message)

        handled_by_callback = False

        for cb in self.callbacks:
            if cb.response is None:
                for f in cb.filters:
                    matched = True
                    for k, v in f.items():
                        if message.get(k) != v:
                            matched = False
                            break
                    if matched:
                        cb.response = message
                        cb.event.set()
                        handled_by_callback = True
                        break

        try:
            method = getattr(self, f"handle_cmd_{message['cmd']}")
        except AttributeError:
            if not handled_by_callback:
                self.logger.info(f"Ignoring unknown cmd {message['cmd']}")
            return

        if callable(method):
            await method(message)

    async def handle_cmd_event(self, message):
        message_copy = message.copy()
        del message_copy["cmd"]
        await self.log_event(message_copy)

    async def handle_cmd_file_info(self, message):
        filename = message["filename"]
        if message["md5"] is None and message["size"] is None:
            try:
                del self.remote_files[filename]
            except KeyError:
                pass
        else:
            self.remote_files[filename] = {
                "md5": message["md5"],
                "size": message["size"],
            }

    async def handle_cmd_file_delete_ok(self, message):
        filename = message["filename"]
        try:
            del self.remote_files[filename]
        except KeyError:
            pass

    async def handle_cmd_metrics_info(self, message):
        metrics = message.copy()
        del metrics["cmd"]
        await self.set_metrics(metrics, timestamp=metrics.pop("time", None))

    async def handle_cmd_net_metrics_info(self, message):
        metrics = message.copy()
        del metrics["cmd"]
        await self.set_metrics(metrics, timestamp=metrics.pop("time", None))

    async def handle_cmd_ping(self, message):
        """Reply to ping requests from the client."""

        reply = {"cmd": "pong"}
        if "seq" in message:
            reply["seq"] = message["seq"]
        if "timestamp" in message:
            reply["timestamp"] = message["timestamp"]
        if "millis" in message:
            reply["millis"] = message["millis"]
        await self.send_message(reply)

    async def handle_cmd_pong(self, message):
        """Receive response to ping."""
        self.last_pong_received = time.time()

        if "timestamp" in message:
            rtt = time.time() - float(message["timestamp"])
            await self.set_metrics({"rtt": int(rtt * 1000)})

    async def handle_cmd_system_info(self, message):
        """Receive system-level metadata from the client."""
        if "esp_sketch_md5" in message:
            # firmware versions will be saved for managing firmware sync later
            self.remote_firmware_active = message["esp_sketch_md5"]

        if message.get("restarted") is True:
            timestamp = time.time() - (message["millis"] / 1000)
            event = {
                "event": "restarted",
                "time": timestamp,
                "esp_reset_reason": message.get("esp_reset_reason"),
                "esp_reset_info": message.get("esp_reset_info"),
            }
            if "net_reset_info" in message:
                event["net_reset_info"] = message["net_reset_info"]
            await self.log_event(event)

        metrics = message.copy()
        del metrics["cmd"]
        await self.set_metrics(metrics, timestamp=metrics.pop("time", None))

    async def handle_cmd_token_auth(self, message):
        """Look-up a token ID and return authentication data to the client."""

        result = await self.manager.hooks.auth_token(
            message["uid"],
            groups=self.token_groups,
            exclude_groups=self.token_exclude_groups,
            location=f"{self.__class__.__name__.lower()}:{self.name}",
            extra={"counter": message.get("ntag_counter", None)},
        )
        if result:
            await self.send_message(
                {
                    "cmd": "token_info",
                    "uid": result["uid"],
                    "name": result["name"],
                    "access": result["access"],
                    "found": True,
                }
            )
        else:
            await self.send_message(
                {
                    "cmd": "token_info",
                    "uid": message["uid"],
                    "found": False,
                }
            )

    async def loop(self):
        if time.time() - self.last_time_sent > self.time_send_interval:
            await self.send_message({"cmd": "time", "time": int(time.time())})
            self.last_time_sent = time.time()
        if time.time() - self.last_ping_sent > self.ping_interval:
            await self.send_message({"cmd": "ping", "timestamp": str(time.time())})
            self.last_ping_sent = time.time()
        if time.time() - self.last_pong_received > 65:
            self.log("no pong received for >65 seconds")
            raise Exception("no pong received for >65 seconds")
        if time.time() - self.last_net_metrics_query > self.net_metrics_query_interval:
            await self.send_message({"cmd": "net_metrics_query"})
            self.last_net_metrics_query = time.time()


class CommonManager:
    connection_class = CommonConnection

    def __init__(self, hooks, tokendb):
        self.hooks = hooks
        self.tokendb = tokendb
        self.loader = fileloader.get_loader()
        self.clients_by_id = {}
        self.clients_by_name = {}

    async def stream_handler(self, reader, writer):
        try:
            conn = self.connection_class(manager=self)
            return await conn.stream_handler(reader, writer)
        except Exception:
            logging.exception("Exception")

    async def command_handler(self, reader, writer):
        try:
            data = await reader.read()
            if len(data) > 0:
                message = json.loads(data)
                response = await self.command(message)
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

    async def command(self, message):
        logging.info(f"command received: {message}")

        if message.get("cmd") == "list-all":
            output = {}
            for clientid in self.clients_by_id.keys():
                output[clientid] = self.clients_by_id[clientid].name
            return json.dumps(output, sort_keys=True)

        if message.get("cmd") == "list":
            output = {}
            for clientid in self.clients_by_id.keys():
                if self.clients_by_id[clientid].connected:
                    output[clientid] = self.clients_by_id[clientid].name
            return json.dumps(output, sort_keys=True)

        if message.get("cmd") == "status":
            output = {}
            for clientid in self.clients_by_id.keys():
                output[clientid] = self.clients_by_id[clientid].status_json()
            return json.dumps(output)

        if message.get("cmd") == "disconnect-all":
            for client in self.clients_by_id.values():
                try:
                    client.disconnect()
                except Exception:
                    pass
            return "OK"

        client = None
        if "id" in message:
            client = self.clients_by_id.get(message["id"]) or self.clients_by_name.get(
                message["id"]
            )
        if "name" in message:
            client = self.clients_by_name.get(message["name"])
        if "clientid" in message:
            client = self.clients_by_id.get(message["clientid"])
        if not client:
            return "Client not found"

        if message.get("cmd") == "send" and "message" in message:
            await client.send_message(message["message"])
            return "OK"

        if message.get("cmd") == "disconnect":
            client.disconnect()
            return "OK"

        return "Bad command"
