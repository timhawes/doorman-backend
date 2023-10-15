import asyncio
import base64
import datetime
import hashlib
import io
import json
import logging
import os
import random
import time
from dataclasses import dataclass


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
        # self.logger.debug('needSync remote={}/{} vs local={}/{}'.format(self.remote_md5, self.remote_size, self.md5, self.size))
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
        # self.logger.debug('recording remote metadata md5={} size={}'.format(md5, size))
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
                return True
            else:
                return False

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
            self.filename = filename
        new_size = os.path.getsize(filename)
        new_md5 = hashlib.md5()
        for chunk in open(filename, "rb"):
            new_md5.update(chunk)
        if new_size != self.size or new_md5.hexdigest() != self.md5:
            self.size = new_size
            self.md5 = new_md5.hexdigest()
            self.handle = None
            self.get_handle()
            return True
        else:
            return False

    def get_handle(self):
        if self.handle is None:
            self.handle = open(self.filename, "rb")
        return self.handle

    def get_size(self):
        return self.size

    def get_md5(self):
        return self.md5


@dataclass
class MessageCallback:
    filters: list
    response: dict | None = None
    event = None

    def __post_init__(self):
        if self.event is None:
            self.event = asyncio.Event()


class Client:
    def __init__(self, clientid, factory, config, hooks, address):
        self.clientid = clientid
        self.factory = factory
        self.config = config
        self.address = address
        self.hooks = hooks
        self.connected = True
        self.connect_start = time.time()
        self.connect_finish = None

        # initial value for slug
        self.slug = self.config.get("slug", self.clientid)

        # chunk size for syncs
        self.chunk_size = self.config.get("sync_chunk_size", 256)

        # configure logging
        self.logger = logging.getLogger(f"client.{self.clientid}/{self.slug}")

        # asyncio network streams, will be populated by the factory
        self.reader = None
        self.writer = None

        # callback for sending an outbound message object
        # will be populated by the asyncio protocol handler
        self.write_callback = None

        # intervals
        self.time_send_interval = 3600
        self.ping_interval = 30
        self.net_metrics_query_interval = 60

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

    def __str__(self):
        if self.writer:
            tags = [
                "slug={}".format(self.slug),
                "clientid={}".format(self.clientid),
            ]
            peername = self.writer.get_extra_info("peername")
            if peername:
                tags.append("peername={}:{}".format(*peername))
            cipher = self.writer.get_extra_info("cipher")
            if cipher:
                tags.append("cipher={}:{}:{}".format(*cipher))
            return "<{} {}>".format(self.__class__.__name__, " ".join(tags))
        else:
            return "<{} clientid={}>".format(self.__class__.__name__, self.clientid)

    async def reload_settings(self, create=False):
        self.config = await self.hooks.get_device(self.clientid)

        self.token_groups = self.config.get("groups")
        self.token_exclude_groups = self.config.get("exclude_groups")
        token_data = await self.factory.tokendb.token_database_v2(
            groups=self.token_groups,
            exclude_groups=self.token_exclude_groups,
            salt=self.config.get("token_salt").encode(),
        )
        if create:
            self.files["tokens.dat"] = SyncableStringFile("tokens.dat", token_data)
        else:
            self.files["tokens.dat"].update(token_data)

        for filename, filedata in self.config.get("files").items():
            content = render_file(filename, filedata)
            if create:
                self.files[filename] = SyncableStringFile(filename, content)
            else:
                self.files[filename].update(content)

        firmware_filename = self.config.get("firmware")
        if firmware_filename:
            self.firmware = await self.get_firmware(firmware_filename)

        try:
            legacy_config_json = render_file(
                "config.json",
                legacy_config(self.config.get("files")),
            )
            if create:
                self.files["config.json"] = SyncableStringFile(
                    "config.json", legacy_config_json
                )
            else:
                self.files["config.json"].update(legacy_config_json)
        except Exception as e:
            logging.exception("Skipping legacy config.json", e)

    def status_json(self):
        status = {
            "clientid": self.clientid,
            "address": f"{self.address[0]}:{self.address[1]}",
            "connected": self.connected,
            "slug": self.slug,
            "metrics": self.metrics,
            "states": self.states,
            "files": self.remote_files,
        }
        if self.connect_start:
            status["connect_start"] = datetime.datetime.fromtimestamp(
                self.connect_start, datetime.timezone.utc
            ).isoformat()
        if self.connect_finish:
            status["connect_finish"] = datetime.datetime.fromtimestamp(
                self.connect_finish, datetime.timezone.utc
            ).isoformat()
        if self.connected:
            status["connect_uptime"] = int(time.time() - self.connect_start)
        return status

    def loggable_message(self, message):
        output = message.copy()
        if output.get("cmd") in ["file_data", "firmware_data"]:
            output["data"] = "..."
        return output

    def log(self, message):
        self.logger.info(
            "{}:{} {}/{} {}".format(
                self.address[0], self.address[1], self.clientid, self.slug, message
            )
        )

    async def log_event(self, event):
        if event.get("time") is None:
            event["time"] = time.time()
        event["clientid"] = self.clientid
        event["device"] = self.slug
        self.logger.info(f"event {event}")
        await self.factory.hooks.log_event(event)

    async def get_firmware(self, filename):
        """Prepare file handle and metadata for the specified firmware file."""

        filename = os.path.join(os.environ.get("FIRMWARE_PATH", "firmware"), filename)

        try:
            data = {
                "filename": filename,
                "handle": open(filename, "rb"),
                "md5": None,
                "size": os.path.getsize(filename),
            }
            md5 = hashlib.md5()
            for chunk in data["handle"]:
                md5.update(chunk)
            data["md5"] = md5.hexdigest()
            return data
        except FileNotFoundError:
            self.logger.error("firmware {} not found".format(filename))
            return None

    async def send_message(self, message):
        self.logger.info("send {}".format(self.loggable_message(message)))
        await self.write_callback(message)

    async def send_and_get_response(self, message, filters, timeout=5):
        cb = MessageCallback(filters=filters)
        self.callbacks.append(cb)
        await self.send_message(message)
        try:
            await asyncio.wait_for(cb.event.wait(), timeout=timeout)
            return cb.response
        except asyncio.TimeoutError:
            self.logger.warn(f"callback timeout waiting for {filter}")
        finally:
            self.callbacks.remove(cb)

    async def set_metrics(self, metrics, timestamp=None):
        self.metrics.update(metrics)
        await self.factory.hooks.log_metrics(
            self.clientid, self.slug, metrics, timestamp=timestamp
        )

    async def set_states(self, states, timestamp=None):
        self.states.update(states)
        await self.factory.hooks.log_states(
            self.clientid, self.slug, states, timestamp=timestamp
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
                "size": self.files[filename].get_size(),
                "md5": self.files[filename].get_md5(),
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

        self.logger.info(f"sync: firmware started")
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
                self.logger.info(f"sync: firmware complete")
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

        self.logger.error(f"sync: firmware no response")

    async def _sync_loop(self):
        self.logger.debug("sync_task: loop begins")

        await self.reload_settings()

        for filename in self.files.keys():
            await self._sync_file(
                filename,
                self.files[filename].get_size(),
                self.files[filename].get_md5(),
                self.files[filename].get_handle(),
                dry_run=self.config.get("sync_dryrun", False),
            )

        if self.firmware:
            await self._sync_firmware(
                self.firmware["size"],
                self.firmware["md5"],
                self.firmware["handle"],
                dry_run=self.config.get("sync_dryrun", False),
            )

        self.logger.debug("sync_task: loop ends")

    async def sync_task(self):
        self.logger.debug("sync_task: starting")
        while True:
            await self._sync_loop()
            await asyncio.sleep(180)

    async def handle_connect(self):
        self.logger.info("connect {}".format(self))
        await self.reload_settings(True)
        event = {
            "event": "connect",
            "address": "{}:{}".format(*self.writer.get_extra_info("peername")),
        }
        cipher = self.writer.get_extra_info("cipher")
        if cipher:
            event["tls_cipher"] = "{}:{}:{}".format(*cipher)
        await self.log_event(event)
        await self.set_states(
            {
                "id": self.clientid,
                "address": self.writer.get_extra_info("peername")[0],
                "firmware_progress": None,
            }
        )
        await self.send_message({"cmd": "ready"})
        await self.send_message({"cmd": "time", "time": int(time.time())})
        await self.send_message({"cmd": "system_query"})

    async def handle_disconnect(self, reason=None):
        self.connect_finish = time.time()
        self.connected = False
        if self.factory.client_from_id(self.clientid) is self:
            self.logger.info("disconnect: {} (final)".format(reason))
            await self.log_event({"event": "disconnect"})
            await self.set_states({"status": "offline", "address": None})
        else:
            self.logger.info("disconnect: {} (replaced)".format(reason))

    async def handle_message(self, message):
        """Handle and dispatch an incoming message from the client."""

        self.logger.info("recv {}".format(self.loggable_message(message)))

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

        result = await self.hooks.auth_token(
            message["uid"],
            groups=self.token_groups,
            exclude_groups=self.token_exclude_groups,
            location="{}:{}".format(self.__class__.__name__.lower(), self.slug),
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


class ClientFactory:
    def __init__(self):
        self.clients_by_id = {}
        self.clients_by_slug = {}

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
        if msg["cmd"] == "hello":
            client = await self.client_from_auth(
                msg["clientid"], msg["password"], address=address
            )
            if client:
                client.reader = reader
                client.writer = writer
                logging.info("client_from_hello -> {}".format(client))
                return client
            else:
                logging.info("client_from_hello -> auth failed")

    async def command(self, message):
        logging.info("command received: {}".format(message))

        if message.get("cmd") == "list-all":
            output = {}
            for clientid in self.clients_by_id.keys():
                output[clientid] = self.clients_by_id[clientid].slug
            return json.dumps(output, sort_keys=True)

        if message.get("cmd") == "list":
            output = {}
            for clientid in self.clients_by_id.keys():
                if self.clients_by_id[clientid].connected:
                    output[clientid] = self.clients_by_id[clientid].slug
            return json.dumps(output, sort_keys=True)

        if message.get("cmd") == "status":
            output = {}
            for clientid in self.clients_by_id.keys():
                output[clientid] = self.clients_by_id[clientid].status_json()
            return json.dumps(output)

        if message.get("cmd") == "disconnect-all":
            for client in self.clients_by_id.values():
                try:
                    client.writer.close()
                except:
                    pass
            return "OK"

        client = None
        if "id" in message:
            client = self.client_from_id(message["id"]) or self.client_from_slug(
                message["id"]
            )
        if "slug" in message:
            client = self.client_from_slug(message["slug"])
        if "clientid" in message:
            client = self.client_from_id(message["clientid"])
        if not client:
            return "Client not found"

        if message.get("cmd") == "send" and "message" in message:
            await client.send_message(message["message"])
            return "OK"

        if message.get("cmd") == "disconnect":
            client.writer.close()
            return "OK"

        return "Bad command"
