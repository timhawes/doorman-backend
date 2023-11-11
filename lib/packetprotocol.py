import asyncio
import json
import logging


logger = logging.getLogger(__name__)


class CustomLoggerAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return f"{self.extra.get('peername', '-')} {msg}", kwargs


class PacketConnection:
    def create_task(self, *args, **kwargs):
        return self.tg.create_task(*args, **kwargs)

    def disconnect(self):
        self._closed_by_server = True
        self._writer.close()

    def get_extra_info(self, name, default=None):
        if hasattr(self, "_writer"):
            return self._writer.get_extra_info(name, default=default)
        else:
            return default

    async def handle_connect(self):
        pass

    async def handle_packet(self, data):
        pass

    async def handle_disconnect(self, reason=None):
        pass

    async def send_packet(self, data):
        if len(data) <= 65535:
            msb = len(data) >> 8
            lsb = len(data) & 255
            async with self._lock:
                self._writer.write(bytes([msb, lsb]) + data)
                await self._writer.drain()
        else:
            raise ValueError("Maximum packet size is 65535")

    async def set_timeout(self, timeout):
        self._timeout = timeout

    async def stream_handler(self, reader, writer):
        self._writer = writer
        self._timeout = 60
        self._lock = asyncio.Lock()
        self._closed_by_server = False
        self.logger = CustomLoggerAdapter(
            logger, {"peername": "{}:{}".format(*self.get_extra_info("peername"))}
        )
        try:
            async with asyncio.TaskGroup() as tg:
                self.tg = tg
                await self.handle_connect()
                while True:
                    async with asyncio.timeout(self._timeout):
                        try:
                            header = await reader.readexactly(2)
                        except asyncio.exceptions.IncompleteReadError:
                            break
                        length = header[0] << 8 | header[1]
                        data = await reader.readexactly(length)
                    await self.handle_packet(data)
                if self._closed_by_server:
                    await self.handle_disconnect(reason="ClosedByServer")
                else:
                    await self.handle_disconnect(reason="EOF")
        except* ConnectionResetError:
            await self.handle_disconnect(reason="ConnectionResetError")
        except* asyncio.exceptions.IncompleteReadError:
            await self.handle_disconnect(reason="IncompleteReadError")
        except* TimeoutError:
            await self.handle_disconnect(reason="TimeoutError")
        except* RuntimeError as e:
            await self.handle_disconnect(reason=repr(e.exceptions[0]))
        except* Exception as e:
            await self.handle_disconnect(reason=repr(e.exceptions[0]))
            logging.exception("Exception in stream handler")
        finally:
            writer.close()


class JsonConnection(PacketConnection):
    async def handle_message(self, message):
        pass

    async def handle_packet(self, data):
        if data:
            try:
                message = json.loads(data)
            except UnicodeDecodeError:
                logging.exception(f"Error processing received packet {data}")
                return
            except json.JSONDecodeError:
                logging.exception(f"Error processing received packet {data}")
                return
            await self.handle_message(message)

    async def send_message(self, message):
        data = json.dumps(message, separators=(",", ":")).encode()
        await self.send_packet(data)
