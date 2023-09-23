import asyncio
import json
import logging
import os
import time

import aiohttp

try:
    import yaml
except ImportError:
    pass

try:
    import tomllib
except ImportError:
    pass


class RemoteTextFile:
    text = True

    def __init__(self, url, headers={}, min_ttl=60):
        self.url = url
        self.headers = headers
        self.min_ttl = min_ttl

        self.lock = asyncio.Lock()
        self.data = None
        self.last_read = 0
        self.etag = None
        self.last_modified = None

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.url}>"

    def parse(self, data):
        return data

    async def load(self):
        async with self.lock:
            if self.data is not None and time.time() - self.last_read < self.min_ttl:
                logging.debug(f"{self} not reloading, min_ttl not reached")
                return
            headers = self.headers.copy()
            if self.etag:
                headers["etag"] = self.etag
            if self.last_modified:
                headers["if-modified-since"] = self.last_modified
            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, headers=headers) as response:
                    response.raise_for_status()
                    if response.status == 200:
                        self.etag = response.headers.get("etag")
                        self.last_modified = response.headers.get("last-modified")
                        self.last_read = time.time()
                        if self.text:
                            self.data = self.parse(await response.text())
                        else:
                            self.data = self.parse(await response.read())
                        logging.info(f"{self} loaded")
                    else:
                        logging.debug(
                            f"{self} not loaded, HTTP {response.status} {response.reason}"
                        )

    async def __aenter__(self):
        await self.load()
        return self.data

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        pass


class RemoteBinaryFile(RemoteTextFile):
    text = False


class RemoteJsonFile(RemoteTextFile):
    text = True

    def parse(self, data):
        return json.loads(data)


class RemoteYamlFile(RemoteTextFile):
    text = True

    def parse(self, data):
        return yaml.safe_load(data)


class RemoteTomlFile(RemoteTextFile):
    text = True

    def parse(self, data):
        return tomllib.loads(data)


class LocalTextFile:
    text = True

    def __init__(self, filename):
        self.filename = filename

        self.lock = asyncio.Lock()
        self.data = None
        self.last_mtime = None

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.filename}>"

    def parse(self, data):
        return data

    async def load(self):
        async with self.lock:
            mtime = os.path.getmtime(self.filename)
            if mtime == self.last_mtime:
                return
            if self.text:
                mode = "r"
            else:
                mode = "rb"
            with open(self.filename, mode) as f:
                self.data = self.parse(f.read())
                self.last_mtime = mtime
                logging.debug(f"{self} loaded with mtime {self.last_mtime}")

    async def __aenter__(self):
        await self.load()
        return self.data

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        pass


class LocalBinaryFile(LocalTextFile):
    text = False


class LocalJsonFile(LocalTextFile):
    text = True

    def parse(self, data):
        return json.loads(data)


class LocalYamlFile(LocalTextFile):
    text = True

    def parse(self, data):
        return yaml.safe_load(data)


class LocalTomlFile(LocalTextFile):
    text = True

    def parse(self, data):
        return tomllib.loads(data)
