import asyncio
import hashlib
import io
import json
import logging
import os
import time
import tomllib
import urllib.parse

import aiohttp
import yaml


def hashed_cache_key(*args, **kwargs):
    output = []
    for arg in args:
        output.append(arg)
    for k, v in kwargs.items():
        output.append(f"{k}={v}")
    return hashlib.md5(":".join(output).encode()).hexdigest()


#
# fresh_until <time> - can be used without revalidation
# stale-if-error <time> - can be used while stale if the server returns 500-series error
# stale-while-revalidate <time> - can be used while stale for this grace period
#
# default_ttl - applies if no cache-control provided
# delete_after - our own expiry, how long to keep stale files
#
def parse_cache_control(headers, default_ttl=60):
    age = int(headers.get("age", 0))

    if "cache-control" not in headers:
        return (
            time.time() - age + default_ttl,
            time.time() - age + default_ttl,
            time.time() - age + default_ttl,
        )

    cc = {}
    for chunk in headers["cache-control"].split(","):
        chunk = chunk.strip()
        if chunk.find("=") > 0:
            k, v = chunk.split("=", 1)
            cc[k.lower()] = v
        else:
            cc[chunk.lower()] = None

    fresh_until = time.time() + default_ttl
    if "no-cache" in cc:
        fresh_until = time.time() - 1
    if "max-age" in cc:
        fresh_until = time.time() - age + int(cc["max-age"])

    stale_while_revalidate_until = fresh_until
    if "stale-while-revalidate" in cc:
        stale_while_revalidate_until = fresh_until + int(cc["stale-while-revalidate"])

    stale_while_revalidate_until = fresh_until
    if "stale-if-error" in cc:
        stale_if_error_until = fresh_until + int(cc["stale-if-error"])

    return fresh_until, stale_while_revalidate_until, stale_if_error_until


def auto_parse(data, content_type=None, filename=None):
    format = None
    if content_type in ["application/json"]:
        format = "json"
    elif content_type in [
        "text/vnd.yaml",
        "text/yaml",
        "text/x-yaml",
        "application/x-yaml",
    ]:
        format = "yaml"
    elif content_type in [
        "text/x-toml",
        "application/toml",
        "text/toml",
        "application/x-toml",
    ]:
        format = "toml"
    elif filename:
        if filename.lower().endswith(".json"):
            format = "json"
        elif filename.lower().endswith(".yaml"):
            format = "yaml"
        elif filename.lower().endswith(".yml"):
            format = "yaml"
        elif filename.lower().endswith(".toml"):
            format = "toml"
    if format == "json":
        return json.loads(data)
    elif format == "yaml":
        return yaml.safe_load(data)
    elif format == "toml":
        if isinstance(data, str):
            return tomllib.loads(data)
        else:
            return tomllib.loads(data.decode())
    raise ValueError("Unable to identify format")


class FileContext:
    def __init__(self, file):
        self.file = file

    @property
    def content(self):
        return self.file.content

    @property
    def data(self):
        if isinstance(self.file.content, str):
            return self.file.content.decode()
        else:
            return self.file.content

    @property
    def text(self):
        if isinstance(self.file.content, str):
            return self.file.content
        else:
            return self.file.content.decode()

    @property
    def handle(self):
        if isinstance(self.file.content, str):
            return io.StringIO(self.file.content)
        else:
            return io.BytesIO(self.file.content)

    @property
    def md5(self):
        return self.file.md5

    @property
    def size(self):
        return self.file.size

    def parse(self):
        return self.file.parse()

    def last_update(self):
        return self.file.last_update()


class MemoryFile:
    def __init__(self, data, text=False, filename=None):
        self._filename = filename
        self._text = text

        self.content = None
        self.size = None
        self.md5 = None
        self._last_update = None

        self.update(data)

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.filename}>"

    def update(self, data):
        if data == self.content:
            # not changed
            return
        self.content = data
        self.size = len(data)
        if self._text:
            self.md5 = hashlib.md5(self.content.encode()).hexdigest()
        else:
            self.md5 = hashlib.md5(self.content).hexdigest()
        self._last_update = time.time()

    def last_update(self):
        return self._last_update

    def parse(self):
        if self._filename:
            return auto_parse(self.content, filename=self._filename)
        raise ValueError("Cannot identify format without a filename")

    async def __aenter__(self):
        return FileContext(self)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        pass


class LocalFile:
    def __init__(self, filename, text=False):
        self.filename = filename
        self.text = text

        self.lock = asyncio.Lock()

        self.content = None
        self.size = None
        self.md5 = None
        self.mtime = None

        logging.debug(f"{self} created")

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.filename}>"

    def last_update(self):
        return self.mtime

    async def load(self):
        async with self.lock:
            mtime = os.path.getmtime(self.filename)
            if mtime == self.mtime:
                return

            if self.text:
                mode = "r"
            else:
                mode = "rb"
            with open(self.filename, mode) as f:
                self.content = f.read()
                logging.debug(f"{self} loaded")

            self.mtime = mtime
            self.size = len(self.content)
            if self.text:
                self.md5 = hashlib.md5(self.content.encode()).hexdigest()
            else:
                self.md5 = hashlib.md5(self.content).hexdigest()

    def parse(self):
        return auto_parse(self.content, filename=self.filename)

    async def __aenter__(self):
        await self.load()
        return FileContext(self)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        pass


class RemoteFile:
    def __init__(
        self,
        url,
        headers={},
        text=False,
        cache_dir=None,
        min_ttl=60,
        default_ttl=3600,
        min_retry_ttl=3600,
        strict=False,
    ):
        self.url = url
        self.url_parsed = urllib.parse.urlparse(url)

        self.request_headers = headers
        self.text = text
        self.cache_dir = cache_dir
        self.min_ttl = min_ttl
        self.default_ttl = default_ttl
        self.min_retry_ttl = min_retry_ttl
        self.strict = strict

        self.lock = asyncio.Lock()
        self.last_read = 0
        self._last_update = None

        self.content = None
        self.size = None
        self.md5 = None
        self.last_modified = None
        self.etag = None
        self.fresh_until = None
        self.stale_while_revalidate_until = None
        self.stale_if_error_until = None

        self.cache_loaded = False

        logging.debug(f"{self} created")

    def __repr__(self):
        return f"<{self.__class__.__name__} {self.url}>"

    def last_update(self):
        return self._last_update

    def load_cache(self):
        if not self.cache_dir:
            return
        path = os.path.join(
            self.cache_dir,
            hashed_cache_key(
                url=self.url, headers=self.request_headers, text=self.text
            ),
        )
        try:
            with open(path + ".meta") as f:
                meta = json.load(f)
                self.etag = meta["etag"]
                self.last_modified = meta["last_modified"]
                self.fresh_until = meta["fresh_until"]
                self.stale_while_revalidate_until = meta["stale_while_revalidate_until"]
                self.stale_if_error_until = meta["stale_if_error_until"]
            if self.text:
                with open(path + ".data") as f:
                    self.content = f.read()
                    self.md5 = hashlib.md5(self.content.encode()).hexdigest()
            else:
                with open(path + ".data", "rb") as f:
                    self.content = f.read()
                    self.md5 = hashlib.md5(self.content).hexdigest()
            self.size = len(self.content)
            logging.debug(f"{self} loaded from cache")
        except FileNotFoundError:
            self.content = None
            self.size = None
            self.md5 = None
            self.etag = None
            self.fresh_until = None
            self.stale_while_revalidate_until = None
            self.stale_if_error_until = None
            self.last_modified = None
        except Exception:
            logging.exception(f"{self} exception loading from cache")
        self.cache_loaded = True

    def save_cache(self):
        if not self.cache_dir:
            return
        path = os.path.join(
            self.cache_dir,
            hashed_cache_key(
                url=self.url, headers=self.request_headers, text=self.text
            ),
        )
        if self.text:
            with open(path + ".data.tmp", "w") as f:
                f.write(self.content)
                os.rename(path + ".data.tmp", path + ".data")
        else:
            with open(path + ".data.tmp", "wb") as f:
                f.write(self.content)
                os.rename(path + ".data.tmp", path + ".data")
        with open(path + ".meta.tmp", "w") as f:
            f.write(
                json.dumps(
                    {
                        "etag": self.etag,
                        "last_modified": self.last_modified,
                        "fresh_until": self.fresh_until,
                        "stale_while_revalidate_until": self.stale_while_revalidate_until,
                        "stale_if_error_until": self.stale_if_error_until,
                    }
                )
            )
            os.rename(path + ".meta.tmp", path + ".meta")

    async def load(self):
        async with self.lock:
            if not self.cache_loaded:
                self.load_cache()

            if self.content and self.fresh_until and time.time() < self.fresh_until:
                # content is still fresh
                logging.info(f"{self} still fresh")
                return

            if self.content and time.time() - self.last_read < self.min_ttl:
                # minimum ttl applies, don't recheck
                logging.info(f"{self} still within minimum TTL")
                return

            if time.time() - self.last_read < self.min_retry_ttl:
                # limit retry attempts
                logging.info(f"{self} still within minimum retry TTL, cannot retry yet")
                return

            headers = self.request_headers.copy()
            if self.etag:
                headers["if-none-match"] = self.etag
            if self.last_modified:
                headers["if-modified-since"] = self.last_modified

            async with aiohttp.ClientSession() as session:
                async with session.get(self.url, headers=headers) as response:
                    if response.status == 200:
                        cc = parse_cache_control(
                            response.headers, default_ttl=self.default_ttl
                        )
                        self.fresh_until = cc[0]
                        self.stale_while_revalidate_until = cc[1]
                        self.stale_if_error_until = cc[2]
                        self.etag = response.headers.get("etag")
                        self.last_modified = response.headers.get("last-modified")
                        self.last_read = time.time()
                        self._last_update = time.time()
                        if self.text:
                            self.content = await response.text()
                            self.md5 = hashlib.md5(self.content.encode()).hexdigest()
                        else:
                            self.content = await response.read()
                            self.md5 = hashlib.md5(self.content).hexdigest()
                        self.size = len(self.content)
                        self.response_headers = response.headers
                        logging.info(f"{self} loaded ({self.response_headers})")
                        self.save_cache()
                    elif response.status == 304:
                        cc = parse_cache_control(
                            response.headers, default_ttl=self.default_ttl
                        )
                        self.fresh_until = cc[0]
                        self.stale_while_revalidate_until = cc[1]
                        self.stale_if_error_until = cc[2]
                        self.last_read = time.time()
                        logging.debug(f"{self} not modified")
                        self.save_cache()
                    else:
                        logging.debug(
                            f"{self} not loaded, HTTP {response.status} {response.reason}"
                        )
                        if (
                            self.content
                            and self.stale_if_error_until
                            and time.time() < self.stale_if_error_until
                        ):
                            logging.debug(f"{self} stale-if-error applies")
                            return
                        if self.content and not self.strict:
                            logging.debug(
                                f"{self} ignoring error and re-using stale data"
                            )
                            return
                        response.raise_for_status()

    def parse(self):
        return auto_parse(
            self.content, content_type=self.response_headers["content-type"]
        )

    async def __aenter__(self):
        await self.load()
        return FileContext(self)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        pass


class Loader:
    def __init__(self, cache_dir=None):
        self.cache_dir = cache_dir
        self.files = {}
        self.lock = asyncio.Lock()
        logging.debug(f"{self} created")

    def memory_file(self, data, text=False, filename=None):
        return MemoryFile(data, text=text, filename=filename)

    def local_file(self, filename, text=False):
        key = f"local:{filename}:text={text}"
        try:
            return self.files[key]
        except KeyError:
            self.files[key] = LocalFile(filename, text=text)
            return self.files[key]

    def remote_file(self, url, headers={}, text=False, min_ttl=60, default_ttl=3600):
        header_hash = hashlib.md5(f"{headers}".encode()).hexdigest()
        key = f"remote:{url}:{header_hash}:text={text}:min_ttl={min_ttl}:default_ttl={default_ttl}"
        try:
            return self.files[key]
        except KeyError:
            logging.debug(f"{self} creating RemoteFile with key {key}")
            self.files[key] = RemoteFile(
                url,
                headers=headers,
                text=text,
                cache_dir=self.cache_dir,
                min_ttl=min_ttl,
                default_ttl=default_ttl,
            )
            return self.files[key]


loader = None


def get_loader():
    global loader
    if loader is None:
        loader = Loader(cache_dir=os.environ.get("FILELOADER_CACHE_DIR", None))
    return loader
