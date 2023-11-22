import os

ENV_PREFIX = "DOORMAN_"


class defaults:
    DEFAULT_NOTIFY_EVENTS = [
        "backend_start",
        "connect",
        "disconnect",
        "restarted",
        "power_mains",
        "power_battery",
        "file_sync_start",
        "file_sync_complete",
        "firmware_sync_start",
        "firmware_sync_complete",
        "exit_request_ignored",
    ]
    INSECURE_PORT = 14260
    TLS_PORT = 14261
    TLS_CERT_FILE = None
    TLS_KEY_FILE = None
    TLS_CIPHERS = "TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256:TLS_AES_128_GCM_SHA256:AES256-SHA256"
    COMMAND_SOCKET = None
    MQTT_HOST = None
    MQTT_PORT = 1883
    MQTT_USERNAME = None
    MQTT_PASSWORD = None
    MQTT_TLS = False
    MQTT_PREFIX = "doorman/"
    DEVICES_FILE = None
    PROFILES_FILE = None
    TOKENS_FILE = None
    REMOTE_TOKENS_URL = None
    REMOTE_AUTH_URL = None
    REMOTE_SECRET = None
    FIRMWARE_PATH = None
    FIRMWARE_MIN_TTL = 60
    FIRMWARE_DEFAULT_TTL = 28800
    APPRISE_URLS = None
    APPRISE_EVENTS = DEFAULT_NOTIFY_EVENTS
    DISCORD_WEBHOOK = None
    DISCORD_EVENTS = DEFAULT_NOTIFY_EVENTS
    DEBUG = False
    CACHE_PATH = None
    SYNC_FIRMWARE = True
    SYNC_FILES = True
    SYNC_CHUNK_SIZE = 256
    SYNC_REPLY_TIMEOUT = 20
    PACKET_REPLY_TIMEOUT = 5
    TIME_SEND_INTERVAL = 3600
    PING_INTERVAL = 30
    PONG_RECEIVE_TIMEOUT = 75
    METRICS_QUERY_INTERVAL = 60
    PACKET_READ_TIMEOUT = 300
    PACKET_AUTH_READ_TIMEOUT = 60
    GENERATE_CONFIG_JSON = False
    LOCATION_PREFIX = "door:"


def parse_boolean(value):
    if value == "":
        return None
    if value.lower() in ["0", "off", "no", "n", "false", "f"]:
        return False
    if value.lower() in ["1", "on", "yes", "y", "true", "t"]:
        return True
    raise ValueError(f'Cannot parse "{value}" as boolean')


def parse_list(value):
    if value is None:
        return None
    return value.strip().split()


def getenv(key, default=None, parser=None):
    try:
        raw = os.environ[f"{ENV_PREFIX}{key}"]
        if parser:
            return parser(raw)
        else:
            return raw
    except KeyError:
        return default


INSECURE_PORT = getenv("INSECURE_PORT", defaults.INSECURE_PORT, parser=int)
TLS_PORT = getenv("TLS_PORT", defaults.TLS_PORT, parser=int)
TLS_CERT_FILE = getenv("TLS_CERT_FILE", defaults.TLS_CERT_FILE)
TLS_KEY_FILE = getenv("TLS_KEY_FILE", defaults.TLS_KEY_FILE)
TLS_CIPHERS = getenv("TLS_CIPHERS", defaults.TLS_CIPHERS)
COMMAND_SOCKET = getenv("COMMAND_SOCKET", defaults.COMMAND_SOCKET)
MQTT_HOST = getenv("MQTT_HOST", defaults.MQTT_HOST)
MQTT_PORT = getenv("MQTT_PORT", defaults.MQTT_PORT, parser=int)
MQTT_USERNAME = getenv("MQTT_USERNAME", defaults.MQTT_USERNAME)
MQTT_PASSWORD = getenv("MQTT_PASSWORD", defaults.MQTT_PASSWORD)
MQTT_TLS = getenv("MQTT_TLS", defaults.MQTT_TLS, parser=parse_boolean)
MQTT_PREFIX = getenv("MQTT_PREFIX", defaults.MQTT_PREFIX)
DEVICES_FILE = getenv("DEVICES_FILE", defaults.DEVICES_FILE)
PROFILES_FILE = getenv("PROFILES_FILE", defaults.PROFILES_FILE)
TOKENS_FILE = getenv("TOKENS_FILE", defaults.TOKENS_FILE)
REMOTE_TOKENS_URL = getenv("REMOTE_TOKENS_URL", defaults.REMOTE_TOKENS_URL)
REMOTE_AUTH_URL = getenv("REMOTE_AUTH_URL", defaults.REMOTE_AUTH_URL)
REMOTE_SECRET = getenv("REMOTE_SECRET", defaults.REMOTE_SECRET)
FIRMWARE_PATH = getenv("FIRMWARE_PATH", defaults.FIRMWARE_PATH)
FIRMWARE_MIN_TTL = getenv("FIRMWARE_MIN_TTL", defaults.FIRMWARE_MIN_TTL, parser=int)
FIRMWARE_DEFAULT_TTL = getenv(
    "FIRMWARE_DEFAULT_TTL", defaults.FIRMWARE_DEFAULT_TTL, parser=int
)
APPRISE_URLS = getenv("APPRISE_URLS", defaults.APPRISE_URLS, parser=parse_list)
APPRISE_EVENTS = getenv("APPRISE_EVENTS", defaults.APPRISE_EVENTS, parser=parse_list)
DISCORD_WEBHOOK = getenv("DISCORD_WEBHOOK", defaults.DISCORD_WEBHOOK)
DISCORD_EVENTS = getenv("DISCORD_EVENTS", defaults.DISCORD_EVENTS, parser=parse_list)
DEBUG = getenv("DEBUG", defaults.DEBUG, parser=parse_boolean)
CACHE_PATH = getenv("CACHE_PATH", defaults.CACHE_PATH)
SYNC_FIRMWARE = getenv("SYNC_FIRMWARE", defaults.SYNC_FIRMWARE, parser=parse_boolean)
SYNC_FILES = getenv("SYNC_FILES", defaults.SYNC_FILES, parser=parse_boolean)
SYNC_CHUNK_SIZE = getenv("SYNC_CHUNK_SIZE", defaults.SYNC_CHUNK_SIZE, parser=int)
SYNC_REPLY_TIMEOUT = getenv(
    "SYNC_REPLY_TIMEOUT", defaults.SYNC_REPLY_TIMEOUT, parser=int
)
PACKET_REPLY_TIMEOUT = getenv(
    "PACKET_REPLY_TIMEOUT", defaults.PACKET_REPLY_TIMEOUT, parser=int
)
TIME_SEND_INTERVAL = getenv(
    "TIME_SEND_INTERVAL", defaults.TIME_SEND_INTERVAL, parser=int
)
PING_INTERVAL = getenv("PING_INTERVAL", defaults.PING_INTERVAL, parser=int)
PONG_RECEIVE_TIMEOUT = getenv(
    "PONG_RECEIVE_TIMEOUT", defaults.PONG_RECEIVE_TIMEOUT, parser=int
)
METRICS_QUERY_INTERVAL = getenv(
    "METRICS_QUERY_INTERVAL", defaults.METRICS_QUERY_INTERVAL, parser=int
)
PACKET_READ_TIMEOUT = getenv(
    "PACKET_READ_TIMEOUT", defaults.PACKET_READ_TIMEOUT, parser=int
)
PACKET_AUTH_READ_TIMEOUT = getenv(
    "PACKET_AUTH_READ_TIMEOUT", defaults.PACKET_AUTH_READ_TIMEOUT, parser=int
)
GENERATE_CONFIG_JSON = getenv(
    "GENERATE_CONFIG_JSON", defaults.GENERATE_CONFIG_JSON, parser=parse_boolean
)
LOCATION_PREFIX = getenv("LOCATION_PREFIX", defaults.LOCATION_PREFIX)
