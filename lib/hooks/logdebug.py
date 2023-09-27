import logging
import time

from .base import BaseHook

logger = logging.getLogger(__name__)


class LogDebug(BaseHook):
    def __init__(self):
        self.state_cache = {}

    async def log_metrics(self, deviceid, devicename, metrics, *, timestamp=None):
        if timestamp is None:
            timestamp = time.time()
        for key, value in metrics.items():
            logging.debug(f"{deviceid}/{devicename} {timestamp} metric {key} {value}")

    async def log_states(self, deviceid, devicename, states, *, timestamp=None):
        if timestamp is None:
            timestamp = time.time()
        for key, value in states.items():
            cache_key = f"{deviceid}:{key}"
            old_value = self.state_cache.get(cache_key)
            if old_value != value:
                logging.debug(
                    f"{deviceid}/{devicename} {timestamp} state {key} {old_value} => {value}"
                )
                self.state_cache[cache_key] = value
