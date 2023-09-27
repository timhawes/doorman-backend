import logging


class HookDispatcher:
    def __init__(self, debug=False):
        self.hooks = []
        self.debug = debug

    def add_hook(self, hook):
        self.hooks.append(hook)

    async def call_method(self, name, *args, **kwargs):
        for hook in self.hooks:
            if hasattr(hook, name):
                try:
                    if self.debug:
                        logging.debug(f"HookDispatcher calling {hook}.{name}")
                    response = await getattr(hook, name)(*args, **kwargs)
                    if response:
                        return response
                except NotImplementedError:
                    pass
        return None

    async def get_device(self, deviceid):
        return await self.call_method("get_device", deviceid)

    async def auth_device(self, deviceid, password):
        return await self.call_method("auth_device", deviceid, password)

    async def get_tokens(self):
        return await self.call_method("get_tokens")

    async def auth_token(
        self, uid, *, groups=None, exclude_groups=None, location=None, extra={}
    ):
        return await self.call_method(
            "auth_token",
            uid,
            groups=groups,
            exclude_groups=exclude_groups,
            location=location,
            extra=extra,
        )

    async def log_metrics(self, deviceid, devicename, metrics, *, timestamp=None):
        return await self.call_method(
            "log_metrics", deviceid, devicename, metrics, timestamp=timestamp
        )

    async def log_states(self, deviceid, devicename, states, *, timestamp=None):
        return await self.call_method(
            "log_states", deviceid, devicename, states, timestamp=timestamp
        )

    async def log_event(self, message):
        return await self.call_method("log_event", message)

    async def log_other(self, message):
        return await self.call_method("log_other", message)
