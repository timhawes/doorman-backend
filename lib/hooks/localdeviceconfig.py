import fileloader

from .base import BaseHook


from mergedicts import mergedicts


class LocalDeviceConfig(BaseHook):
    def __init__(self, filename):
        self.config_yaml = fileloader.get_loader().local_file(filename, text=True)

    async def get_device(self, clientid):
        if len(clientid) == 0:
            return None
        if clientid == "DEFAULTS":
            return None
        async with self.config_yaml as file:
            data = file.parse()
            return mergedicts([data[clientid], data["DEFAULTS"]])

    async def auth_device(self, clientid, password):
        if len(clientid) == 0:
            return None
        if len(password) == 0:
            return None
        device = await self.get_device(clientid)
        if device:
            our_password = device["password"]
            if isinstance(our_password, str):
                if password == our_password:
                    return device
            if isinstance(our_password, list):
                if password in our_password:
                    return device
        return None
