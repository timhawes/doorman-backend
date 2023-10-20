import fileloader
import mergedicts
from hooks.base import BaseHook

PARENT_KEY = "profiles"


class LocalDeviceConfig(BaseHook):
    def __init__(self, devices_filename, profiles_filename=None):
        loader = fileloader.get_loader()
        self.devices = loader.local_file(devices_filename)
        if profiles_filename:
            self.profiles = loader.local_file(profiles_filename)
        else:
            self.profiles = None

    async def get_device(self, clientid):
        if len(clientid) == 0:
            return None
        async with self.devices as d:
            try:
                device_data = d.parse()[clientid]
            except KeyError:
                return None
        if PARENT_KEY in device_data:
            if not self.profiles:
                raise KeyError(
                    "Device references a profile but no profiles file was configured"
                )
            profiles_data = []
            async with self.profiles as p:
                pdata = p.parse()
                for name in device_data[PARENT_KEY]:
                    parent_profile = mergedicts.get(pdata, name, PARENT_KEY)
                    profiles_data.append(parent_profile)
            output = mergedicts.mergedicts([device_data] + profiles_data)
            del output[PARENT_KEY]
            return output
        else:
            return device_data

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
